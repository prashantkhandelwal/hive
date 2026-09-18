use std::{
    env,
    fs,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use sysinfo::{Pid, System};
use tempfile::TempDir;
use tokio::{sync::Semaphore, time};

const DEFAULT_REQUESTS: usize = 50_000;
const DEFAULT_CONCURRENCY: usize = 2_000;

struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "resource-intensive benchmark; run explicitly in release mode"]
async fn benchmark_entire_application_under_concurrent_load() {
    let requests = env_usize("HIVE_BENCH_REQUESTS", DEFAULT_REQUESTS);
    let concurrency = env_usize("HIVE_BENCH_CONCURRENCY", DEFAULT_CONCURRENCY);
    assert!(requests > 0, "HIVE_BENCH_REQUESTS must be greater than zero");
    assert!(concurrency > 0, "HIVE_BENCH_CONCURRENCY must be greater than zero");

    let port = reserve_local_port();
    let temporary_directory = TempDir::new().expect("benchmark directory should be created");
    let config_path = temporary_directory.path().join("hive-benchmark.toml");
    let database_path = temporary_directory.path().join("hive-benchmark.db");
    fs::write(
        &config_path,
        format!(
            r#"default_protocol = "http"
http_addr = "127.0.0.1:{port}"
udp_addr = "127.0.0.1:0"
database_path = '{}'
announce_interval = 1800
peer_timeout = 3600
persistence_interval = 3600
rate_limit_per_minute = 4294967295
log_filter = "hive_tracker=warn"
"#,
            database_path.display().to_string().replace('\\', "/")
        ),
    )
    .expect("benchmark configuration should be written");

    let executable = env!("CARGO_BIN_EXE_hive-tracker");
    let child = Command::new(executable)
        .args(["--config", config_path.to_str().unwrap(), "--protocol", "http"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Hive server should start");
    let process_id = child.id();
    let mut server = ServerProcess(child);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency)
        .build()
        .expect("HTTP client should build");
    let base_url = format!("http://127.0.0.1:{port}");
    wait_until_healthy(&client, &base_url).await;

    let sampling = Arc::new(AtomicBool::new(true));
    let peak_memory_bytes = Arc::new(AtomicU64::new(0));
    let cpu_samples = Arc::new(AtomicU64::new(0));
    let cpu_total_millipercent = Arc::new(AtomicU64::new(0));
    let sampler = tokio::spawn(sample_resources(
        process_id,
        Arc::clone(&sampling),
        Arc::clone(&peak_memory_bytes),
        Arc::clone(&cpu_samples),
        Arc::clone(&cpu_total_millipercent),
    ));

    let successful = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let latency_micros = Arc::new(AtomicU64::new(0));
    let permits = Arc::new(Semaphore::new(concurrency));
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();

    for request_id in 0..requests {
        let permit = Arc::clone(&permits).acquire_owned().await.unwrap();
        let client = client.clone();
        let successful = Arc::clone(&successful);
        let failed = Arc::clone(&failed);
        let latency_micros = Arc::clone(&latency_micros);
        let url = announce_url(&base_url, request_id);
        tasks.spawn(async move {
            let request_started = Instant::now();
            let succeeded = match client.get(url).send().await {
                Ok(response) if response.status().is_success() => response.bytes().await.is_ok(),
                _ => false,
            };
            latency_micros.fetch_add(
                request_started.elapsed().as_micros() as u64,
                Ordering::Relaxed,
            );
            if succeeded {
                successful.fetch_add(1, Ordering::Relaxed);
            } else {
                failed.fetch_add(1, Ordering::Relaxed);
            }
            drop(permit);
        });
    }
    while tasks.join_next().await.is_some() {}

    let elapsed = started.elapsed();
    sampling.store(false, Ordering::Relaxed);
    sampler.await.expect("resource sampler should complete");
    let successful = successful.load(Ordering::Relaxed);
    let failed = failed.load(Ordering::Relaxed);
    let average_latency_ms = latency_micros.load(Ordering::Relaxed) as f64
        / requests as f64
        / 1_000.0;
    let samples = cpu_samples.load(Ordering::Relaxed);
    let average_cpu_percent = if samples == 0 {
        0.0
    } else {
        cpu_total_millipercent.load(Ordering::Relaxed) as f64 / samples as f64 / 1_000.0
    };

    println!("\nHive full-application load benchmark");
    println!("requests:            {requests}");
    println!("concurrency:         {concurrency}");
    println!("successful:          {successful}");
    println!("failed:              {failed}");
    println!("elapsed:             {:.2?}", elapsed);
    println!("throughput:          {:.0} requests/second", requests as f64 / elapsed.as_secs_f64());
    println!("average latency:     {average_latency_ms:.2} ms");
    println!("average process CPU: {average_cpu_percent:.1}%");
    println!(
        "peak process memory: {:.1} MiB",
        peak_memory_bytes.load(Ordering::Relaxed) as f64 / 1_048_576.0
    );

    assert_eq!(failed, 0, "all benchmark requests should succeed");
    assert_eq!(successful, requests);
    server.0.kill().expect("Hive server should stop");
}

async fn sample_resources(
    process_id: u32,
    sampling: Arc<AtomicBool>,
    peak_memory_bytes: Arc<AtomicU64>,
    cpu_samples: Arc<AtomicU64>,
    cpu_total_millipercent: Arc<AtomicU64>,
) {
    let mut system = System::new();
    let process_id = Pid::from_u32(process_id);
    while sampling.load(Ordering::Relaxed) {
        system.refresh_process(process_id);
        if let Some(process) = system.process(process_id) {
            peak_memory_bytes.fetch_max(process.memory(), Ordering::Relaxed);
            cpu_total_millipercent.fetch_add(
                (process.cpu_usage() * 1_000.0) as u64,
                Ordering::Relaxed,
            );
            cpu_samples.fetch_add(1, Ordering::Relaxed);
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_until_healthy(client: &reqwest::Client, base_url: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = client.get(format!("{base_url}/health")).send().await {
            if response.status().is_success() {
                return;
            }
        }
        assert!(Instant::now() < deadline, "Hive did not become healthy within 10 seconds");
        time::sleep(Duration::from_millis(50)).await;
    }
}

fn announce_url(base_url: &str, request_id: usize) -> String {
    let info_hash = percent_encoded_identifier(request_id as u64 / 50);
    let peer_id = percent_encoded_identifier(request_id as u64);
    let url = format!(
        "{base_url}/announce?info_hash={info_hash}&peer_id={peer_id}&port=6881&left=1&numwant=50"
    );
    url
}

fn percent_encoded_identifier(value: u64) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut bytes = [0_u8; 20];
    bytes[..8].copy_from_slice(&value.to_be_bytes());
    let mut encoded = String::with_capacity(bytes.len() * 3);
    for byte in bytes {
        encoded.push('%');
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn reserve_local_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("an ephemeral port should be available")
        .local_addr()
        .unwrap()
        .port()
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|value| value.parse().unwrap_or_else(|_| panic!("{name} must be a positive integer")))
        .unwrap_or(default)
}