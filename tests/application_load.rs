use std::{
    env, fs,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use sysinfo::{Pid, System};
use tempfile::TempDir;
use tokio::{sync::Semaphore, time};

const DEFAULT_REQUESTS: usize = 100_000;
const DEFAULT_CONCURRENCY: usize = 2_000;

struct BenchmarkResult {
    requests: usize,
    concurrency: usize,
    successful: usize,
    failed: usize,
    elapsed: Duration,
    throughput: f64,
    average_latency_ms: f64,
    average_cpu_percent: f64,
    peak_memory_mib: f64,
    request_metrics: Vec<RequestMetric>,
}

struct RequestMetric {
    request_id: usize,
    succeeded: bool,
    completed_micros: u64,
    latency_micros: u64,
    cpu_millipercent: u64,
    memory_bytes: u64,
}

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
    let output_directory = env::var_os("HIVE_BENCH_OUTPUT_DIRECTORY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("benchmark-results"));
    let data_path = output_directory.join("benchmark-results.dat");
    let request_data_path = output_directory.join("benchmark-requests.dat");
    let gnuplot_path = output_directory.join("benchmark-results.gnuplot");
    initialize_output(
        &output_directory,
        &data_path,
        &request_data_path,
        &gnuplot_path,
    );

    let result = run_benchmark(requests, concurrency).await;
    print_result(&result);
    append_result(&data_path, &result);
    write_request_metrics(&request_data_path, &result.request_metrics);
    assert_eq!(
        result.successful + result.failed,
        requests,
        "every benchmark request should be accounted for"
    );

    println!("\nGnuplot data: {}", data_path.display());
    println!("Per-request data: {}", request_data_path.display());
    println!("Gnuplot script: {}", gnuplot_path.display());
}

async fn run_benchmark(requests: usize, concurrency: usize) -> BenchmarkResult {
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
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--protocol",
            "http",
        ])
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
    let current_cpu_millipercent = Arc::new(AtomicU64::new(0));
    let current_memory_bytes = Arc::new(AtomicU64::new(0));
    let sampler = tokio::spawn(sample_resources(
        process_id,
        Arc::clone(&sampling),
        Arc::clone(&peak_memory_bytes),
        Arc::clone(&cpu_samples),
        Arc::clone(&cpu_total_millipercent),
        Arc::clone(&current_cpu_millipercent),
        Arc::clone(&current_memory_bytes),
    ));

    let permits = Arc::new(Semaphore::new(concurrency));
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();

    for request_id in 0..requests {
        let permit = Arc::clone(&permits).acquire_owned().await.unwrap();
        let client = client.clone();
        let current_cpu_millipercent = Arc::clone(&current_cpu_millipercent);
        let current_memory_bytes = Arc::clone(&current_memory_bytes);
        let url = announce_url(&base_url, request_id);
        tasks.spawn(async move {
            let request_started = Instant::now();
            let succeeded = match client.get(url).send().await {
                Ok(response) if response.status().is_success() => response.bytes().await.is_ok(),
                _ => false,
            };
            let metric = RequestMetric {
                request_id,
                succeeded,
                completed_micros: started.elapsed().as_micros() as u64,
                latency_micros: request_started.elapsed().as_micros() as u64,
                cpu_millipercent: current_cpu_millipercent.load(Ordering::Relaxed),
                memory_bytes: current_memory_bytes.load(Ordering::Relaxed),
            };
            drop(permit);
            metric
        });
    }
    let mut request_metrics = Vec::with_capacity(requests);
    while let Some(result) = tasks.join_next().await {
        request_metrics.push(result.expect("benchmark request task should complete"));
    }
    request_metrics.sort_unstable_by_key(|metric| metric.completed_micros);

    let elapsed = started.elapsed();
    sampling.store(false, Ordering::Relaxed);
    sampler.await.expect("resource sampler should complete");
    let successful = request_metrics
        .iter()
        .filter(|metric| metric.succeeded)
        .count();
    let failed = requests - successful;
    let average_latency_ms = request_metrics
        .iter()
        .map(|metric| metric.latency_micros)
        .sum::<u64>() as f64
        / requests as f64
        / 1_000.0;
    let samples = cpu_samples.load(Ordering::Relaxed);
    let average_cpu_percent = if samples == 0 {
        0.0
    } else {
        cpu_total_millipercent.load(Ordering::Relaxed) as f64 / samples as f64 / 1_000.0
    };

    server.0.kill().expect("Hive server should stop");
    BenchmarkResult {
        requests,
        concurrency,
        successful,
        failed,
        elapsed,
        throughput: requests as f64 / elapsed.as_secs_f64(),
        average_latency_ms,
        average_cpu_percent,
        peak_memory_mib: peak_memory_bytes.load(Ordering::Relaxed) as f64 / 1_048_576.0,
        request_metrics,
    }
}

fn initialize_output(
    output_directory: &Path,
    data_path: &Path,
    request_data_path: &Path,
    gnuplot_path: &Path,
) {
    fs::create_dir_all(output_directory).expect("benchmark output directory should be created");
    fs::write(
        data_path,
        "# Hive full-application benchmark result\n\
# requests concurrency successful failed throughput_rps latency_ms cpu_percent memory_mib\n",
    )
    .expect("benchmark data file should be initialized");
    fs::write(
        request_data_path,
        "# Per-request Hive benchmark metrics in completion order\n\
# completion_index request_id completed_ms success latency_ms cumulative_rps cpu_percent memory_mib\n",
    )
    .expect("per-request benchmark data file should be initialized");

    let request_data_path = gnuplot_path_string(request_data_path);
    let image_path = gnuplot_path_string(&output_directory.join("benchmark-results.png"));
    let script = format!(
        r#"set terminal pngcairo size 1600,1000 enhanced font 'Arial,10'
set output '{image_path}'
set datafile separator whitespace
set key outside right
set grid
set xlabel 'Completed requests'
set multiplot layout 2,2 title 'Hive per-request load metrics'

set ylabel 'Request latency (ms)'
plot '{request_data_path}' using 1:($4 == 1 ? $5 : 1/0) with points pointtype 7 pointsize 0.2 title 'Successful', \
    '{request_data_path}' using 1:($4 == 0 ? $5 : 1/0) with points pointtype 7 pointsize 0.8 linecolor rgb 'red' title 'Failed'

set ylabel 'Cumulative throughput (requests/second)'
plot '{request_data_path}' using 1:6 with lines title 'Throughput'

set ylabel 'Server process CPU (%)'
plot '{request_data_path}' using 1:7 with lines title 'CPU'

set ylabel 'Server process memory (MiB)'
plot '{request_data_path}' using 1:8 with lines title 'Memory'

unset multiplot
"#,
    );
    fs::write(gnuplot_path, script).expect("gnuplot script should be written");
}

fn append_result(data_path: &Path, result: &BenchmarkResult) {
    let mut data_file = OpenOptions::new()
        .append(true)
        .open(data_path)
        .expect("benchmark data file should open");
    writeln!(
        data_file,
        "{} {} {} {} {:.0} {:.2} {:.1} {:.1}",
        result.requests,
        result.concurrency,
        result.successful,
        result.failed,
        result.throughput,
        result.average_latency_ms,
        result.average_cpu_percent,
        result.peak_memory_mib,
    )
    .expect("benchmark result should be written");
}

fn write_request_metrics(data_path: &Path, metrics: &[RequestMetric]) {
    let mut data_file = OpenOptions::new()
        .append(true)
        .open(data_path)
        .expect("per-request benchmark data file should open");
    for (index, metric) in metrics.iter().enumerate() {
        let completion_index = index + 1;
        let completed_ms = metric.completed_micros as f64 / 1_000.0;
        let cumulative_rps =
            completion_index as f64 * 1_000_000.0 / metric.completed_micros.max(1) as f64;
        writeln!(
            data_file,
            "{} {} {:.3} {} {:.3} {:.1} {:.1} {:.1}",
            completion_index,
            metric.request_id,
            completed_ms,
            usize::from(metric.succeeded),
            metric.latency_micros as f64 / 1_000.0,
            cumulative_rps,
            metric.cpu_millipercent as f64 / 1_000.0,
            metric.memory_bytes as f64 / 1_048_576.0,
        )
        .expect("per-request benchmark metric should be written");
    }
}

fn print_result(result: &BenchmarkResult) {
    println!("\nHive full-application load benchmark");
    println!("requests:            {}", result.requests);
    println!("concurrency:         {}", result.concurrency);
    println!("successful:          {}", result.successful);
    println!("failed:              {}", result.failed);
    println!("elapsed:             {:.2?}", result.elapsed);
    println!(
        "throughput:          {:.0} requests/second",
        result.throughput
    );
    println!("average latency:     {:.2} ms", result.average_latency_ms);
    println!("average process CPU: {:.1}%", result.average_cpu_percent);
    println!("peak process memory: {:.1} MiB", result.peak_memory_mib);
}

fn env_usize(name: &str, default: usize) -> usize {
    match env::var(name) {
        Ok(value) => parse_positive_usize(name, &value),
        Err(env::VarError::NotPresent) => default,
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must contain valid Unicode"),
    }
}

fn parse_positive_usize(name: &str, value: &str) -> usize {
    let parsed = value
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{name} must be a positive integer"));
    assert!(parsed > 0, "{name} must be greater than zero");
    parsed
}

fn gnuplot_path_string(path: &Path) -> String {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .expect("current directory should be available")
            .join(path)
    };
    path.display()
        .to_string()
        .replace('\\', "/")
        .replace('\'', "''")
}

async fn sample_resources(
    process_id: u32,
    sampling: Arc<AtomicBool>,
    peak_memory_bytes: Arc<AtomicU64>,
    cpu_samples: Arc<AtomicU64>,
    cpu_total_millipercent: Arc<AtomicU64>,
    current_cpu_millipercent: Arc<AtomicU64>,
    current_memory_bytes: Arc<AtomicU64>,
) {
    let mut system = System::new();
    let process_id = Pid::from_u32(process_id);
    while sampling.load(Ordering::Relaxed) {
        system.refresh_process(process_id);
        if let Some(process) = system.process(process_id) {
            let memory_bytes = process.memory();
            let cpu_millipercent = (process.cpu_usage() * 1_000.0) as u64;
            peak_memory_bytes.fetch_max(memory_bytes, Ordering::Relaxed);
            current_memory_bytes.store(memory_bytes, Ordering::Relaxed);
            current_cpu_millipercent.store(cpu_millipercent, Ordering::Relaxed);
            cpu_total_millipercent.fetch_add(cpu_millipercent, Ordering::Relaxed);
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
        assert!(
            Instant::now() < deadline,
            "Hive did not become healthy within 10 seconds"
        );
        time::sleep(Duration::from_millis(50)).await;
    }
}

fn announce_url(base_url: &str, request_id: usize) -> String {
    let info_hash = percent_encoded_identifier(request_id as u64 / 50);
    let peer_id = percent_encoded_identifier(request_id as u64);
    let url = format!(
        "{base_url}/announce?info_hash={info_hash}&peer_id={peer_id}&port=6881&uploaded=0&downloaded=0&left=1&numwant=50"
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
