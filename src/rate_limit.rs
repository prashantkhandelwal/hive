use std::{
    net::IpAddr,
    sync::Mutex,
    time::{Duration, Instant},
};

use dashmap::DashMap;

struct Bucket {
    tokens: f64,
    updated_at: Instant,
}

pub struct RateLimiter {
    buckets: DashMap<IpAddr, Mutex<Bucket>>,
    capacity: f64,
    refill_per_second: f64,
}

impl RateLimiter {
    pub fn per_minute(limit: u32) -> Self {
        let capacity = f64::from(limit.max(1));
        Self {
            buckets: DashMap::new(),
            capacity,
            refill_per_second: capacity / 60.0,
        }
    }

    pub fn check(&self, ip: IpAddr) -> bool {
        let entry = self.buckets.entry(ip).or_insert_with(|| {
            Mutex::new(Bucket {
                tokens: self.capacity,
                updated_at: Instant::now(),
            })
        });
        let Ok(mut bucket) = entry.lock() else {
            return false;
        };
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.updated_at).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_per_second).min(self.capacity);
        bucket.updated_at = now;
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }

    pub fn remove_idle(&self) {
        self.buckets.retain(|_, bucket| {
            bucket
                .lock()
                .map(|value| value.updated_at.elapsed() < Duration::from_secs(300))
                .unwrap_or(false)
        });
    }
}
