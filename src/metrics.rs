use crc::{CRC_16_IBM_SDLC, Crc};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use tentacle_metrics::pb::{Bucket, Metric, MetricPayload, metric};

pub const X25: Crc<u16> = Crc::<u16>::new(&CRC_16_IBM_SDLC);

const METRIC_TYPE_COUNTER: i32 = metric::Type::Counter as i32;
const METRIC_TYPE_GAUGE: i32 = metric::Type::Gauge as i32;
const METRIC_TYPE_HISTOGRAM: i32 = metric::Type::Histogram as i32;

const BUCKET_BOUNDARIES_US: &[u64] = &[
    5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000,
    10_000_000,
];

const BUCKET_COUNT: usize = BUCKET_BOUNDARIES_US.len() + 1;

fn init_atomic_buckets() -> [AtomicU64; BUCKET_COUNT] {
    std::array::from_fn(|_| AtomicU64::new(0))
}

pub struct MetricsManager {
    enabled: bool,
    active_connections: AtomicU64,
    connection_attempts_total: AtomicU64,
    connection_failures_total: AtomicU64,
    bytes_transmitted_total: AtomicU64,
    transport_latency_seconds: [AtomicU64; BUCKET_COUNT],
}

pub struct MetricsSnapshot {
    pub tentacle_id: String,
    pub service: String,
    pub timestamp_ms: i64,

    pub active_connections: u64,
    pub connection_attempts_delta: u64,
    pub connection_failures_delta: u64,
    pub bytes_transmitted_delta: u64,
    pub transport_latency_seconds: [u64; BUCKET_COUNT],
}

impl MetricsSnapshot {
    pub fn default(tentacle_id: String, service: String) -> MetricsSnapshot {
        MetricsSnapshot {
            tentacle_id,
            service,
            timestamp_ms: 0,
            active_connections: 0,
            connection_attempts_delta: 0,
            connection_failures_delta: 0,
            bytes_transmitted_delta: 0,
            transport_latency_seconds: [0u64; BUCKET_COUNT],
        }
    }
}

impl MetricsManager {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            active_connections: AtomicU64::new(0),
            connection_attempts_total: AtomicU64::new(0),
            connection_failures_total: AtomicU64::new(0),
            bytes_transmitted_total: AtomicU64::new(0),
            transport_latency_seconds: init_atomic_buckets(),
        }
    }

    pub fn add_active_connection(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_active_connection(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    pub fn add_attempts_total(&self) {
        if !self.enabled {
            return;
        }
        self.connection_attempts_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_failures_total(&self) {
        if !self.enabled {
            return;
        }
        self.connection_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_bytes_transmitted_total(&self, bytes: u64) {
        if !self.enabled {
            return;
        }
        self.bytes_transmitted_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn observe_duration(&self, duration: u64) {
        if !self.enabled {
            return;
        }
        let idx = BUCKET_BOUNDARIES_US
            .iter()
            .position(|&le| duration <= le)
            .unwrap_or(BUCKET_BOUNDARIES_US.len());
        for bucket in self.transport_latency_seconds.iter().skip(idx) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn take_snapshot(&self, snapshot: &mut MetricsSnapshot) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut transport_latency_seconds = [0u64; BUCKET_COUNT];

        for (dest, src) in transport_latency_seconds
            .iter_mut()
            .zip(&self.transport_latency_seconds)
        {
            *dest = src.load(Ordering::Relaxed);
        }

        snapshot.timestamp_ms = timestamp;
        snapshot.active_connections = self.active_connections.load(Ordering::Relaxed);
        snapshot.connection_attempts_delta = self.connection_attempts_total.load(Ordering::Relaxed);
        snapshot.connection_failures_delta = self.connection_failures_total.load(Ordering::Relaxed);
        snapshot.bytes_transmitted_delta = self.bytes_transmitted_total.load(Ordering::Relaxed);
        snapshot.transport_latency_seconds = transport_latency_seconds;
    }

    pub fn commit_sent_metrics(&self, snapshot: &MetricsSnapshot) {
        self.connection_attempts_total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v - snapshot.connection_attempts_delta)
            })
            .ok();
        self.connection_failures_total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v - snapshot.connection_failures_delta)
            })
            .ok();

        self.bytes_transmitted_total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v - snapshot.bytes_transmitted_delta)
            })
            .ok();

        for i in 0..BUCKET_COUNT {
            self.transport_latency_seconds[i]
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v - snapshot.transport_latency_seconds[i])
                })
                .ok();
        }
    }

    pub fn encode_to_binary(
        snapshot: &MetricsSnapshot,
        payload_buf: &mut Vec<u8>,
        frame: &mut Vec<u8>,
    ) {
        payload_buf.clear();
        frame.clear();

        let mut payload_metrics = Vec::with_capacity(5);

        let build_pb_buckets = |counts: &[u64; BUCKET_COUNT]| -> Vec<Bucket> {
            let mut pb_buckets = Vec::with_capacity(BUCKET_COUNT);
            for (i, &count) in counts.iter().enumerate() {
                let le = if i < BUCKET_BOUNDARIES_US.len() {
                    BUCKET_BOUNDARIES_US[i] as f64 / 1_000_000.0
                } else {
                    f64::INFINITY
                };
                pb_buckets.push(Bucket { le, count });
            }
            pb_buckets
        };

        let mut add_metric = |name: &str, m_type: i32, value: f64, buckets: Vec<Bucket>| {
            payload_metrics.push(Metric {
                name: name.to_string(),
                r#type: m_type,
                value,
                labels: std::collections::HashMap::new(),
                buckets,
            });
        };

        add_metric(
            "tentacle_active_connections",
            METRIC_TYPE_GAUGE,
            snapshot.active_connections as f64,
            vec![],
        );

        add_metric(
            "tentacle_connection_attempts_total",
            METRIC_TYPE_COUNTER,
            snapshot.connection_attempts_delta as f64,
            vec![],
        );
        add_metric(
            "tentacle_connection_failures_total",
            METRIC_TYPE_COUNTER,
            snapshot.connection_failures_delta as f64,
            vec![],
        );
        add_metric(
            "tentacle_bytes_transmitted_total",
            METRIC_TYPE_COUNTER,
            snapshot.bytes_transmitted_delta as f64,
            vec![],
        );

        add_metric(
            "tentacle_transport_latency_seconds",
            METRIC_TYPE_HISTOGRAM,
            0.0,
            build_pb_buckets(&snapshot.transport_latency_seconds),
        );

        let payload = MetricPayload {
            tentacle_id: snapshot.tentacle_id.clone(),
            service: snapshot.service.clone(),
            timestamp_ms: snapshot.timestamp_ms,
            metrics: payload_metrics,
        };

        payload.encode(payload_buf).unwrap();

        frame.reserve(6 + payload_buf.len() + 2);
        frame.push(0xBE); // Magic
        frame.push(0x01); // Version
        frame.extend_from_slice(&(payload_buf.len() as u32).to_be_bytes()); // Length
        frame.extend_from_slice(payload_buf);

        let checksum = X25.checksum(frame); // CRC16
        frame.extend_from_slice(&checksum.to_be_bytes());
    }
}
