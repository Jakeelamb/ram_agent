use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use ram_agent::*;

struct Mock { used: Arc<AtomicU64>, total: u64 }
impl Mock {
    fn new(total: u64, used: Arc<AtomicU64>) -> Self { Self { used, total } }
}
impl MemoryProvider for Mock {
    fn snapshot(&mut self) -> MemorySnapshot {
        MemorySnapshot { total_bytes: self.total, used_bytes: self.used.load(Ordering::Relaxed) }
    }
}

fn cfg() -> RamAgentConfig {
    RamAgentConfig {
        max_concurrency: 10,
        poll_interval: Duration::from_millis(50),
        warning_delay: Duration::from_millis(5),
        ..Default::default()
    }
}

fn agent(used_pct: u64) -> (RamAgent, Arc<AtomicU64>) {
    let used = Arc::new(AtomicU64::new(used_pct * 10)); // out of 1000
    let a = RamAgent::start_with_provider(cfg(), Mock::new(1000, used.clone()));
    (a, used)
}

async fn wait() { tokio::time::sleep(Duration::from_millis(150)).await; }

#[tokio::test]
async fn normal_full_concurrency() {
    let (a, _) = agent(50);
    assert!(a.acquire_permit().await.is_ok());
    assert_eq!(a.current_pressure(), PressureLevel::Normal);
    a.shutdown().await;
}

#[tokio::test]
async fn warning_reduces_concurrency() {
    let (a, used) = agent(50);
    wait().await;
    used.store(750, Ordering::Relaxed);
    wait().await;
    assert_eq!(a.current_pressure(), PressureLevel::Warning);
    assert!(a.acquire_permit().await.is_ok());
    a.shutdown().await;
}

#[tokio::test]
async fn critical_pauses_work() {
    let (a, used) = agent(50);
    wait().await;
    used.store(900, Ordering::Relaxed);
    wait().await;
    assert_eq!(a.current_pressure(), PressureLevel::Critical);
    assert!(tokio::time::timeout(Duration::from_millis(200), a.acquire_permit()).await.is_err());
    a.shutdown().await;
}

#[tokio::test]
async fn recovery_from_critical() {
    let (a, used) = agent(90);
    wait().await;
    assert_eq!(a.current_pressure(), PressureLevel::Critical);
    used.store(500, Ordering::Relaxed);
    wait().await;
    assert_eq!(a.current_pressure(), PressureLevel::Normal);
    assert!(a.acquire_permit().await.is_ok());
    a.shutdown().await;
}

#[tokio::test]
async fn hysteresis_prevents_oscillation() {
    let (a, used) = agent(90);
    wait().await;
    assert_eq!(a.current_pressure(), PressureLevel::Critical);
    used.store(820, Ordering::Relaxed); // above 80% threshold, stays critical
    wait().await;
    assert_eq!(a.current_pressure(), PressureLevel::Critical);
    used.store(750, Ordering::Relaxed); // below 80%, drops to warning
    wait().await;
    assert_eq!(a.current_pressure(), PressureLevel::Warning);
    a.shutdown().await;
}

#[tokio::test]
async fn concurrent_permits_bounded() {
    let (a, _) = agent(50);
    wait().await;
    let permits: Vec<_> = futures::future::join_all((0..10).map(|_| a.acquire_permit()))
        .await.into_iter().map(|r| r.unwrap()).collect();
    assert!(tokio::time::timeout(Duration::from_millis(100), a.acquire_permit()).await.is_err());
    drop(permits);
    assert!(a.acquire_permit().await.is_ok());
    a.shutdown().await;
}

#[tokio::test]
async fn pressure_watcher() {
    let (a, used) = agent(50);
    let mut w = a.pressure_watcher();
    used.store(750, Ordering::Relaxed);
    assert!(tokio::time::timeout(Duration::from_millis(300), w.changed()).await.is_ok());
    assert_eq!(*w.borrow(), PressureLevel::Warning);
    a.shutdown().await;
}

#[tokio::test]
async fn submit_and_spawn() {
    let (a, _) = agent(50);
    assert_eq!(a.submit(async { 42 }).await.unwrap(), 42);
    assert_eq!(a.spawn(async { 99 }).await.unwrap().await.unwrap(), 99);
    a.shutdown().await;
}
