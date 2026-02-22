use std::{future::Future, sync::Arc, time::Duration};
use sysinfo::System;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

// --- Types ---

pub type WorkPermit = OwnedSemaphorePermit;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureLevel { Normal, Warning, Critical }

#[derive(Debug, thiserror::Error)]
pub enum RamAgentError {
    #[error("agent has been shut down")]
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
pub struct MemorySnapshot {
    pub total_bytes: u64,
    pub used_bytes: u64,
}

impl MemorySnapshot {
    pub fn usage(&self) -> f64 {
        if self.total_bytes == 0 { 0.0 } else { self.used_bytes as f64 / self.total_bytes as f64 }
    }
}

pub trait MemoryProvider: Send + 'static {
    fn snapshot(&mut self) -> MemorySnapshot;
}

pub struct SysInfoProvider(System);
impl Default for SysInfoProvider {
    fn default() -> Self { Self(System::new()) }
}
impl MemoryProvider for SysInfoProvider {
    fn snapshot(&mut self) -> MemorySnapshot {
        self.0.refresh_memory();
        MemorySnapshot { total_bytes: self.0.total_memory(), used_bytes: self.0.used_memory() }
    }
}

// --- Config ---

#[derive(Debug, Clone)]
pub struct RamAgentConfig {
    pub max_concurrency: usize,
    pub warning_threshold: f64,
    pub critical_threshold: f64,
    pub hysteresis_margin: f64,
    pub poll_interval: Duration,
    pub warning_fraction: f64,
    pub warning_delay: Duration,
}

impl Default for RamAgentConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 32,
            warning_threshold: 0.70,
            critical_threshold: 0.85,
            hysteresis_margin: 0.05,
            poll_interval: Duration::from_secs(1),
            warning_fraction: 0.50,
            warning_delay: Duration::from_millis(50),
        }
    }
}

// --- Pressure classification ---

fn classify(usage: f64, config: &RamAgentConfig, current: PressureLevel) -> PressureLevel {
    match current {
        PressureLevel::Critical if usage >= config.critical_threshold - config.hysteresis_margin => {
            PressureLevel::Critical
        }
        _ if usage >= config.critical_threshold => PressureLevel::Critical,
        _ if usage >= config.warning_threshold => PressureLevel::Warning,
        _ => PressureLevel::Normal,
    }
}

// --- Agent ---

pub struct RamAgent {
    semaphore: Arc<Semaphore>,
    shutdown_tx: watch::Sender<bool>,
    pressure_rx: watch::Receiver<PressureLevel>,
    monitor: Option<JoinHandle<()>>,
    config: RamAgentConfig,
}

impl RamAgent {
    pub fn start(config: RamAgentConfig) -> Self {
        Self::start_with_provider(config, SysInfoProvider::default())
    }

    pub fn start_with_provider(config: RamAgentConfig, provider: impl MemoryProvider) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (pressure_tx, pressure_rx) = watch::channel(PressureLevel::Normal);

        let monitor = tokio::spawn(Self::monitor(
            config.clone(), provider, semaphore.clone(), pressure_tx, shutdown_rx,
        ));

        Self { semaphore, shutdown_tx, pressure_rx, monitor: Some(monitor), config }
    }

    async fn monitor(
        config: RamAgentConfig,
        mut provider: impl MemoryProvider,
        semaphore: Arc<Semaphore>,
        pressure_tx: watch::Sender<PressureLevel>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let max = config.max_concurrency;
        let mut current_permits = max;
        let mut level = PressureLevel::Normal;
        let mut interval = tokio::time::interval(config.poll_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown_rx.changed() => { semaphore.close(); return; }
            }

            let new_level = classify(provider.snapshot().usage(), &config, level);
            if new_level == level { continue; }
            level = new_level;
            let _ = pressure_tx.send(level);

            let target = match level {
                PressureLevel::Normal => max,
                PressureLevel::Warning => ((max as f64 * config.warning_fraction).round() as usize).max(1),
                PressureLevel::Critical => 0,
            };

            if target < current_permits {
                semaphore.forget_permits(current_permits - target);
            } else {
                semaphore.add_permits(target - current_permits);
            }
            current_permits = target;
        }
    }

    pub async fn acquire_permit(&self) -> Result<WorkPermit, RamAgentError> {
        if *self.pressure_rx.borrow() == PressureLevel::Warning {
            tokio::time::sleep(self.config.warning_delay).await;
        }
        self.semaphore.clone().acquire_owned().await.map_err(|_| RamAgentError::Shutdown)
    }

    pub async fn submit<F: Future>(&self, f: F) -> Result<F::Output, RamAgentError> {
        let _permit = self.acquire_permit().await?;
        Ok(f.await)
    }

    pub async fn spawn<F, T>(&self, f: F) -> Result<JoinHandle<T>, RamAgentError>
    where F: Future<Output = T> + Send + 'static, T: Send + 'static {
        let permit = self.acquire_permit().await?;
        Ok(tokio::spawn(async move { let r = f.await; drop(permit); r }))
    }

    pub fn pressure_watcher(&self) -> watch::Receiver<PressureLevel> { self.pressure_rx.clone() }
    pub fn current_pressure(&self) -> PressureLevel { *self.pressure_rx.borrow() }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(h) = self.monitor.take() { let _ = h.await; }
    }
}

impl Drop for RamAgent {
    fn drop(&mut self) { let _ = self.shutdown_tx.send(true); }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(usage: f64) -> f64 { usage }
    fn cfg() -> RamAgentConfig { RamAgentConfig { warning_threshold: 0.70, critical_threshold: 0.85, ..Default::default() } }

    #[test]
    fn pressure_classification() {
        let c = cfg();
        assert_eq!(classify(snap(0.50), &c, PressureLevel::Normal), PressureLevel::Normal);
        assert_eq!(classify(snap(0.75), &c, PressureLevel::Normal), PressureLevel::Warning);
        assert_eq!(classify(snap(0.90), &c, PressureLevel::Normal), PressureLevel::Critical);
    }

    #[test]
    fn hysteresis() {
        let c = cfg();
        // At 82%, above 80% (critical - margin), stays critical
        assert_eq!(classify(snap(0.82), &c, PressureLevel::Critical), PressureLevel::Critical);
        // At 75%, below 80%, drops to warning
        assert_eq!(classify(snap(0.75), &c, PressureLevel::Critical), PressureLevel::Warning);
        // At 60%, drops to normal
        assert_eq!(classify(snap(0.60), &c, PressureLevel::Critical), PressureLevel::Normal);
    }
}
