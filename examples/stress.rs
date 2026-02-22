use std::time::Duration;
use ram_agent::{PressureLevel, RamAgent, RamAgentConfig};

/// Simulates memory-heavy work. Run this to see throttling in action.
/// While it runs, eat RAM in another terminal:
///   stress-ng --vm 1 --vm-bytes 80% -t 10s
#[tokio::main]
async fn main() {
    let agent = RamAgent::start(RamAgentConfig {
        max_concurrency: 8,
        warning_threshold: 0.70,
        critical_threshold: 0.85,
        poll_interval: Duration::from_millis(250),
        ..Default::default()
    });

    println!("ram_agent stress test — 8 max concurrency");
    println!("tip: run `stress-ng --vm 1 --vm-bytes 80% -t 10s` in another terminal\n");

    // Watcher prints pressure transitions
    let mut watcher = agent.pressure_watcher();
    tokio::spawn(async move {
        while watcher.changed().await.is_ok() {
            let level = *watcher.borrow();
            let label = match level {
                PressureLevel::Normal   => "\x1b[32mNormal\x1b[0m   — full speed",
                PressureLevel::Warning  => "\x1b[33mWarning\x1b[0m  — throttled to 50%",
                PressureLevel::Critical => "\x1b[31mCritical\x1b[0m — paused",
            };
            println!("[pressure] {label}");
        }
    });

    // Continuously submit work for 15 seconds
    let start = tokio::time::Instant::now();
    let mut task_id = 0u32;

    while start.elapsed() < Duration::from_secs(15) {
        let id = task_id;
        task_id += 1;

        // This blocks if we're in Critical — that's the whole point
        match agent.acquire_permit().await {
            Ok(permit) => {
                tokio::spawn(async move {
                    println!("  [{:>5.1}s] task {id} running", start.elapsed().as_secs_f64());
                    // Simulate work
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    drop(permit);
                });
            }
            Err(e) => {
                println!("agent error: {e}");
                break;
            }
        }

        // Small gap between submissions so output is readable
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("\n{task_id} tasks submitted in 15s");
    agent.shutdown().await;
}
