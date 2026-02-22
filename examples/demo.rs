use std::time::Duration;
use ram_agent::{PressureLevel, RamAgent, RamAgentConfig};

#[tokio::main]
async fn main() {
    let agent = RamAgent::start(RamAgentConfig {
        poll_interval: Duration::from_millis(500),
        ..Default::default()
    });

    println!("ram_agent running — polling every 500ms");
    println!("pressure: {:?}\n", agent.current_pressure());

    // Spawn a watcher that prints pressure changes
    let mut watcher = agent.pressure_watcher();
    tokio::spawn(async move {
        while watcher.changed().await.is_ok() {
            let level = *watcher.borrow();
            match level {
                PressureLevel::Normal   => println!("[pressure] Normal — full concurrency"),
                PressureLevel::Warning  => println!("[pressure] Warning — throttling to 50%"),
                PressureLevel::Critical => println!("[pressure] Critical — all new work paused"),
            }
        }
    });

    // Simulate 20 units of work gated by the agent
    let mut handles = Vec::new();
    for i in 0..20 {
        let handle = agent.spawn(async move {
            println!("  task {i:>2} started");
            tokio::time::sleep(Duration::from_millis(200)).await;
            println!("  task {i:>2} done");
            i
        }).await.unwrap();
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    let sum: i32 = results.into_iter().map(|r| r.unwrap()).sum();
    println!("\nall tasks complete — sum: {sum}");
    println!("final pressure: {:?}", agent.current_pressure());

    agent.shutdown().await;
}
