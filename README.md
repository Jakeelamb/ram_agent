# ram_agent

Background RAM monitor that throttles async concurrency based on real-time memory pressure. Add it to any Rust project to keep the system at max throughput without OOM or swap thrashing.

## How it works

A background tokio task polls system memory and classifies pressure into three levels:

| Level | Default Threshold | Effect |
|-------|------------------|--------|
| **Normal** | < 70% RAM | Full concurrency |
| **Warning** | 70–85% RAM | Concurrency halved, delay between permits |
| **Critical** | > 85% RAM | All new work paused until memory recovers |

Hysteresis prevents oscillation — after entering Critical, memory must drop 5% below the threshold before resuming.

Under the hood it's just a dynamically-resized `tokio::Semaphore`. In-flight work is never cancelled.

## Usage

```toml
[dependencies]
ram_agent = { git = "https://github.com/Jakeelamb/ram_agent" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use ram_agent::{RamAgent, RamAgentConfig};

#[tokio::main]
async fn main() {
    let agent = RamAgent::start(RamAgentConfig::default());

    // Gate work behind a permit
    let _permit = agent.acquire_permit().await.unwrap();
    do_heavy_work().await;
    drop(_permit);

    // Or submit directly
    let result = agent.submit(do_heavy_work()).await.unwrap();

    // Or spawn as a task
    let handle = agent.spawn(do_heavy_work()).await.unwrap();

    agent.shutdown().await;
}
```

## Config

All fields have sensible defaults. Override what you need:

```rust
RamAgentConfig {
    max_concurrency: 16,
    warning_threshold: 0.80,
    critical_threshold: 0.95,
    poll_interval: Duration::from_millis(500),
    ..Default::default()
}
```

## Reactive pressure watching

```rust
let mut watcher = agent.pressure_watcher();
tokio::spawn(async move {
    while watcher.changed().await.is_ok() {
        println!("pressure: {:?}", *watcher.borrow());
    }
});
```

## Examples

```bash
cargo run --example demo     # basic: 20 tasks gated by permits
cargo run --example stress   # 15s continuous load with colored pressure output
```

To see throttling in action during the stress test:

```bash
stress-ng --vm 1 --vm-bytes 80% -t 10s
```

## Testing

```bash
cargo test
```

Uses a mock `MemoryProvider` to simulate pressure transitions without touching real memory.
