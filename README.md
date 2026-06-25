# kqloop

A low-level event loop library for Unix systems, built in Rust using raw kqueue syscalls.

Designed for efficient, panic-resilient, and observable I/O event handling without depending on a full async runtime.

## Features

- **fd registration** — register read and write callbacks on any file descriptor
- **Single-shot timers** — fire a callback once after a delay
- **Interval timers** — fire a callback repeatedly at a fixed interval
- **Deadline-aware polling** — poll timeout adapts to the nearest timer deadline, no busy-spinning
- **Panic-safe dispatch** — callbacks are wrapped in `catch_unwind`; a panicking callback is isolated and cannot crash the loop
- **Per-fd error handlers** — attach an error callback per fd to handle failures gracefully
- **Metrics** — Arc-shared `AtomicU64` counters tracking polls, dispatched events, callbacks executed, and timer fires — zero lock overhead
- **Graceful shutdown** — stop the loop cleanly via a `ShutdownHandle`

## Usage

```rust
use kqloop::{EventLoop, ShutdownHandle};
use std::time::Duration;

fn main() -> kqloop::Result<()> {
    let mut el = EventLoop::new()?;

    // Single-shot timer
    el.add_timer(Duration::from_secs(2), || {
        println!("fired after 2s");
        Ok(())
    });

    // Interval timer
    el.add_interval(Duration::from_millis(500), || {
        println!("heartbeat");
        Ok(())
    });

    // Register read on stdin (fd 0)
    el.register_read(0, |fd, _event| {
        println!("fd {} is readable", fd);
        Ok(())
    })?;

    // Shutdown after 10s
    let handle = el.shutdown_handle();
    el.add_timer(Duration::from_secs(10), move || {
        handle.shutdown();
        Ok(())
    });

    el.run()
}
```

See [`examples/`](examples/) for more usage patterns.

## Platform

Unix only — macOS and BSD. Requires kqueue support.

## License

MIT