use kqloop::EventLoop;
use std::time::Duration;

fn main() -> kqloop::Result<()> {
    let mut el = EventLoop::new()?;

    el.add_timer(Duration::from_secs(1), || {
        println!("single-shot fired after 1s");
        Ok(())
    });

    let mut count = 0u64;
    el.add_interval(Duration::from_millis(500), move || {
        count += 1;
        println!("tick #{}", count);
        Ok(())
    });

    let handle = el.shutdown_handle();
    el.add_timer(Duration::from_secs(5), move || {
        println!("shutting down after 5s");
        handle.shutdown();
        Ok(())
    });

    el.run()
}
