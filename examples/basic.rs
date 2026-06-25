use kqloop::EventLoop;
use std::time::Duration;

fn main() -> kqloop::Result<()> {
    let mut el = EventLoop::new()?;

    el.register_read(0, |fd, _event| {
        let mut buf = [0u8; 1024];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as _, buf.len()) };
        if n > 0 {
            let input = std::str::from_utf8(&buf[..n as usize]).unwrap_or("");
            print!("read: {}", input);
        }
        Ok(())
    })?;

    let handle = el.shutdown_handle();
    el.add_timer(Duration::from_secs(10), move || {
        println!("shutting down");
        handle.shutdown();
        Ok(())
    });

    println!("listening on stdin for 10s...");
    el.run()
}
