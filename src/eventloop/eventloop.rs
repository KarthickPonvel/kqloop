use std::{collections::HashMap, fmt::Display, io, os::fd::RawFd, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}, time::{Duration, Instant}};

use crate::sys::{Event, Filter, unix::kqueue::{Kqueue, KqueueError}};

// Error
#[derive(Debug)]
pub enum EventLoopError{
    Kqueue(KqueueError),
    Io(io::Error),
    CallbackPanic(String),
    HandlerNotFound(RawFd),
    Shutdown,
}

impl From<KqueueError> for EventLoopError {
    fn from(err: KqueueError) -> Self {
        EventLoopError::Kqueue(err)
    }
}

impl From<io::Error> for EventLoopError {
    fn from(err: io::Error) -> Self {
        EventLoopError::Io(err)
    }
}

impl Display for EventLoopError{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self{
            EventLoopError::Kqueue(e) => write!(f, "kqueue error: {}", e),
            EventLoopError::Io(e) => write!(f, "io error: {}", e),
            EventLoopError::CallbackPanic(msg) => write!(f, "callback panic: {}", msg),
            EventLoopError::HandlerNotFound(fd) => write!(f, "handler not found for fd {}", fd),
            EventLoopError::Shutdown => write!(f, "event loop shutdown"),
        }
    }
}

impl std::error::Error for EventLoopError{}

pub type Result<T> = std::result::Result<T, EventLoopError>;

// Callback types for even handlers
pub type ReadCallback = Box<dyn FnMut(RawFd, &Event) -> Result<()> + Send>;
pub type WriteCallback = Box<dyn FnMut(RawFd, &Event) -> Result<()> + Send>;
pub type ErrorCallback = Box<dyn FnMut(RawFd, &EventLoopError) + Send>;
pub type TimerCallback = Box<dyn FnMut() -> Result<()> + Send>;

// Fd event
pub struct FdEvent{
    pub fd: RawFd,
    filter: Filter,
    on_read: Option<ReadCallback>,
    on_write: Option<WriteCallback>,
    on_error: Option<ErrorCallback>,
    events_processed: u64
}

impl FdEvent {
    
    pub fn new(fd: RawFd) -> Self{
        Self { 
            fd, 
            filter: Filter::empty(), 
            on_read: None, 
            on_write: None, 
            on_error: None,
            events_processed: 0
        }
    }

    pub fn has_callbacks(&self) -> bool{
        self.on_read.is_some() || self.on_write.is_some()
    }
}

// Timer event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(u64);

pub struct Timer{
    id: TimerId,
    deadline: Instant,
    interval: Option<Duration>,
    callback: TimerCallback
}

// Event loop configuration
#[derive(Debug, Clone)]
pub struct EventLoopConfig {
    pub max_events: usize,
    pub poll_timeout_ms: Option<i32>,
    pub enable_metrics: bool,
}

impl Default for EventLoopConfig {
    fn default() -> Self {
        Self {
            max_events: 1024,
            poll_timeout_ms: Some(100),
            enable_metrics: true,
        }
    }
}

// Metrics 

#[derive(Debug, Default)]
pub struct EventLoopMetrics {
    pub poll_count: AtomicU64,
    pub events_dispatched: AtomicU64,
    pub callbacks_executed: AtomicU64,
    pub errors_handled: AtomicU64,
    pub timers_fired: AtomicU64,
}

impl EventLoopMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            poll_count: self.poll_count.load(Ordering::Relaxed),
            events_dispatched: self.events_dispatched.load(Ordering::Relaxed),
            callbacks_executed: self.callbacks_executed.load(Ordering::Relaxed),
            errors_handled: self.errors_handled.load(Ordering::Relaxed),
            timers_fired: self.timers_fired.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub poll_count: u64,
    pub events_dispatched: u64,
    pub callbacks_executed: u64,
    pub errors_handled: u64,
    pub timers_fired: u64,
}

// Event loop
pub struct EventLoop{
    kqueue: Kqueue,
    handlers: HashMap<RawFd, FdEvent>,
    ready_events: Vec<Event>,
    timers: Vec<Timer>,
    next_timer_id: u64,
    config: EventLoopConfig,
    metrics: Arc<EventLoopMetrics>,
    running: Arc<AtomicBool>
}

impl EventLoop{
    pub fn new() -> Result<Self>{
        Self::with_config(EventLoopConfig::default())
    }
    
    pub fn with_config(config: EventLoopConfig) -> Result<Self>{
        Ok(Self { 
            kqueue: Kqueue::new()?, 
            handlers: HashMap::with_capacity(config.max_events), 
            ready_events: Vec::with_capacity(config.max_events), 
            timers: Vec::new(), 
            next_timer_id: 0,
            config,
            metrics: Arc::new(EventLoopMetrics::default()),
            running: Arc::new(AtomicBool::new(false)) 
        })
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn shutdown(&self){
        self.running.store(false, Ordering::Release);
    }

    // Fd registration
    pub fn register_read<F>(&mut self, fd: RawFd, callback: F) -> Result<()>
    where 
        F: FnMut(RawFd, &Event) -> Result<()> + Send + 'static,
    {
        self.register(fd, Filter::READ, Some(Box::new(callback)), None, None)
    }

    pub fn register_write<F>(&mut self, fd: RawFd, callback: F) -> Result<()>
    where 
        F: FnMut(RawFd, &Event) -> Result<()> + Send + 'static,
    {
        self.register(fd, Filter::WRITE, Some(Box::new(callback)), None, None)
    }

    pub fn register_read_write<R, W>(&mut self, fd: RawFd, read_cb: R, write_cb: W) -> Result<()>
    where 
        R: FnMut(RawFd, &Event) -> Result<()> + Send + 'static,
        W: FnMut(RawFd, &Event) -> Result<()> + Send + 'static,
    {
        self.register(fd, Filter::all(), Some(Box::new(read_cb)), Some(Box::new(write_cb)), None)
    }

    fn register(
        &mut self, 
        fd: RawFd, 
        filter: Filter,
        on_read: Option<ReadCallback>,
        on_write: Option<WriteCallback>,
        on_error: Option<ErrorCallback>,
    ) -> Result<()>{
        let handler = self.handlers.entry(fd).or_insert_with(|| FdEvent::new(fd));

        let new_filter = filter & !handler.filter;
        if !new_filter.is_empty() {
            self.kqueue.register(fd, new_filter)?;
            handler.filter.insert(new_filter);
        }

        if let Some(cb) = on_read {
            handler.on_read = Some(cb);
        }
        if let Some(cb) = on_write {
            handler.on_write = Some(cb);
        }
        if let Some(cb) = on_error {
            handler.on_error = Some(cb);
        }

        Ok(())
    }

    pub fn deregister(
        &mut self,
        fd: RawFd,
        filter: Filter
    ) -> Result<()> {
        let remove_handler = if let Some(handler) = self.handlers.get_mut(&fd){
            let to_remove = filter & handler.filter;
            if to_remove.is_empty() {
                return Ok(());
            }

            self.kqueue.deregister(fd, to_remove)?;
            handler.filter.remove(to_remove);

            if to_remove.contains(Filter::READ){
                handler.on_read = None;
            }
            if to_remove.contains(Filter::WRITE){
                handler.on_write = None;
            }

            !handler.has_callbacks()
        } else {
            return Ok(())
        };

        if remove_handler {
            self.handlers.remove(&fd);
        }
        Ok(())
    }

    pub fn update_filter(&mut self, fd: RawFd, filter: Filter) -> Result<()> {
        if let Some(handler) = self.handlers.get_mut(&fd){
            let to_add = filter & !handler.filter;
            let to_remove = handler.filter & !filter;

            if !to_add.is_empty(){
                self.kqueue.register(fd, to_add)?;
                handler.filter.insert(to_add);
            }

            if !to_remove.is_empty(){
                self.kqueue.deregister(fd, to_remove)?;
                handler.filter.remove(to_remove);
            }
        }
        Ok(())
    }

    // Timer
    pub fn add_timer<F>(&mut self, delay: Duration, callback: F) -> TimerId
    where 
        F: FnMut() -> Result<()> + Send + 'static,
    {
        let id = TimerId(self.next_timer_id);
        self.next_timer_id += 1;

        self.timers.push(Timer { 
            id, 
            deadline: Instant::now() + delay, 
            interval: None, 
            callback: Box::new(callback), 
        });

        id
    }

    pub fn add_interval<F>(&mut self, interval: Duration, callback: F) -> TimerId 
    where 
        F: FnMut() -> Result<()> + Send + 'static,
    {
        let id = TimerId(self.next_timer_id);
        self.next_timer_id += 1;
        
        self.timers.push(
            Timer { 
                id,
                deadline: Instant::now() + interval,
                interval: Some(interval),
                callback: Box::new(callback),
            }
        );
        
        id
    }

    pub fn cancel_timer(&mut self, id: TimerId) {
        self.timers.retain(|t| t.id != id);
    }

    pub fn process_timers(&mut self) -> Result<()> {
        let now = Instant::now();
        let mut i = 0;

        while i < self.timers.len(){
            if self.timers[i].deadline <= now {
                let mut timer = self.timers.swap_remove(i);

                if let Err(e) = (timer.callback)(){
                    eprintln!("Timer callback error: {}", e);
                    if self.config.enable_metrics {
                        self.metrics.errors_handled.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    if self.config.enable_metrics {
                        self.metrics.timers_fired.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Re-schedule interval timers
                if let Some(interval) = timer.interval{
                    timer.deadline = now + interval;
                    self.timers.push(timer);
                }
            }else {
                i += 1;
            }
        }
        Ok(())
    }

    pub fn next_timer_deadline(&self) -> Option<Duration>{
        let now = Instant::now();
        self.timers
            .iter()
            .map(|t| t.deadline.saturating_duration_since(now))
            .min()
    }

    // Event loop execution

    fn poll(&mut self) -> Result<usize>{
        if self.ready_events.capacity() == 0 {
            self.ready_events.reserve(self.config.max_events);
        }

        let cap = self.ready_events.capacity();
        self.ready_events.resize_with(cap, || Event { 
            fd: -1, 
            filter: Filter::empty() 
        });

        let timeout: Option<Duration> = match (
            self.config.poll_timeout_ms,
            self.next_timer_deadline(),
        ) {
            (Some(ms), Some(td)) => {
                Some(td.min(Duration::from_millis(ms as u64)))
            }
            (Some(ms), None) => {
                Some(Duration::from_millis(ms as u64))
            }
            (None, Some(td)) => Some(td),
            (None, None) => None,
        };

        let n = self.kqueue.poll(&mut self.ready_events, timeout)?;
        self.ready_events.truncate(n);

        if self.config.enable_metrics {
            self.metrics.poll_count.fetch_add(1, Ordering::Relaxed);
            self.metrics.events_dispatched.fetch_add(n as u64, Ordering::Relaxed);
        }

        Ok(n)
    }
    
    fn dispatch(&mut self) -> Result<()> {
        let events: Vec<Event> = self.ready_events.drain(..).collect();

        for event in events {
            let fd = event.fd;

    
            if let Some(handler) = self.handlers.get_mut(&fd){
                handler.events_processed += 1;


                // Handle read events
                if event.filter.contains(Filter::READ) {

                    if let Some(ref mut cb) = handler.on_read{
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            cb(fd, &event)
                        })) {
                            
                            Ok(Ok(())) =>{
                                if self.config.enable_metrics {
                                    self.metrics.callbacks_executed.fetch_add(1, Ordering::Relaxed);
                                }
                            },
                            Ok(Err(err)) => {
                                if self.config.enable_metrics {
                                    self.metrics.errors_handled.fetch_add(1, Ordering::Relaxed);
                                }
                                if let Some(ref mut err_cb) = handler.on_error {
                                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        err_cb(fd, &err);
                                    }));
                                }else{
                                    eprintln!("Read callback error on fd {}: {}", fd, err);
                                }
                            },
                            Err(_) =>{
                                let err = EventLoopError::CallbackPanic(
                                    format!("read callback panicked on fd {}", fd)
                                );

                                if let Some(ref mut err_cb) = handler.on_error{
                                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        err_cb(fd, &err);
                                    }));
                                }
                                return Err(err);
                            }
                        }
                    }
                }

                // Handle write events
                if event.filter.contains(Filter::WRITE) {
                    if let Some(ref mut cb) = handler.on_write {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            cb(fd, &event)
                        })) {
                            Ok(Ok(())) => {
                                if self.config.enable_metrics {
                                    self.metrics.callbacks_executed.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(Err(err)) => {
                                if self.config.enable_metrics {
                                    self.metrics.errors_handled.fetch_add(1, Ordering::Relaxed);
                                }
                                if let Some(ref mut err_cb) = handler.on_error {
                                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        err_cb(fd, &err);
                                    }));
                                } else {
                                    eprintln!("Write callback error on fd {}: {}", fd, err);
                                }
                            }
                            Err(_) => {
                                let err = EventLoopError::CallbackPanic(
                                    format!("write callback panicked on fd {}", fd)
                                );
                                if let Some(ref mut err_cb) = handler.on_error {
                                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        err_cb(fd, &err);
                                    }));
                                }
                                return Err(err);
                            }
                        }
                    }
                }
            }else{
                if self.config.enable_metrics {
                    self.metrics.errors_handled.fetch_add(1, Ordering::Relaxed);
                }
                eprintln!("event for unknown fd {}", fd);
                continue;
            }
        }
        Ok(())
    }

    pub fn run_once(&mut self) -> Result<bool>{
        if !self.is_running() {
            return Ok(false);
        }

        self.process_timers()?;
        self.poll()?;
        self.dispatch()?;

        Ok(true)
    }

    pub fn run(&mut self) -> Result<()> {
        self.running.store(true, Ordering::Release);

        while self.run_once()? {}
        Ok(())
    }
}


// ********************
//GENERATED USING AI
// ********************
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_event_loop_single_and_interval_timers() {
        let mut el = EventLoop::new().unwrap();

        // Shared state for counters
        let single_counter = Arc::new(Mutex::new(0));
        let interval_counter = Arc::new(Mutex::new(0));

        // Single-shot timer (fires immediately)
        let single_clone = single_counter.clone();
        el.add_timer(Duration::from_millis(0), move || {
            let mut val = single_clone.lock().unwrap();
            *val += 1;
            Ok(())
        });

        // Interval timer (fires every 1ms, stop after 3 fires)
        let interval_clone = interval_counter.clone();
        el.add_interval(Duration::from_millis(1), move || {
            let mut val = interval_clone.lock().unwrap();
            *val += 1;
        
            Ok(())
        });

        el.running.store(true, std::sync::atomic::Ordering::Release);

        // Run enough iterations to cover both timers
        // Process timers until counters are what we expect
        while *single_counter.lock().unwrap() == 0 || *interval_counter.lock().unwrap() < 3 {
            el.run_once().unwrap();
        }

        // Assertions
        assert_eq!(*single_counter.lock().unwrap(), 1, "Single-shot timer should fire once");
        assert!(*interval_counter.lock().unwrap() >= 3, "Interval timer should fire at least 3 times");
    }
}
