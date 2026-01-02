use std::io;
use std::mem;
use std::os::fd::RawFd;
use std::time::Duration;

use libc::{
    kevent, kqueue, timespec,
    EV_ADD, EV_DELETE,
    EVFILT_READ, EVFILT_WRITE,
};

use crate::sys::Event;
use crate::sys::Filter;


/// kqueue-specific errors
#[derive(Debug)]
pub enum KqueueError {
    Create(io::Error),
    Kevent(io::Error),
}

impl std::fmt::Display for KqueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KqueueError::Create(err) => {
                write!(f, "kqueue creation failed: {err}")
            }
            KqueueError::Kevent(err) => {
                write!(f, "kevent call failed: {err}")
            }
        }
    }
}

impl std::error::Error for KqueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KqueueError::Create(err) => Some(err),
            KqueueError::Kevent(err) => Some(err),
        }
    }
}

/// kqueue wrapper
pub struct Kqueue {
    fd: RawFd,
}

impl Kqueue {
    /// Create a new kqueue instance
    pub fn new() -> Result<Self, KqueueError> {
        let fd = unsafe { kqueue() };
        if fd < 0 {
            return Err(KqueueError::Create(io::Error::last_os_error()));
        }

        Ok(Self { fd })
    }

    /// Register interest
    pub fn register(&self, fd: RawFd, filter: Filter) -> Result<(), KqueueError> {
        let mut changes = Vec::new();

        if filter.contains(Filter::READ) {
            changes.push(Self::kevent(fd, EVFILT_READ, EV_ADD));
        }

        if filter.contains(Filter::WRITE) {
            changes.push(Self::kevent(fd, EVFILT_WRITE, EV_ADD));
        }

        self.apply_changes(&changes)
    }

    /// Deregister interest
    pub fn deregister(&self, fd: RawFd, filter: Filter) -> Result<(), KqueueError> {
        let mut changes = Vec::new();

        if filter.contains(Filter::READ) {
            changes.push(Self::kevent(fd, EVFILT_READ, EV_DELETE));
        }

        if filter.contains(Filter::WRITE) {
            changes.push(Self::kevent(fd, EVFILT_WRITE, EV_DELETE));
        }

        self.apply_changes(&changes)
    }

    /// Poll events
    pub fn poll(
        &self,
        events: &mut Vec<Event>,
        timeout: Option<Duration>,
    ) -> Result<usize, KqueueError> {
        let mut kevents: [libc::kevent; 1024] =
            unsafe { mem::zeroed() };

        let ts = timeout.map(|d| timespec {
            tv_sec: d.as_secs() as _,
            tv_nsec: d.subsec_nanos() as _,
        });

        let n = unsafe {
            kevent(
                self.fd,
                std::ptr::null(),
                0,
                kevents.as_mut_ptr(),
                kevents.len() as _,
                ts.as_ref().map_or(std::ptr::null(), |t| t),
            )
        };

        if n < 0 {
            return Err(KqueueError::Kevent(io::Error::last_os_error()));
        }

        events.clear();

        for kev in kevents.iter().take(n as usize) {
            let filter = match kev.filter {
                EVFILT_READ => Filter::READ,
                EVFILT_WRITE => Filter::WRITE,
                _ => continue,
            };

            events.push(Event {
                fd: kev.ident as RawFd,
                filter,
            });
        }

        Ok(n as usize)
    }

    #[inline]
    fn apply_changes(&self, changes: &[libc::kevent]) -> Result<(), KqueueError> {
        let res = unsafe {
            kevent(
                self.fd,
                changes.as_ptr(),
                changes.len() as _,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };

        if res < 0 {
            Err(KqueueError::Kevent(io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn kevent(fd: RawFd, filter: i16, flags: u16) -> libc::kevent {
        libc::kevent {
            ident: fd as _,
            filter,
            flags,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        }
    }
}

impl Drop for Kqueue {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}