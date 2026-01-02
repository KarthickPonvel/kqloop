use std::os::fd::RawFd;

pub mod unix;

use bitflags::bitflags;

bitflags! {
    /// Interest mask (READ / WRITE)
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub struct Filter: u8 {
        const READ  = 1 << 0;
        const WRITE = 1 << 1;
    }
}

/// Event returned by kqueue
#[derive(Debug)]
pub struct Event {
    pub fd: RawFd,
    pub filter: Filter,
}