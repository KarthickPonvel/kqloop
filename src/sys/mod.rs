use std::os::fd::RawFd;

pub mod unix;

/// Interest mask (READ / WRITE)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Filter(u8);

impl Filter {
    pub const NONE: Self  = Self(0);
    pub const READ: Self  = Self(1 << 0);
    pub const WRITE: Self = Self(1 << 1);
    pub const BOTH: Self  = Self(Self::READ.0 | Self::WRITE.0);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }
}

/// Event returned by kqueue
#[derive(Debug)]
pub struct Event {
    pub fd: RawFd,
    pub filter: Filter,
}