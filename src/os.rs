//! This module is just a facade to the os-specific implementations.

#[cfg(unix)]
pub use crate::unix::*;

#[cfg(windows)]
pub use crate::windows::*;

/// Returns a granularity that is used for rounding values given by the user
/// regarding lengths and capacities.
///
/// The value is based on the required or recommended alignment of virtual
/// mappings on the operating system.
pub fn granularity() -> usize {
    get_alignment()
}
