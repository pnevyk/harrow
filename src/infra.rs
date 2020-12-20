//! Internals used to implement [`FileMut`](crate::FileMut) and
//! [`FileRef`](crate::FileRef).

use std::fmt;
use std::io;
use std::path::Path;

use crate::align::{align_add, align_sub, ALIGNMENT};
use crate::cache::{Cache, ViewMut, ViewRef};
use crate::os;

/// File wrapper that manages a cache of virtual mapping used for acquiring
/// parts of the file.
///
/// If the file is opened as read-only, all methods taking exclusive reference
/// panic.
pub struct File {
    raw: os::RawFile,
    cache: Cache,
    mode: Mode,
    cache_block_size: usize,
}

impl File {
    pub fn open_writable(
        path: &Path,
        len: usize,
        cache_capacity: usize,
        cache_block_size: usize,
    ) -> io::Result<Self> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "len must be greater than zero",
            ));
        }

        let len = align_add(len);
        let raw = os::RawFile::open_writable(path, len)?;

        Ok(Self {
            raw,
            cache: Cache::with_capacity(cache_capacity),
            mode: Mode::Writable,
            cache_block_size: fix_cache_block_size(cache_block_size),
        })
    }

    pub fn open_readonly(
        path: &Path,
        cache_capacity: usize,
        cache_block_size: usize,
    ) -> io::Result<Self> {
        let raw = os::RawFile::open_readonly(path)?
            .ok_or(io::Error::new(io::ErrorKind::InvalidData, "file is empty"))?;

        Ok(Self {
            raw,
            cache: Cache::with_capacity(cache_capacity),
            mode: Mode::Readonly,
            cache_block_size: fix_cache_block_size(cache_block_size),
        })
    }

    pub fn resize(&mut self, new_len: usize) -> io::Result<()> {
        assert_eq!(
            self.mode,
            Mode::Writable,
            "underlying file was opened as read-only"
        );

        if new_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "new_len must be greater than zero",
            ));
        }

        let old_len = self.raw.len();
        let new_len = align_add(new_len);

        if old_len == new_len {
            return Ok(());
        }

        // Resize the underlying file.
        self.raw.resize(new_len)
    }

    pub fn copy_within(&mut self, src: usize, dst: usize, count: usize) -> io::Result<()> {
        assert_eq!(
            self.mode,
            Mode::Writable,
            "underlying file was opened as read-only"
        );

        if src + count > self.raw.len() {
            panic!("src out of bounds");
        }

        if dst + count > self.raw.len() {
            panic!("dst out of bounds");
        }

        if count == 0 {
            return Ok(());
        }

        let src_view = self
            .cache
            .take(src, count)
            .or_fetch(|off, len| self.fetch(off, len))?;

        let dst_view = self
            .cache
            .take(dst, count)
            .or_fetch(|off, len| self.fetch(off, len))?;

        if src >= dst && src < dst + count || dst >= src && dst < src + count {
            // Pointers are overlapping.

            // SAFETY: Destination pointer is valid for writing because this
            // method requires writable mode and that is checked at the top. We
            // have pointers to bytes so the pointers is properly aligned
            // trivially.
            unsafe {
                std::ptr::copy(src_view.as_ptr(), dst_view.as_ptr() as *mut _, count);
            }
        } else {
            // Pointers are not overlapping.

            // SAFETY: The safety of pointers is explained above. Moreover, we
            // just checked the non-overlapping property.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src_view.as_ptr(),
                    dst_view.as_ptr() as *mut _,
                    count,
                );
            }
        }

        Ok(())
    }

    pub fn view(&self, off: usize, len: usize) -> io::Result<ViewRef<'_>> {
        self.check_file_bounds(off, len);
        self.cache
            .take(off, len)
            .or_fetch(|off, len| self.fetch(off, len))
    }

    pub fn view_mut(&mut self, off: usize, len: usize) -> io::Result<ViewMut<'_>> {
        assert_eq!(
            self.mode,
            Mode::Writable,
            "underlying file was opened as read-only"
        );

        self.check_file_bounds(off, len);

        let raw = &self.raw;
        let cache_block_size = self.cache_block_size;
        let writable = self.mode.is_writable();

        self.cache
            .take_mut(off, len)
            .or_fetch(|off, len| Self::fetch_impl(raw, cache_block_size, writable, off, len))
    }

    pub fn len(&self) -> usize {
        self.raw.len()
    }

    pub fn cache_block_size(&self) -> usize {
        self.cache_block_size
    }

    fn check_file_bounds(&self, off: usize, len: usize) -> usize {
        let end = off + len;
        if end > self.raw.len() {
            panic!("out of bounds");
        } else {
            end
        }
    }

    fn fetch(&self, off: usize, len: usize) -> io::Result<os::RawView> {
        Self::fetch_impl(
            &self.raw,
            self.cache_block_size,
            self.mode.is_writable(),
            off,
            len,
        )
    }

    fn fetch_impl(
        raw: &os::RawFile,
        cache_block_size: usize,
        writable: bool,
        off: usize,
        len: usize,
    ) -> io::Result<os::RawView> {
        // Determine the end of the block. We allocate a block of size at least
        // the cache block size setting.
        let end = std::cmp::max(align_add(off + len), cache_block_size);
        // Align the offset.
        let off = align_sub(off);
        // Don't allocate a block that would exceed the end of the file.
        let end = std::cmp::min(end, raw.len());
        let len = end - off;
        raw.view(off, len, writable)
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "File {{ .. }}")
    }
}

// SAFETY: All mutating operations take exclusive reference to File. It also
// does not implement Copy nor Clone, so there is exactly one file descriptor
// and cache for the underlying file.
unsafe impl Send for File {}
unsafe impl Sync for File {}

/// Iterator over the bytes in the underlying file.
///
/// # Panics
///
/// If the iteration encounters an I/O error, the iterator simply panics.
pub struct Iter<'a> {
    file: &'a File,
    view: ViewRef<'a>,
    cur: usize,
    cum: usize,
}

impl<'a> Iter<'a> {
    pub(crate) fn from_file(file: &'a File) -> io::Result<Self> {
        let block_size = std::cmp::min(file.len(), file.cache_block_size());
        let view = file.view(0, block_size)?;

        Ok(Self {
            file,
            view,
            cur: 0,
            cum: 0,
        })
    }
}

impl Iterator for Iter<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cum == self.file.len() {
            return None;
        }

        if self.cur == self.view.len() {
            let block_size =
                std::cmp::min(self.file.len() - self.cum, self.file.cache_block_size());
            self.view = self.file.view(self.cur, block_size).unwrap();
            self.cur = 0;
        }

        let byte = self.view[self.cur];
        self.cur += 1;
        self.cum += 1;

        Some(byte)
    }
}

fn fix_cache_block_size(cache_block_size: usize) -> usize {
    if cache_block_size == 0 {
        *ALIGNMENT
    } else {
        align_add(cache_block_size)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Mode {
    Writable,
    Readonly,
}

impl Mode {
    pub fn is_writable(&self) -> bool {
        match self {
            Mode::Writable => true,
            Mode::Readonly => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::mem;
    use std::path::PathBuf;

    fn should_panic<F: FnOnce() -> R + std::panic::UnwindSafe, R: std::fmt::Debug>(
        body: F,
        message: &str,
    ) {
        let result = std::panic::catch_unwind(body).map_err(|m| m.downcast::<&str>().unwrap());

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), Box::new(message));
    }

    fn should_fail_assert<F: FnOnce() -> R + std::panic::UnwindSafe, R: std::fmt::Debug>(
        body: F,
        message: &str,
    ) {
        let result = std::panic::catch_unwind(body).map_err(|m| m.downcast::<String>().unwrap());

        assert!(result.is_err());
        assert!(result.unwrap_err().contains(message));
    }

    fn pb(path: &str) -> PathBuf {
        PathBuf::from(path)
    }

    #[test]
    fn cache_consistency_after_resize() {
        let alignment = os::get_alignment();

        let mut file = File::open_writable(
            pb("cache_consistency_after_resize.tmp").as_path(),
            512,
            2,
            alignment,
        )
        .unwrap();

        // Write a byte to the first position.
        file.view_mut(0, 1).unwrap()[0] = 3;

        // Resize the file for the first time. This should allocate cache to be
        // of full cache capacity.
        file.resize(alignment).unwrap();

        // Write a different byte to the first position.
        file.view_mut(0, alignment).unwrap()[0] = 5;

        // Resize the file such that it is forced to reallocate cache on slice request to the newly allocated region.
        file.resize(2 * alignment).unwrap();

        // Access the new region to force cache reallocation.
        file.view(alignment, alignment).unwrap();

        // Access the original region to force cache reallocation again.
        let byte = file.view(0, alignment).unwrap()[0];

        assert_eq!(byte, 5);
    }

    #[test]
    fn copy_within_overlapping() {
        let mut file =
            File::open_writable(pb("copy_within_overlapping.tmp").as_path(), 512, 2, 512).unwrap();

        file.view_mut(0, 8).unwrap()[0..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);

        file.copy_within(0, 4, 8).unwrap();

        assert_eq!(
            &file.view(0, 12).unwrap()[0..12],
            &[1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn copy_within_non_overlapping() {
        let alignment = os::get_alignment();

        let mut file = File::open_writable(
            pb("copy_within_non_overlapping.tmp").as_path(),
            4 * alignment,
            2,
            alignment,
        )
        .unwrap();

        file.view_mut(0, 8).unwrap()[0..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);

        file.copy_within(0, 2 * alignment, 8).unwrap();

        assert_eq!(&file.view(0, 8).unwrap()[0..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            &file.view(2 * alignment, 8).unwrap()[0..8],
            &[1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn check_zero_len() {
        let alignment = os::get_alignment();

        let file = File::open_writable(pb("check_zero_len1.tmp").as_path(), 0, 2, alignment);
        assert!(file.is_err());
        assert_eq!(
            file.unwrap_err().to_string(),
            "len must be greater than zero"
        );

        let file = File::open_writable(pb("check_zero_len2.tmp").as_path(), 512, 2, 0);
        assert!(file.is_ok());
        assert_eq!(file.unwrap().cache_block_size(), alignment);

        let mut file =
            File::open_writable(pb("check_zero_len3.tmp").as_path(), 512, 2, alignment).unwrap();
        let result = file.resize(0);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "new_len must be greater than zero"
        );
        mem::drop(file);

        let file_path = pb("check_zero_len.tmp");
        let std_file = fs::File::create(file_path.as_path()).unwrap();
        mem::drop(std_file);

        let file = File::open_readonly(file_path.as_path(), 2, alignment);
        assert!(file.is_err());
        assert_eq!(file.unwrap_err().to_string(), "file is empty");

        let mut std_file = fs::File::create(file_path.as_path()).unwrap();
        std_file.write(&[1, 2, 3, 4]).unwrap();
        std_file.flush().unwrap();
        mem::drop(std_file);

        let file = File::open_readonly(file_path.as_path(), 2, 0);
        assert!(file.is_ok());
        assert_eq!(file.unwrap().cache_block_size(), alignment);

        fs::remove_file(file_path.as_path()).unwrap();
    }

    #[test]
    fn bounds_checks() {
        let alignment = os::get_alignment();

        // Writable access to the file that is supposed to be temporary.

        let file = File::open_writable(pb("bounds_checks1.tmp").as_path(), alignment, 2, alignment)
            .unwrap();
        let _ = file.view(0, alignment);
        mem::drop(file);

        should_panic(
            || {
                let file = File::open_writable(
                    pb("bounds_checks2.tmp").as_path(),
                    alignment,
                    2,
                    alignment,
                )
                .unwrap();
                let _ = file.view(0, alignment + 1);
            },
            "out of bounds",
        );

        should_panic(
            || {
                let mut file = File::open_writable(
                    pb("bounds_checks3.tmp").as_path(),
                    alignment,
                    2,
                    alignment,
                )
                .unwrap();
                let _ = file.copy_within(alignment + 1, 0, 0);
            },
            "src out of bounds",
        );

        should_panic(
            || {
                let mut file = File::open_writable(
                    pb("bounds_checks4.tmp").as_path(),
                    alignment,
                    2,
                    alignment,
                )
                .unwrap();
                let _ = file.copy_within(alignment, 0, 1);
            },
            "src out of bounds",
        );

        should_panic(
            || {
                let mut file = File::open_writable(
                    pb("bounds_checks5.tmp").as_path(),
                    alignment,
                    2,
                    alignment,
                )
                .unwrap();
                let _ = file.copy_within(0, alignment + 1, 0);
            },
            "dst out of bounds",
        );

        should_panic(
            || {
                let mut file = File::open_writable(
                    pb("bounds_checks6.tmp").as_path(),
                    alignment,
                    2,
                    alignment,
                )
                .unwrap();
                let _ = file.copy_within(0, alignment, 1);
            },
            "dst out of bounds",
        );

        // Readonly access to the file of size 4.

        let file_path = pb("bounds_checks.tmp");
        let mut std_file = fs::File::create(file_path.as_path()).unwrap();
        std_file.write(&[1, 2, 3, 4]).unwrap();
        std_file.flush().unwrap();
        mem::drop(std_file);

        let file = File::open_readonly(file_path.as_path(), 2, alignment);

        let file = file.unwrap();
        let _ = file.view(0, 4);
        mem::drop(file);

        should_panic(
            || {
                let file = File::open_readonly(file_path.as_path(), 2, alignment).unwrap();
                let _ = file.view(0, 5);
            },
            "out of bounds",
        );

        fs::remove_file(file_path.as_path()).unwrap();
    }

    #[test]
    fn protection_checks() {
        let alignment = os::get_alignment();

        let file_path = pb("protection_checks.tmp");
        let mut std_file = fs::File::create(file_path.as_path()).unwrap();
        std_file.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        std_file.flush().unwrap();
        mem::drop(std_file);

        should_fail_assert(
            || {
                let mut file = File::open_readonly(file_path.as_path(), 2, alignment).unwrap();
                let _ = file.view_mut(0, 8);
            },
            "underlying file was opened as read-only",
        );

        should_fail_assert(
            || {
                let mut file = File::open_readonly(file_path.as_path(), 2, alignment).unwrap();
                let _ = file.resize(alignment);
            },
            "underlying file was opened as read-only",
        );

        should_fail_assert(
            || {
                let mut file = File::open_readonly(file_path.as_path(), 2, alignment).unwrap();
                let _ = file.copy_within(0, 4, 4);
            },
            "underlying file was opened as read-only",
        );

        fs::remove_file(file_path.as_path()).unwrap();
    }

    #[test]
    fn view_behind_alignment() {
        let alignment = os::get_alignment();
        let file = File::open_writable(
            pb("view_behind_alignment.tmp").as_path(),
            2 * alignment,
            2,
            alignment,
        )
        .unwrap();

        assert!(file.view(alignment, alignment).is_ok());
    }
}
