//! A low-level library for making file-backed storage for huge data structures.
//!
//! In short, *harrow* opens a (possibly temporary) file and provides random
//! access to portions of the file, while managing a faster-access cache with
//! virtually mapped buffers. The use case is when the data is larger than is
//! possible to fit or reasonable to hold in memory.
//!
//! *CAUTION:* The library uses a lot of *unsafe* and OS-specific APIs, without
//! author's deep knowledge of either. Do not use it where animals may be
//! harmed. Any help with testing and reviewing is much appreciated.
//!
//! Supported platforms (as far as a small bunch of tests indicate):
//!
//! * Linux (works on my machine)
//! * MacOS (I suppose for its unixness)
//! * Windows (works on Windows 10 inside VirtualBox)
//!
//! Dual-licensed under MIT and [UNLICENSE](https://unlicense.org/). Feel free
//! to use it, contribute or spread the word.
//!
//! # Usage
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! harrow = "0.1"
//! ```
//!
//! Then check the documentation (including examples) of
//! [`FileMut`](crate::FileMut) (writable buffer) and
//! [`FileRef`](crate::FileRef) (read-only buffer). There are also some sample
//! codes in `examples/` directory.
//!
//! # Roadmap
//!
//! * Ensure correctness with tests and thorough reviews
//! * Improve average performance
//! * Polish and extend APIs
//!   * This includes implementation of standard traits
//! * Automatic multi-platform testing in CI
//!
//! Contributions are welcome!
//!
//! # Advantages over using `std::fs::File`
//!
//! * Requested chunks of the file are mapped into the virtual memory and they
//!   can be referenced almost at no cost
//! * Virtually mapped chunks are cached in a LRU cache, so a cache friendly
//!   access is fast
//! * Content protection to prevent external modifications
//!
//! # Errors
//!
//! *Harrow* propagates all I/O errors that happen during the operation. Mostly
//! these are file creation and manipulation operations and creating virtual
//! mappings inside the file.
//!
//! The only error that does not come directly from OS is checking for non-zero
//! length of the file, both in initialization and resizing.
//!
//! # Panics
//!
//! Any access out of the bounds of the allocated file results in a panic. It is
//! the responsibility of the user to ensure correct access.
//!
//! # Temporaries
//!
//! When a non-existing file with writable access is opened, it is considered as
//! a temporary, and thus is deleted when the owner goes out of scope. Thanks to
//! this, the user doesn't need to worry about cleaning.
//!
//! # Caching
//!
//! To create read-only or mutable slices into the underlying file (to mimic
//! heap-backed storage), it is necessary to create a virtual mappings to the
//! file. In an attempt to have accesses (potentially) faster, the storages
//! maintain a cache of such mapped blocks with of larger size and use them if a
//! request is in bounds of one of the blocks.
//!
//! The strategy is basically a traditional least-recently used (LRU), or more
//! precisely, least-recently freed.
//!
//! The cache implementation is currently quite naive, but (hopefully) correct.
//! The further work should be put to improve its efficiency.
//!
//! # Locking
//!
//! *Harrow* tries its best to prevent external modifications to the underlying
//! files. Current techniques are yet unspecified, but we may be using limited
//! permissions when creating files, limiting [share
//! modes](https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew),
//! file locking (even if it is only
//! [advisory](https://www.baeldung.com/linux/file-locking)), etc.
//!
//! # Name
//!
//! [Harrow](https://en.wikipedia.org/wiki/Harrow_(tool)) is an agricultural
//! tool used for preparing the soil structure on a field that is suitable for
//! planting seeds. So the metaphor is obvious: *harrow* will prepare a file for
//! you in which you can seed your bytes as you need. Files are bigger than
//! operational memory in the same way as fields are bigger than gardens.

#![doc(html_root_url = "https://docs.rs/harrow/0.1.0")]
#![deny(missing_docs)]

use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

mod align;
mod cache;
mod ext;
mod infra;
mod os;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub use cache::{ViewMut, ViewRef};
pub use infra::Iter;
pub use os::granularity;

use infra::File;

/// The default cache capacity if not specified. Currently, it is 5 blocks.
pub const DEFAULT_CACHE_CAPACITY: usize = 5;

/// The default cache block size if not specified. Currently, it is 128 MiB.
pub const DEFAULT_CACHE_BLOCK_SIZE: usize = 128 * 1024 * 1024;

/// A writable file-backed buffer.
///
/// This type of buffer can be used both for reading from and writing to a file.
/// Apart from writing, the buffer can be also resized such that the underlying
/// file reflects that.
///
/// For read-only variant, refer to [`FileRef`](crate::FileRef).
///
/// # Use cases
///
/// `FileMut` can be used to hold data that are too big to keep them in the
/// operational memory. Contents written to this buffer are stored in a file at
/// given path.
///
/// Note that `harrow` is intentionally low-level and thus this type more or
/// less represents a raw pointer to a buffer of bytes.
///
/// # Examples
///
/// Store a huge bunch of numbers.
///
/// ```
/// use std::mem;
/// use harrow::FileMut;
///
/// // To justify the use of `harrow`, this number should be probably much bigger.
/// let n = 1_000_000;
/// // Allocate the file for our numbers.
/// let mut numbers = FileMut::new("numbers.bin", n * mem::size_of::<u64>()).unwrap();
///
/// // Fill the file with the numbers.
/// for i in 0..n {
///     let number = i as u64;
///     // We need to serialize the number to bytes.
///     numbers.write_at(&number.to_ne_bytes(), i * mem::size_of::<u64>()).unwrap();
/// }
///
/// let mut buf = [0u8; 8];
/// // Read the number at the third position.
/// numbers.read_at(&mut buf, 2 * mem::size_of::<u64>()).unwrap();
/// assert_eq!(u64::from_ne_bytes(buf), 2);
/// ```
pub struct FileMut(File);

impl FileMut {
    /// Creates new writable buffer for the file at given `path` with the
    /// default cache capacity and block size.
    ///
    /// The `len` argument must not be zero. If the file does not exist, it is
    /// automatically created and then automatically removed when `FileMut` is
    /// dropped. If the file exists, it may be **truncated** to the aligned
    /// size. To avoid destroying any existing data, make sure to call this
    /// constructor with `len` at least of the size of the file.
    ///
    /// The length is actually rounded to the closest bigger number that is
    /// aligned with the alignment that is required or recommended by the
    /// operating system.
    pub fn new<P: AsRef<Path>>(path: P, len: usize) -> io::Result<Self> {
        Self::with_cache(path, len, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_BLOCK_SIZE)
    }

    /// Creates new writable buffer for the file at given `path` with specified
    /// cache capacity and block size. For more information see
    /// [`FileMut::new`](crate::FileMut::new).
    ///
    /// The cache capacity must be greater than zero. The cache block size is
    /// actually rounded to the closest bigger number that is aligned with the
    /// alignment that is required or recommended by the operating system.
    pub fn with_cache<P: AsRef<Path>>(
        path: P,
        len: usize,
        cache_capacity: usize,
        cache_block_size: usize,
    ) -> io::Result<Self> {
        File::open_writable(path.as_ref(), len, cache_capacity, cache_block_size).map(Self)
    }

    /// Returns the size of the underlying file.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Acquires a view to bytes at given offset and of given length.
    pub fn view(&self, off: usize, len: usize) -> io::Result<ViewRef<'_>> {
        self.0.view(off, len)
    }

    /// Acquires a view to bytes at given range.
    pub fn view_range(&self, range: Range<usize>) -> io::Result<ViewRef<'_>> {
        self.0.view(range.start, range.end - range.start)
    }

    /// Acquires a mutable view to bytes at given offset and of given length.
    pub fn view_mut(&mut self, off: usize, len: usize) -> io::Result<ViewMut<'_>> {
        self.0.view_mut(off, len)
    }

    /// Acquires a mutable view to bytes at given range.
    pub fn view_range_mut(&mut self, range: Range<usize>) -> io::Result<ViewMut<'_>> {
        self.0.view_mut(range.start, range.end - range.start)
    }

    /// Resizes the underlying file to `new_len`.
    ///
    /// The new size must be greater than zero.
    pub fn resize(&mut self, new_len: usize) -> io::Result<()> {
        self.0.resize(new_len)
    }

    /// Copies `count` bytes from index `src` to index `dst`.
    ///
    /// Overlapping regions are properly handled.
    pub fn copy_within(&mut self, src: usize, dst: usize, count: usize) -> io::Result<()> {
        self.0.copy_within(src, dst, count)
    }

    /// Reads the bytes from the buffer starting from offset `off` into buffer
    /// `buf`. The size of the bytes read is determined by `buf.len()`.
    pub fn read_at(&self, buf: &mut [u8], off: usize) -> io::Result<()> {
        buf.copy_from_slice(self.0.view(off, buf.len())?.as_slice());
        Ok(())
    }

    /// Writes the bytes in `buf` to the buffer starting from offset `off`.
    pub fn write_at(&mut self, buf: &[u8], off: usize) -> io::Result<()> {
        Ok(self
            .0
            .view_mut(off, buf.len())?
            .as_mut_slice()
            .copy_from_slice(buf))
    }

    /// Returns an iterator over bytes.
    ///
    /// Note that even if the creation of the iterator succeeds, an I/O error
    /// iteration can happen during iteration. In that case, the iterator
    /// panics.
    pub fn iter(&self) -> io::Result<Iter<'_>> {
        Iter::from_file(&self.0)
    }
}

/// A read-only file-backed buffer.
///
/// This type of buffer can be used only for reading from a file. This
/// constraint allows shared ownership of the underlying buffer, so invoking
/// `clone` on `FileRef` reuses the same buffer with all the internals such as
/// the cache.
///
/// For writable variant, refer to [`FileMut`](crate::FileMut).
///
/// # Use cases
///
/// `FileRef` can be used to implement nice API over data stored in a file
/// without actually loading the whole contents to the operational memory.
///
/// Note that `harrow` is intentionally low-level and thus this type more or
/// less represents a raw pointer to a buffer of bytes.
///
/// # Examples
///
/// Store the offsets to newlines in a large file in order to quickly point to
/// *i*-th line.
///
/// ```
/// use std::fs;
/// use std::io::Write;
/// use harrow::FileRef;
///
/// // Assume some text in the file.
/// fs::File::create("text.txt")
///     .unwrap()
///     .write(b"Hello world!\nGreetings from the file.\n")
///     .unwrap();
///
/// let text = FileRef::new("text.txt").unwrap();
/// let mut offsets = Vec::new();
///
/// offsets.push(0);
/// for (offset, byte) in text.iter().unwrap().enumerate() {
///     if byte == '\n' as u8 {
///         offsets.push(offset + 1);
///     }
/// }
///
/// offsets.push(text.len());
///
/// // Load the second line according to the offsets we found.
/// let view = text.view_range(offsets[1]..offsets[2]).unwrap();
/// let second_line =
///     std::str::from_utf8(view.as_slice()).unwrap();
///
/// assert_eq!(second_line, "Greetings from the file.\n");
///
/// # let _ = fs::remove_file("text.txt");
/// ```
#[derive(Clone)]
pub struct FileRef(Arc<File>);

impl FileRef {
    /// Creates new read-only buffer for the file at given `path` with the
    /// default cache capacity and block size.
    ///
    /// If the file is empty, an error is returned.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::with_cache(path, DEFAULT_CACHE_BLOCK_SIZE, DEFAULT_CACHE_CAPACITY)
    }

    /// Creates new read-only buffer for the file at given `path` with specified
    /// cache capacity and block size. For more information see
    /// [`FileRef::new`](crate::FileRef::new).
    ///
    /// The cache capacity must be greater than zero. The cache block size is
    /// actually rounded to the closest bigger number that is aligned with the
    /// alignment that is required or recommended by the operating system.
    pub fn with_cache<P: AsRef<Path>>(
        path: P,
        cache_capacity: usize,
        cache_block_size: usize,
    ) -> io::Result<Self> {
        File::open_readonly(path.as_ref(), cache_capacity, cache_block_size)
            .map(Arc::new)
            .map(Self)
    }

    /// Returns the size of the underlying file.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Acquires a view to bytes at given offset and of given length.
    pub fn view(&self, off: usize, len: usize) -> io::Result<ViewRef<'_>> {
        self.0.view(off, len)
    }

    /// Acquires a view to bytes at given range.
    pub fn view_range(&self, range: Range<usize>) -> io::Result<ViewRef<'_>> {
        self.0.view(range.start, range.end - range.start)
    }

    /// Reads the bytes from the buffer starting from offset `off` into buffer
    /// `buf`. The size of the bytes read is determined by `buf.len()`.
    pub fn read_at(&self, buf: &mut [u8], off: usize) -> io::Result<()> {
        buf.copy_from_slice(self.0.view(off, buf.len())?.as_slice());
        Ok(())
    }

    /// Returns an iterator over bytes.
    ///
    /// Note that even if the creation of the iterator succeeds, and I/O error
    /// iteration can happen during iteration. In that case, the iterator
    /// panics.
    pub fn iter(&self) -> io::Result<Iter<'_>> {
        Iter::from_file(&*self.0)
    }
}
