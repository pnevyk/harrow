use std::collections::VecDeque;
use std::io;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use crate::os;

pub struct Cache {
    // Blocks available for acquiring, no reference to this memory exists in the
    // outside world. This has a limited capacity.
    available: RwLock<VecDeque<CachedBlock>>,
    // Blocks that are currently lent to the outside world. This vector must
    // hold all cached blocks until all references are dropped. In that case,
    // the cached block goes into available ones.
    lent: RwLock<Vec<CachedBlock>>,
    // A block that is current lent as ViewMut. There can be only one at a time.
    exclusive: Mutex<Option<CachedBlock>>,
    // Current size of the available blocks.
    len: AtomicUsize,
    // Capacity for the available blocks.
    capacity: NonZeroUsize,
}

impl Cache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            available: RwLock::new(VecDeque::with_capacity(capacity)),
            lent: RwLock::new(Vec::new()),
            exclusive: Mutex::new(None),
            len: AtomicUsize::new(0),
            capacity: NonZeroUsize::new(capacity).expect("capacity must be non-zero"),
        }
    }

    #[cfg(test)]
    pub fn available(&self) -> usize {
        self.available.read().unwrap().len()
    }

    pub fn lent(&self) -> usize {
        self.lent.read().unwrap().len()
    }

    #[cfg(test)]
    pub fn holds(&self, off: usize, len: usize) -> bool {
        let in_available = self
            .available
            .read()
            .unwrap()
            .iter()
            .any(|block| block.is_hit(off, len));

        if in_available {
            return true;
        }

        self.lent
            .read()
            .unwrap()
            .iter()
            .any(|block| block.is_hit(off, len))
    }

    pub fn take(&self, off: usize, len: usize) -> Take<'_> {
        // First, search in available blocks. It is more probable that the
        // request is in available blocks rather than in lent blocks, because
        // the latter means requesting the same data multiple times.

        if let Some(block) = self.acquire_available(off, len) {
            // Fe were able to acquire a block from available blocks. Now we
            // create the first reference to it and idd the block to lent
            // blocks.
            let view = block.view_ref(self, off, len);
            self.lent.write().unwrap().push(block);
            Take {
                cache: self,
                view: Some(view),
                off,
                len,
            }
        } else {
            self.lent
                .read()
                .unwrap()
                .iter()
                .rev()
                .find(|view| view.is_hit(off, len))
                .map(|block| {
                    // We found the block in lent blocks, that is, it is already
                    // lent as at least one other ViewRef. We just create a new
                    // reference to it.
                    let view = block.view_ref(self, off, len);
                    Take {
                        cache: self,
                        view: Some(view),
                        off,
                        len,
                    }
                })
                // There is no satisfying block in the lent either, so we return
                // an empty Take.
                .unwrap_or(Take {
                    cache: self,
                    view: None,
                    off,
                    len,
                })
        }
    }

    pub fn take_mut(&mut self, off: usize, len: usize) -> TakeMut<'_> {
        // No references are living in the outside world - everything is held in
        // the cache.
        assert!(self.lent() == 0);

        // We take exclusive reference due to API enforcements, but in the body
        // we need shared reference instead.
        let cache = &*self;

        // Search in the available blocks only, the lent collection is empty.
        self.acquire_available(off, len)
            .map(|block| {
                // We were able to find a block in available, we assign it to
                // the exclusive field and return the only mutable reference.
                let view = block.view_mut(cache, off, len);
                *self.exclusive.lock().unwrap() = Some(block);
                TakeMut {
                    cache,
                    view: Some(view),
                    off,
                    len,
                }
            })
            // There is no satisfying block in the lent, so we return an empty
            // TakeMut.
            .unwrap_or(TakeMut {
                cache,
                view: None,
                off,
                len,
            })
    }

    fn add_fetched_ref(&self, view: os::RawView, off: usize, len: usize) -> ViewRef<'_> {
        // New block must have been fetched, we store it and return a reference.
        let block = CachedBlock::new(view);
        let view = block.view_ref(self, off, len);
        self.lent.write().unwrap().push(block);
        view
    }

    fn add_fetched_mut(&self, view: os::RawView, off: usize, len: usize) -> ViewMut<'_> {
        // New block must have been fetched, we store it and return a reference.
        // Note that we put that into available blocks. This is an optimization,
        // see `take_mut` for justification.
        let block = CachedBlock::new(view);
        let view = block.view_mut(self, off, len);
        *self.exclusive.lock().unwrap() = Some(block);
        view
    }

    fn restore_ref<'a>(&self, view: &ViewRef<'a>) {
        let mut lent = self.lent.write().unwrap();

        let mut available = None;
        for (index, block) in lent.iter().enumerate() {
            if block.holds(view.as_base_ptr()) {
                if block.restore_ref(view) {
                    // All references returned, the block is again available.
                    available = Some(index);
                }

                break;
            }
        }

        if let Some(index) = available {
            let block = lent.remove(index);
            std::mem::drop(lent);
            self.add_available(block);
        }
    }

    fn restore_mut<'a>(&self, view: &ViewMut<'a>) {
        // The block must be in self.exclusive.
        let block = self.exclusive.lock().unwrap().take().unwrap();
        block.restore_mut(view);
        self.add_available(block);
    }

    fn acquire_available(&self, off: usize, len: usize) -> Option<CachedBlock> {
        let mut available = self.available.write().unwrap();
        let found = available
            .iter()
            .enumerate()
            .rev()
            .find(|(_, view)| view.is_hit(off, len))
            .map(|(index, _)| index);

        if let Some(index) = found {
            // If we have a hit, we need to move the cached item from the
            // available blocks into the lent blocks.
            self.len.fetch_sub(1, Ordering::SeqCst);
            let block = available.remove(index).unwrap();
            debug_assert!(block.is_hit(off, len));
            Some(block)
        } else {
            None
        }
    }

    fn add_available(&self, block: CachedBlock) {
        let mut available = self.available.write().unwrap();

        // We need to drop those blocks that overlap with the block being added.
        // This is necessary for keeping consistency when doing mutable views,
        // since the data is being flushed only when the dirty cached block is
        // being dropped.
        let view = block.raw_view();
        available.retain(|block| !block.is_overlapping(view.offset(), view.len()));
        let mut len = available.len();

        // Check if we are going to exceed the capacity. In such case, we
        // discard the least recent block.
        if len < self.capacity.get() {
            len += 1;
        } else {
            let mut dropped = available.pop_front().unwrap();
            dropped.flush_if_dirty();
        }

        // Finally, store the block.
        available.push_back(block);
        self.len.store(len, Ordering::SeqCst);
    }
}

struct CachedBlock {
    view: os::RawView,
    refs: AtomicUsize,
    dirty: AtomicBool,
}

impl CachedBlock {
    pub fn new(view: os::RawView) -> Self {
        Self {
            view,
            refs: AtomicUsize::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    pub fn is_hit(&self, off: usize, len: usize) -> bool {
        self.view.offset() <= off && self.view.offset() + self.view.len() >= off + len
    }

    pub fn is_overlapping(&self, off: usize, len: usize) -> bool {
        // Determine the intersection of the regions and then check if it empty
        // or not.
        let start = std::cmp::max(self.view.offset(), off);
        let end = std::cmp::min(self.view.offset() + self.view.len(), off + len);
        start < end
    }

    pub fn raw_view(&self) -> &os::RawView {
        &self.view
    }

    pub fn view_ref<'a>(&self, cache: &'a Cache, off: usize, len: usize) -> ViewRef<'a> {
        self.refs.fetch_add(1, Ordering::SeqCst);
        ViewRef {
            cache,
            base_ptr: self.view.as_ptr(),
            off: off - self.view.offset(),
            len,
        }
    }

    pub fn view_mut<'a>(&self, cache: &'a Cache, off: usize, len: usize) -> ViewMut<'a> {
        assert!(self.refs.load(Ordering::SeqCst) == 0);
        ViewMut {
            cache,
            base_ptr: self.view.as_ptr() as *mut u8,
            off: off - self.view.offset(),
            len,
        }
    }

    pub fn holds<'a>(&self, ptr: *const u8) -> bool {
        self.view.as_ptr() == ptr
    }

    pub fn restore_ref<'a>(&self, view: &ViewRef<'a>) -> bool {
        assert!(self.holds(view.as_base_ptr()));
        self.refs.fetch_sub(1, Ordering::SeqCst) == 1
    }

    pub fn restore_mut<'a>(&self, _view: &ViewMut<'a>) {
        self.dirty.store(true, Ordering::SeqCst);
    }

    pub fn flush_if_dirty(&mut self) {
        if self.dirty.load(Ordering::SeqCst) {
            let _ = self.view.flush();
        }
    }
}

pub struct Take<'a> {
    cache: &'a Cache,
    view: Option<ViewRef<'a>>,
    off: usize,
    len: usize,
}

impl<'a> Take<'a> {
    pub fn or_fetch<F>(self, fetch: F) -> io::Result<ViewRef<'a>>
    where
        F: FnOnce(usize, usize) -> io::Result<os::RawView>,
    {
        match self.view {
            Some(view) => Ok(view),
            None => fetch(self.off, self.len)
                .map(|fetched| self.cache.add_fetched_ref(fetched, self.off, self.len)),
        }
    }
}

pub struct TakeMut<'a> {
    cache: &'a Cache,
    view: Option<ViewMut<'a>>,
    off: usize,
    len: usize,
}

impl<'a> TakeMut<'a> {
    pub fn or_fetch<F>(self, fetch: F) -> io::Result<ViewMut<'a>>
    where
        F: FnOnce(usize, usize) -> io::Result<os::RawView>,
    {
        match self.view {
            Some(view) => Ok(view),
            None => fetch(self.off, self.len)
                .map(|fetched| self.cache.add_fetched_mut(fetched, self.off, self.len)),
        }
    }
}

/// A read-only virtually mapped view into the underlying file.
///
/// Essentially, it represents a shared reference to a slice of bytes `&[u8]`,
/// only that it needs to be wrapped into a special type due to resource
/// management.
pub struct ViewRef<'a> {
    cache: &'a Cache,
    base_ptr: *const u8,
    off: usize,
    len: usize,
}

impl<'a> ViewRef<'a> {
    pub(crate) fn as_base_ptr(&self) -> *const u8 {
        self.base_ptr
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        // SAFETY: The code that constructs the ViewRef must ensure that
        // base_ptr is valid for len bytes from offset off.
        unsafe { self.base_ptr.add(self.off) }
    }

    /// Reinterprets the view to a slice of bytes.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: The returned slice has the lifetime of self borrow, so it
        // does not outlive the view trivially.
        unsafe { self.as_slice_dangling() }
    }

    /// Reinterprets the view to a slice of bytes.
    ///
    /// # Safety
    ///
    /// Note that the lifetime of the returned slice is not bound to the
    /// lifetime of `self`. The caller must ensure that the slice does not
    /// outlive the source `ViewRef`. Otherwise, the slice points to an invalid
    /// memory after the view is dropped.
    pub unsafe fn as_slice_dangling(&self) -> &'a [u8] {
        // SAFETY: ViewRef represents a shared reference, nothing can mutate
        // data in cached blocks during the lifetime of this reference. The
        // pointer is still referring to a valid memory, because Cache instance
        // is owning the source RawView and is keeping it in its `lent` blocks.
        // The correctness of the pointer and length is guaranteed by RawView.
        std::slice::from_raw_parts(self.as_ptr(), self.len)
    }
}

impl Drop for ViewRef<'_> {
    fn drop(&mut self) {
        self.cache.restore_ref(self);
    }
}

impl Deref for ViewRef<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for ViewRef<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

// SAFETY: ViewRef represents a chunk of read-only memory. There is no way to
// mutate the underlying memory: the cache requires exclusive access for
// mutating views and tanks to the lifetimes both read-only and mutable views
// cannot happen at the same time.
unsafe impl Send for ViewRef<'_> {}
unsafe impl Sync for ViewRef<'_> {}

/// A writable virtually mapped view into the underlying file.
///
/// Essentially, it represents an exclusive reference to a slice of bytes `&mut
/// [u8]`, only that it needs to be wrapped into a special type due to resource
/// management.
pub struct ViewMut<'a> {
    cache: &'a Cache,
    base_ptr: *mut u8,
    off: usize,
    len: usize,
}

impl<'a> ViewMut<'a> {
    pub(crate) fn as_ptr(&self) -> *const u8 {
        // SAFETY: The code that constructs the ViewRef must ensure that
        // base_ptr is valid for len bytes from offset off.
        unsafe { self.base_ptr.add(self.off) }
    }

    /// Reinterprets the view to a slice of bytes.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: The returned slice has the lifetime of self borrow, so it
        // does not outlive the view trivially.
        unsafe { self.as_slice_dangling() }
    }

    /// Reinterprets the view to a mutable slice of bytes.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: The returned slice has the lifetime of self borrow, so it
        // does not outlive the view trivially.
        unsafe { self.as_mut_slice_dangling() }
    }

    /// Reinterprets the view to a slice of bytes.
    ///
    /// # Safety
    ///
    /// Note that the lifetime of the returned slice is not bound to the
    /// lifetime of `self`. The caller must ensure that the slice does not
    /// outlive the source `ViewMut`. Otherwise, the slice points to an invalid
    /// memory after the view is dropped.
    pub unsafe fn as_slice_dangling(&self) -> &'a [u8] {
        // SAFETY: The pointer is still referring to a valid memory, because
        // Cache instance is owning the source RawView. Although the RawView is
        // stored in the available blocks, nothing can actually discard it from
        // there because no other views (neither shared nor exclusive) can ve
        // created from the cache during the lifetime of this ViewMut.
        // The correctness of the pointer and length is guaranteed by RawView.
        std::slice::from_raw_parts(self.as_ptr(), self.len)
    }

    /// Reinterprets the view to a mutable slice of bytes.
    ///
    /// # Safety
    ///
    /// Note that the lifetime of the returned slice is not bound to the
    /// lifetime of `self`. The caller must ensure that the slice does not
    /// outlive the source `ViewMut`. Otherwise, the slice points to an invalid
    /// memory after the view is dropped.
    pub unsafe fn as_mut_slice_dangling(&mut self) -> &'a mut [u8] {
        // SAFETY: The pointer validity is explained in `as_slice`. Mutable
        // views can be acquired from the cache only through exclusive access -
        // this guarantees that there is no lent block in the outside world. So
        // indeed, this ViewMut is the only pointer to this block of memory.
        std::slice::from_raw_parts_mut(self.as_ptr() as *mut _, self.len)
    }
}

impl Drop for ViewMut<'_> {
    fn drop(&mut self) {
        self.cache.restore_mut(self);
    }
}

impl Deref for ViewMut<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for ViewMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl AsRef<[u8]> for ViewMut<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for ViewMut<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

// SAFETY: To acquire ViewMut, which is a mutable chunk of memory, the cache
// requires to have exclusive access. This exclusive access holds during the
// whole lifetime of ViewMut, so an instance of ViewMut is a one and only one
// mutable view to the underlying file. Moreover, ViewMut is not Copy nor Clone,
// so there is no way how this mutable access can be shared.
unsafe impl Send for ViewMut<'_> {}
unsafe impl Sync for ViewMut<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;
    use std::path::PathBuf;

    fn new_file(name: &str, pages: usize) -> os::RawFile {
        os::RawFile::open_writable(PathBuf::from(name).as_path(), pages * os::get_alignment())
            .unwrap()
    }

    #[test]
    fn take_one_then_return() {
        let file = new_file("take_one_then_return.tmp", 1);
        let cache = Cache::with_capacity(1);

        let view = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        assert_eq!(cache.available(), 0);
        assert_eq!(cache.lent(), 1);

        mem::drop(view);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 0);
    }

    #[test]
    fn take_twice_then_return() {
        let file = new_file("take_twice_then_return.tmp", 1);
        let cache = Cache::with_capacity(1);

        let view1 = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        let view2 = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        assert_eq!(cache.available(), 0);
        assert_eq!(cache.lent(), 1);
        assert_eq!(view1.as_ptr(), view2.as_ptr());

        mem::drop(view1);

        assert_eq!(cache.available(), 0);
        assert_eq!(cache.lent(), 1);

        mem::drop(view2);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 0);
    }

    #[test]
    fn take_more_then_return() {
        let file = new_file("take_more_then_return.tmp", 2);
        let cache = Cache::with_capacity(2);

        let view1 = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        let view2 = cache
            .take(os::get_alignment(), os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        assert_eq!(cache.available(), 0);
        assert_eq!(cache.lent(), 2);
        assert_ne!(view1.as_base_ptr(), view2.as_base_ptr());

        mem::drop(view1);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 1);

        mem::drop(view2);

        assert_eq!(cache.available(), 2);
        assert_eq!(cache.lent(), 0);
    }

    #[test]
    fn take_mut_then_return() {
        let file = new_file("take_mut_then_return.tmp", 1);
        let mut cache = Cache::with_capacity(1);

        let view = cache
            .take_mut(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        mem::drop(view);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 0);
    }

    #[test]
    fn capacity_limit() {
        let file = new_file("capacity_limit.tmp", 2);
        let cache = Cache::with_capacity(1);

        let view1 = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        let view2 = cache
            .take(os::get_alignment(), os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        assert_eq!(cache.available(), 0);
        assert_eq!(cache.lent(), 2);
        assert_ne!(view1.as_base_ptr(), view2.as_base_ptr());

        mem::drop(view1);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 1);

        mem::drop(view2);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 0);
        assert!(cache.holds(os::get_alignment(), os::get_alignment()));
    }

    #[test]
    fn take_mut_capacity_limit() {
        let file = new_file("take_mut_capacity_limit.tmp", 2);
        let mut cache = Cache::with_capacity(1);

        let view = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        mem::drop(view);

        let view = cache
            .take_mut(os::get_alignment(), os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, true))
            .unwrap();

        mem::drop(view);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 0);
        assert!(cache.holds(os::get_alignment(), os::get_alignment()));
        assert!(!cache.holds(0, os::get_alignment()));

        let view = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        mem::drop(view);

        assert_eq!(cache.available(), 1);
        assert_eq!(cache.lent(), 0);
        assert!(cache.holds(0, os::get_alignment()));
        assert!(!cache.holds(os::get_alignment(), os::get_alignment()));
    }

    #[test]
    fn hit_correctness() {
        let file = new_file("hit_correctness.tmp", 1);
        let cache = Cache::with_capacity(1);

        let view1 = cache
            .take(0, os::get_alignment())
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        let view2 = cache
            .take(0, 64)
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        assert_eq!(view1.as_base_ptr(), view2.as_base_ptr());
        assert_eq!(cache.available(), 0);
        assert_eq!(cache.lent(), 1);

        let view3 = cache
            .take(os::get_alignment() - 64, 64)
            .or_fetch(|off, len| file.view(off, len, false))
            .unwrap();

        assert_eq!(view1.as_base_ptr(), view3.as_base_ptr());
        assert_eq!(cache.available(), 0);
        assert_eq!(cache.lent(), 1);
    }
}
