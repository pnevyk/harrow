use std::ffi::CString;
use std::io;
use std::path::Path;
use std::ptr::NonNull;

use crate::ext::ResultExt;

pub struct RawFile {
    fd: libc::c_int,
    len: usize,
}

impl RawFile {
    pub fn open_writable(path: &Path, len: usize) -> io::Result<Self> {
        let exists = path.exists();
        let path = cstr(path)?;

        let mut flags = libc::O_RDWR;
        if !exists {
            flags |= libc::O_CREAT;
        }

        // SAFETY: The argument path was of type Path which is guaranteed to be
        // a valid path. It is passed to ffi::open as CStr which is a valid
        // null-terminated string. The flags and mode come from the ffi module
        // itself so they are supposed to be valid.
        //
        // The subsequent operations are performed on a valid file descriptor
        // thanks to the implementation of ffi::open.
        let fd = unsafe {
            // Open the file descriptor for creating virtual mappings.
            let fd = ffi::open(&path, flags)?;

            // Simulate delete_on_close. The file will be removed from the
            // directory, but will exists while we have the file descriptor
            // open.
            if !exists {
                ffi::remove(&path).cleanup(|| ffi::close(fd))?;
            }

            // Reserve the space in the file. This is required, otherwise, mmap
            // would fail.
            ffi::truncate(fd, len as libc::off_t).cleanup(|| ffi::close(fd))?;

            // Lock the file so there is higher chance that the underlying file
            // will not be modified.
            ffi::lock(fd, len as libc::off_t, true).cleanup(|| ffi::close(fd))?;

            fd
        };

        Ok(Self { fd, len })
    }

    pub fn open_readonly(path: &Path) -> io::Result<Option<Self>> {
        let len = path.metadata()?.len() as usize;

        if len == 0 {
            return Ok(None);
        }

        let path = cstr(path)?;

        // SAFETY: See open_writable.
        let fd = unsafe {
            // Open the file descriptor for creating virtual mappings.
            let fd = ffi::open(&path, libc::O_RDONLY)?;

            // Lock the file so there is higher chance that the underlying file
            // will not be modified.
            ffi::lock(fd, len as libc::off_t, false).cleanup(|| ffi::close(fd))?;

            fd
        };

        Ok(Some(Self { fd, len }))
    }

    pub fn resize(&mut self, new_len: usize) -> io::Result<()> {
        unsafe {
            ffi::truncate(self.fd, new_len as libc::off_t)?;
        }

        self.len = new_len;

        Ok(())
    }

    pub fn view(&self, off: usize, len: usize, writable: bool) -> io::Result<RawView> {
        let prot = if writable {
            libc::PROT_READ | libc::PROT_WRITE
        } else {
            libc::PROT_READ
        };

        // SAFETY: The file descriptor is valid and len is not zero.
        let ptr = unsafe { ffi::map(self.fd, len as libc::size_t, off as libc::off_t, prot)? };

        Ok(RawView { ptr, off, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for RawFile {
    fn drop(&mut self) {
        unsafe {
            let _ = ffi::unlock(self.fd, self.len as libc::off_t);
            let _ = ffi::close(self.fd);
        }
    }
}

pub struct RawView {
    ptr: NonNull<libc::c_void>,
    off: usize,
    len: usize,
}

impl RawView {
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr() as *const u8
    }

    pub fn offset(&self) -> usize {
        self.off
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn flush(&self) -> io::Result<()> {
        unsafe { ffi::sync(self.ptr, self.len) }
    }
}

impl Drop for RawView {
    fn drop(&mut self) {
        unsafe {
            let _ = ffi::unmap(self.ptr, self.len);
        }
    }
}

pub fn get_alignment() -> usize {
    // SAFETY: A simple call to the function with a valid option name.
    let result = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };

    // This should not happen, because sysconf returns -1 only if (1) the option
    // name is invalid, (2) the option is a limit and that limit indeterminate,
    // (3) the option is not supported. PAGESIZE option is valid and is not a
    // limit. Regarding the last point, it is among the POSIX.1 variables so its
    // support should be ubiquitous.
    if result == -1 {
        // Some reasonable default if this unlikely situation happens.
        4096
    } else {
        result as usize
    }
}

fn cstr(path: &Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(CString::new(path.as_os_str().as_bytes())?)
}

mod ffi {
    use std::ffi::CStr;
    use std::io;
    use std::ptr::NonNull;

    pub unsafe fn open(path: &CStr, flags: libc::c_int) -> io::Result<libc::c_int> {
        // Sets only reading permission for the user, so nobody (except a user
        // with root permissions) can modify or delete the file. Note that this
        // read-only permission applies only to new files and only for future
        // accesses, not the file descriptor we are just opening.
        let fd = libc::open(path.as_ptr(), flags, libc::S_IRUSR);

        if fd == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(fd)
        }
    }

    pub unsafe fn truncate(fd: libc::c_int, len: libc::off_t) -> io::Result<()> {
        if libc::ftruncate(fd, len) == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn close(fd: libc::c_int) -> io::Result<()> {
        if libc::close(fd) == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn remove(path: &CStr) -> io::Result<()> {
        if libc::remove(path.as_ptr()) == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn lock(fd: libc::c_int, len: libc::off_t, exclusive: bool) -> io::Result<()> {
        let lock_type = if exclusive {
            libc::F_WRLCK
        } else {
            libc::F_RDLCK
        };

        let flock = libc::flock {
            l_type: lock_type as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: 0,
            l_len: len,
            l_pid: libc::getpid(),
        };

        if libc::fcntl(fd, libc::F_SETLK, &flock) == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn unlock(fd: libc::c_int, len: libc::off_t) -> io::Result<()> {
        let flock = libc::flock {
            l_type: libc::F_UNLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: 0,
            l_len: len,
            l_pid: libc::getpid(),
        };

        if libc::fcntl(fd, libc::F_SETLK, &flock) == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn map(
        fd: libc::c_int,
        len: libc::size_t,
        off: libc::off_t,
        prot: libc::c_int,
    ) -> io::Result<NonNull<libc::c_void>> {
        let ptr = libc::mmap(std::ptr::null_mut(), len, prot, libc::MAP_SHARED, fd, off);

        if ptr == libc::MAP_FAILED {
            Err(io::Error::last_os_error())
        } else {
            NonNull::new(ptr).ok_or(io::Error::last_os_error())
        }
    }

    pub unsafe fn unmap(ptr: NonNull<libc::c_void>, len: libc::size_t) -> io::Result<()> {
        if libc::munmap(ptr.as_ptr(), len) == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn sync(ptr: NonNull<libc::c_void>, len: libc::size_t) -> io::Result<()> {
        if libc::msync(ptr.as_ptr(), len, libc::MS_SYNC) == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
