use std::io;
use std::path::Path;

use winapi::{
    shared::minwindef::DWORD,
    um::{
        fileapi::{CREATE_NEW, OPEN_EXISTING},
        memoryapi::{FILE_MAP_ALL_ACCESS, FILE_MAP_READ},
        winbase::{FILE_FLAG_DELETE_ON_CLOSE, FILE_FLAG_RANDOM_ACCESS},
        winnt::{
            FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_TEMPORARY,
            FILE_SHARE_READ, GENERIC_READ, GENERIC_WRITE, PAGE_READONLY, PAGE_READWRITE,
        },
    },
};

use crate::ext::ResultExt;

pub struct RawFile {
    file_hndl: ffi::RawHandle,
    map_hndl: ffi::RawHandle,
    len: usize,
    map_protect: DWORD,
}

impl RawFile {
    pub fn open_writable(path: &Path, len: usize) -> io::Result<Self> {
        let exists = path.exists();
        let path = lpcwstr(path);

        let desired_access = GENERIC_READ | GENERIC_WRITE;
        let share_mode = 0;
        let creation = if exists { OPEN_EXISTING } else { CREATE_NEW };
        let mut attributes = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS;
        if !exists {
            attributes |= FILE_FLAG_DELETE_ON_CLOSE | FILE_ATTRIBUTE_TEMPORARY;
        }
        let protect = PAGE_READWRITE;

        let (file_hndl, map_hndl) = unsafe {
            let file_hndl =
                ffi::create_file(&path, desired_access, share_mode, creation, attributes)?;
            ffi::resize_file(file_hndl, len).cleanup(|| ffi::close(file_hndl))?;
            // TODO: Lock the file using LockFileEx
            let map_hndl =
                ffi::create_mapping(file_hndl, protect).cleanup(|| ffi::close(file_hndl))?;
            (file_hndl, map_hndl)
        };

        Ok(Self {
            file_hndl,
            map_hndl,
            len,
            map_protect: protect,
        })
    }

    pub fn open_readonly(path: &Path) -> io::Result<Option<Self>> {
        let len = path.metadata()?.len() as usize;

        if len == 0 {
            return Ok(None);
        }

        let path = lpcwstr(path);

        let desired_access = GENERIC_READ;
        let share_mode = FILE_SHARE_READ;
        let creation = OPEN_EXISTING;
        let attributes = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS | FILE_ATTRIBUTE_READONLY;
        let protect = PAGE_READONLY;

        let file_hndl =
            unsafe { ffi::create_file(&path, desired_access, share_mode, creation, attributes)? };
        // TODO: Lock the file using LockFileEx
        let map_hndl =
            unsafe { ffi::create_mapping(file_hndl, protect).cleanup(|| ffi::close(file_hndl))? };

        Ok(Some(Self {
            file_hndl,
            map_hndl,
            len,
            map_protect: protect,
        }))
    }

    pub fn resize(&mut self, new_len: usize) -> io::Result<()> {
        unsafe {
            ffi::close(self.map_hndl)?;
            ffi::resize_file(self.file_hndl, new_len)?;
            self.map_hndl = ffi::create_mapping(self.file_hndl, self.map_protect)?;
        }

        self.len = new_len;

        Ok(())
    }

    pub fn view(&self, off: usize, len: usize, writable: bool) -> io::Result<RawView> {
        let desired_access = if writable {
            FILE_MAP_ALL_ACCESS
        } else {
            FILE_MAP_READ
        };

        let ptr = unsafe { ffi::map_view(self.map_hndl, desired_access, len, off)? };

        Ok(RawView { ptr, off, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for RawFile {
    fn drop(&mut self) {
        unsafe {
            // The order does not really matter.
            let _ = ffi::close(self.map_hndl);
            let _ = ffi::close(self.file_hndl);
        }
    }
}

pub struct RawView {
    ptr: ffi::RawPtr,
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
        unsafe { ffi::flush_view(self.ptr, self.len) }
    }
}

impl Drop for RawView {
    fn drop(&mut self) {
        unsafe {
            let _ = ffi::unmap_view(self.ptr);
        }
    }
}

pub fn get_alignment() -> usize {
    use winapi::um::sysinfoapi::{GetSystemInfo, SYSTEM_INFO};
    let mut system_info = SYSTEM_INFO::default();
    unsafe {
        GetSystemInfo(&mut system_info as *mut _);
    }
    system_info.dwAllocationGranularity as usize
}

fn lpcwstr(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    let mut wstr = path.as_os_str().encode_wide().collect::<Vec<_>>();
    wstr.push(0);
    wstr
}

mod ffi {
    use std::io;
    use std::ptr::NonNull;

    use winapi::{
        shared::{basetsd::SIZE_T, minwindef::DWORD, ntdef::LONGLONG},
        um::{
            fileapi::{CreateFileW, SetFileInformationByHandle, FILE_END_OF_FILE_INFO},
            handleapi::{CloseHandle, INVALID_HANDLE_VALUE},
            memoryapi::{CreateFileMappingW, FlushViewOfFile, MapViewOfFile, UnmapViewOfFile},
            minwinbase::FileEndOfFileInfo,
            winnt::WCHAR,
        },
    };

    pub type RawHandle = NonNull<winapi::ctypes::c_void>;
    pub type RawPtr = NonNull<winapi::ctypes::c_void>;

    pub unsafe fn create_file(
        path: &[WCHAR],
        desired_access: DWORD,
        share_mode: DWORD,
        creation: DWORD,
        attributes: DWORD,
    ) -> io::Result<RawHandle> {
        let hndl = CreateFileW(
            path.as_ptr(),
            desired_access,
            share_mode,
            std::ptr::null_mut(),
            creation,
            attributes,
            std::ptr::null_mut(),
        );

        if hndl == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            NonNull::new(hndl).ok_or(io::Error::last_os_error())
        }
    }

    pub unsafe fn resize_file(hndl: RawHandle, len: usize) -> io::Result<()> {
        let mut info = FILE_END_OF_FILE_INFO::default();
        *info.EndOfFile.QuadPart_mut() = len as LONGLONG;

        let result = SetFileInformationByHandle(
            hndl.as_ptr(),
            FileEndOfFileInfo,
            &mut info as *mut _ as *mut _,
            std::mem::size_of::<FILE_END_OF_FILE_INFO>() as DWORD,
        );

        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn create_mapping(hndl: RawHandle, protect: DWORD) -> io::Result<RawHandle> {
        // Passing 0,0 to maximum size arguments make the mapping the same size
        // as is the size of the file.
        let hndl = CreateFileMappingW(
            hndl.as_ptr(),
            std::ptr::null_mut(),
            protect,
            0,
            0,
            std::ptr::null(),
        );

        if hndl == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            NonNull::new(hndl).ok_or(io::Error::last_os_error())
        }
    }

    pub unsafe fn close(hndl: RawHandle) -> io::Result<()> {
        if CloseHandle(hndl.as_ptr()) == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn map_view(
        hndl: RawHandle,
        desired_access: DWORD,
        len: SIZE_T,
        offset: SIZE_T,
    ) -> io::Result<RawPtr> {
        let offset_high = (offset >> 32) as DWORD;
        let offset_low = offset as DWORD;

        let ptr = MapViewOfFile(hndl.as_ptr(), desired_access, offset_high, offset_low, len);

        NonNull::new(ptr).ok_or(io::Error::last_os_error())
    }

    pub unsafe fn unmap_view(base_address: RawPtr) -> io::Result<()> {
        if UnmapViewOfFile(base_address.as_ptr()) == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub unsafe fn flush_view(base_address: RawPtr, len: SIZE_T) -> io::Result<()> {
        if FlushViewOfFile(base_address.as_ptr(), len) == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
