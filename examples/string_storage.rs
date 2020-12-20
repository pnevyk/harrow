use std::io;
use std::path::Path;

use harrow::FileMut;

struct StrPtr {
    start: usize,
    len: usize,
}

struct StringStorage {
    data: FileMut,
    len: usize,
}

impl StringStorage {
    pub fn new<P: AsRef<Path>>(temp_path: P) -> io::Result<Self> {
        // Let's use a bit smaller block size for caching if we don't expect
        // that much long strings. Moreover, set the initial size of the file to
        // even smaller value to avoid possibly unnecessary big file.
        const SMALLER_BLOCK_SIZE: usize = 4096;
        const INITIAL_FILE_SIZE: usize = 1024;

        Ok(Self {
            data: FileMut::with_cache(temp_path, INITIAL_FILE_SIZE, 2, SMALLER_BLOCK_SIZE)?,
            len: 0,
        })
    }

    pub fn add(&mut self, value: &str) -> io::Result<StrPtr> {
        // Prepare the pointer to the added string. It will begin at the current
        // tail.
        let ptr = StrPtr {
            start: self.len,
            len: value.len(),
        };

        // Ensure that we have enough space for the new string. If not, we
        // double the allocated size, or set it to the end of the pointer if
        // this exceeds would-be new size.
        if self.len + value.len() > self.data.len() {
            let new_len = std::cmp::max(2 * self.data.len(), ptr.start + ptr.len);
            self.data.resize(new_len)?;
        }

        // Write the string to the buffer and update the tail pointer.
        self.data.write_at(value.as_bytes(), self.len)?;
        self.len += value.len();

        Ok(ptr)
    }

    pub fn get(&self, ptr: &StrPtr) -> String {
        // Acquire the view for the string. Let's believe that the allocation
        // succeeds.
        let view = self.data.view(ptr.start, ptr.len).unwrap();
        // We have written a valid string and we don't expect that the
        // underlying data changed (but we can't be 100% sure since the storage
        // is backed by a file).
        std::string::String::from_utf8(view.to_owned()).unwrap()
    }
}

fn main() {
    let mut strings = StringStorage::new("temp.txt").unwrap();
    let hello_world = strings.add("Hello world!").unwrap();
    strings.add("Foobar").unwrap();

    assert_eq!(strings.get(&hello_world), "Hello world!");
}
