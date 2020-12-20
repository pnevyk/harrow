use std::fmt;
use std::io;
use std::ops::Deref;
use std::path::Path;

use harrow::{FileRef, ViewRef};

struct Lines {
    data: FileRef,
    offsets: Vec<usize>,
}

impl Lines {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        // The chunks of file contents are mapped into the virtual memory of the
        // process as needed. But overall memory footprint of this approach will
        // remain small, as long as the user does not ask for huge chunks from
        // the file.
        let data = FileRef::new(path)?;

        // We will however allocate a vector on the heap for the offsets to the
        // file.
        let mut offsets = Vec::new();

        // Traverse the file to discover newline characters and store the
        // offsets to line beginnings.
        let mut last_offset = 0;
        offsets.push(last_offset);
        for (offset, byte) in data.iter()?.enumerate() {
            // Is the byte the newline character?
            if byte == '\n' as u8 {
                // Check if the line is valid UTF-8.
                std::str::from_utf8(&data.view_range(last_offset..offset)?)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

                last_offset = offset + 1;
                offsets.push(last_offset);
            }
        }

        // Store the offset of EOF.
        offsets.push(data.len());

        Ok(Self { data, offsets })
    }

    pub fn get(&self, line: usize) -> Line<'_> {
        let start = self.offsets[line];
        let end = self.offsets[line + 1];
        let view = self.data.view_range(start..end).unwrap();
        Line::new(view)
    }
}

struct Line<'a>(ViewRef<'a>);

impl<'a> Line<'a> {
    pub fn new(view: ViewRef<'a>) -> Self {
        // For safety, check the validity of the bytes.
        std::str::from_utf8(view.as_slice()).unwrap();
        Self(view)
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: We checked the validity in the constructor. The underlying
        // memory can't be changed while we are holding the ViewRef.
        unsafe { std::str::from_utf8_unchecked(self.0.as_slice()) }
    }
}

impl Deref for Line<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for Line<'_> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for Line<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl fmt::Display for Line<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

fn main() {
    let lines = Lines::new("examples/lorem.txt").unwrap();
    println!("Text on 3rd line: {}", lines.get(2));
}
