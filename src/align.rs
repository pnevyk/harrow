use once_cell::sync::Lazy;

use crate::os;

pub static ALIGNMENT: Lazy<usize> = Lazy::new(|| os::get_alignment());

pub fn align_add(len: usize) -> usize {
    let alignment = *ALIGNMENT;
    let offset = len % alignment;
    len + if offset > 0 { alignment - offset } else { 0 }
}

pub fn align_sub(len: usize) -> usize {
    let alignment = *ALIGNMENT;
    let factor = len / alignment;
    factor * alignment
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment() {
        let alignment = os::get_alignment();
        assert_eq!(align_add(1), alignment);
        assert_eq!(align_add(0), 0);
        assert_eq!(align_add(alignment), alignment);
        assert_eq!(align_add(alignment + 1), 2 * alignment);

        assert_eq!(align_sub(1), 0);
        assert_eq!(align_sub(0), 0);
        assert_eq!(align_sub(alignment), alignment);
        assert_eq!(align_sub(alignment + 1), alignment);
    }
}
