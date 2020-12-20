pub trait ResultExt<T, E> {
    /// Calls the `clean` callback if the result is an error.
    ///
    /// Use this in constructors of low-level structures where individual steps
    /// can fail and, since the structure is not yet constructed, no drop is
    /// called on the so-far initialized resources.
    fn cleanup<F, R>(self, clean: F) -> Self
    where
        F: FnOnce() -> R;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn cleanup<F, R>(self, clean: F) -> Self
    where
        F: FnOnce() -> R,
    {
        match self {
            Err(_) => {
                clean();
            }
            _ => {}
        }
        self
    }
}
