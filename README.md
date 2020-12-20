# Harrow

A Rust low-level library for making file-backed storage for huge data
structures.

In short, *harrow* opens a (possibly temporary) file and provides random access
to portions of the file, while managing a faster-access cache with virtually
mapped buffers. The use case is when the data is larger than is possible to fit
or reasonable to hold in memory.

*CAUTION:* The library uses a lot of *unsafe* and OS-specific APIs, without any
deep knowledge of either by the author. Do not use it where animals may be
harmed. Any help with testing and reviewing is much appreciated.

Supported platforms (as far as a small bunch of tests indicate):

* Linux (works on my machine)
* MacOS (I suppose for its unixness)
* Windows (works on Windows 10 inside VirtualBox)

## [Documentation](https://docs.rs/harrow)

## Roadmap

* Ensure correctness with tests and thorough reviews
* Improve average performance
* Polish and extend APIs
  * This partially includes implementing standard traits as well as choosing
    appropriate internal implementation to allow more auto trait
    implementations
* Automatic multi-platform testing in CI

Contributions are welcome!

## Name

[Harrow](https://en.wikipedia.org/wiki/Harrow_(tool)) is an agricultural tool
used for preparing the soil structure on a field that is suitable for planting
seeds. So the metaphor is obvious: *harrow* will prepare a file for you in which
you can seed your bytes as you need. Files are bigger than operational memory in
the same way as fields are bigger than gardens.

## License

Dual-licensed under [MIT](LICENSE) and [UNLICENSE](UNLICENSE). Feel free to use
it, contribute or spread the word.
