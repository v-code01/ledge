pub mod disk;
pub mod io;

pub use disk::DiskObjectStore;
pub use io::{IoBackend, PlatformIo};
