#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub use decode::{MessageEvent, PayloadTooLargeError, SseDecoder, SseEvent};
pub use retry::SseRetryConfig;
#[cfg(feature = "stream")]
pub use stream::{SseStream, SseStreamError, SseStreamResult};

mod decode;
mod retry;
#[cfg(feature = "stream")]
mod stream;
