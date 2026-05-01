# sse-core

A high-performance, `no_std` compatible state-machine parser for Server-Sent
Events (SSE).

`sse-core` is designed to be the foundational parsing layer for SSE clients. It
does not perform any network I/O. Instead, it provides a highly efficient state
machine that consumes raw byte buffers and yields parsed SSE events.

## Features

- **`no_std` Compatible:** Requires only the `alloc` crate, making it perfect
  for embedded environments or custom network stacks.
- **Zero-I/O State Machine:** Operates strictly on byte buffers (`bytes::Buf`),
  cleanly decoupling parsing logic from the transport layer.
- **Async Stream Wrapper:** Includes an optional `SseStream` wrapper to easily
  integrate with standard async network streams (`futures-core::TryStream`).
- **Memory Safe:** Enforces strict, configurable maximum payload sizes to
  prevent memory exhaustion from malicious or misconfigured servers.
- **Smart Backoff:** Includes a customizable utility (`SseRetryConfig`) for
  calculating exponential backoffs and reconnect delays with jitter.

## Usage

### The Low-Level Decoder

If you are managing your own buffers, use the `SseDecoder` directly:

```rust
use bytes::Bytes;
use sse_core::SseDecoder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut decoder = SseDecoder::new();
    let mut buffer = Bytes::from("data: hello world\n\n");

    while let Some(event) = decoder.next(&mut buffer)? {
        println!("Parsed event: {:?}", event);
    }
    Ok(())
}
```

### The Async Stream Wrapper

If you have an existing async byte stream (like a TCP socket or HTTP body), wrap
it in `SseStream`:

```rust,ignore
use sse_core::SseStream;
use futures_util::StreamExt;

// Assume `tcp_byte_stream` implements `TryStream<Ok = bytes::Bytes>`
let mut stream = SseStream::new(tcp_byte_stream);

while let Some(result) = stream.next().await {
    match result {
        Ok(event) => println!("Event: {:?}", event),
        Err(e) => eprintln!("Stream error: {}", e),
    }
}
```

## Feature Flags

`sse-core` is highly configurable, allowing you to strip out async or standard
library dependencies for constrained environments. By default, **all features
are enabled**.

- `std`: Enables standard library support. Disable this for `no_std`
  environments. (note: the `alloc` crate is still required).
- `stream`: Enables the `SseStream` wrapper for asynchronous byte streams.
  Disable this if you only need the raw, synchronous `SseDecoder` state machine.
- `fastrand`: Enables randomized jitter calculations in `SseRetryConfig` to
  prevent thundering herd scenarios (requires `std`).
- `serde`: Implements `serde`'s `Serialize` and `Deserialize` traits on common
  types.

### `no_std` Usage

To use `sse-core` in a `no_std` environment (using only the raw state-machine
parser), disable the default features in your `Cargo.toml`:

```toml
[dependencies]
sse-core = { version = "0.1", default-features = false }

# Or if you need async
sse-core = { version = "0.1", default-features = false, features = ["stream"] }
```
