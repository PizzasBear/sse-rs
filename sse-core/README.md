# sse-core

A high-performance, `no_std` compatible state-machine parser for Server-Sent
Events (SSE).

`sse-core` is designed to be the foundational parsing layer for SSE clients. It
does not perform any network I/O. Instead, it provides a highly efficient,
allocation-minimized state machine that consumes raw bytes and yields parsed SSE
events.

## Features

- **`no_std` Compatible:** Requires only the `alloc` crate, making it perfect
  for embedded environments or custom network stacks.
- **Zero-I/O State Machine:** The `SseDecoder` operates strictly on byte buffers
  (`bytes::Buf`), decoupling parsing from the transport layer.
- **Async Stream Wrapper:** Includes a lightweight `SseStream` wrapper to easily
  integrate with `futures-core::TryStream` (e.g., standard async network
  streams).
- **Memory Efficient:** Leverages `Cow` for event names to avoid allocating
  strings for standard `"message"` events, and cleanly enforces configurable
  maximum payload sizes to prevent memory exhaustion from malicious servers.
- **Jittered Backoff:** Includes a customizable mathematical utility
  (`SseRetryConfig`) for calculating exponential backoffs and reconnect delays.

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

- `jitter` _(enabled by default)_: Includes randomized jitter calculations in
  `SseRetryConfig` to prevent thundering herd problems on reconnects. Requires
  the `std` library. Disable this for `no_std` usage.
