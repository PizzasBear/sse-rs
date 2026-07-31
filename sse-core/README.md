# sse-core

[![Documentation](https://docs.rs/sse-core/badge.svg)](https://docs.rs/sse-core)
[![Latest version](https://img.shields.io/crates/v/sse-core.svg)](https://crates.io/crates/sse-core)

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

    while let Some(event) = decoder.next(&mut buffer) {
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

## Performance

`sse-core` is built from the ground up for zero-copy, zero-allocation parsing
where possible. By relying on `bytes::Buf`, the state machine advances an
internal cursor rather than shifting memory or eagerly allocating strings.

This architecture provides massive performance gains, particularly when
processing heavily fragmented network streams or large data payloads.

The benchmarks were ran with realistic network fragmentation across standard
1460-byte TCP packet boundaries unless said otherwise.

| Benchmark Scenario                                 | `sse-core`      | `eventsource-stream` | `sse-stream`    |
| :------------------------------------------------- | :-------------- | :------------------- | :-------------- |
| **Large Events (40KiB)**                           | **~16.9 GiB/s** | ~70.3 MiB/s          | ~1.43 GiB/s     |
| **Small Events (9B)**                              | **~483 MiB/s**  | ~118 MiB/s           | ~316 MiB/s      |
| **Keepalives**                                     | **~1.55 GiB/s** | ~151 MiB/s           | ~1.53 GiB/s     |
| **High Fragmentation (4KiB data, 10-byte chunks)** | **~574 MiB/s**  | ~5.00 MiB/s          | ~69.6 MiB/s     |

Keepalives are the one scenario where `sse-core` holds no real lead: a stream of
comment lines is the worst case for a resumable state machine, since it maximises
the number of lines per byte, and the gap to `sse-stream` there is smaller than
the run-to-run spread. That is the same trade that wins the fragmentation row by
an order of magnitude — `sse-stream` splits a whole chunk into lines at once,
which is fast until a line is split across chunks.

These benchmarks were run using `cargo bench` on a Ryzen 5900x Linux PC.

Because micro-benchmarks operating at sub-millisecond speeds are highly
sensitive to
[code alignment](https://www.bazhenov.me/posts/2024-02-performance-roulette/)
and instruction cache thrashing, these results represent the stabilized metrics.
To eliminate layout-induced variance and ensure reproducible results, the
benchmarks were compiled using Fat LTO alongside explicit LLVM alignment flags:
`RUSTFLAGS="-C llvm-args=-align-all-functions=6 -C llvm-args=-align-all-nofallthru-blocks=6"`

The figures above are medians of seven full runs of the suite, each pinned to a
single core. Machine noise only ever makes a run slower, so the spread across
runs — 3–7% for most entries, and up to 24% for `sse-stream` on large events —
is a better guide to how much a difference means than any single run is.

## Feature Flags

`sse-core` is highly configurable, allowing you to strip out async or standard
library dependencies for constrained environments.

- **`std` (default):** Enables standard library support. Disable this for
  `no_std` environments. (note: the `alloc` crate is still required).
- **`stream` (default):** Enables the `SseStream` wrapper for asynchronous byte
  streams. Disable this if you only need the raw, synchronous `SseDecoder` state
  machine.
- **`fastrand` (default):** Uses the `fastrand` crate to generate the random
  jitter value used to prevent thundering herd scenarios (requires `std`).
- **`serde`:** Implements `serde`'s `Serialize` and `Deserialize` traits on
  common types.

### `no_std` Usage

To use `sse-core` in a `no_std` environment (using only the raw state-machine
parser), disable the default features in your `Cargo.toml`:

```toml
[dependencies]
sse-core = { version = "0.2", default-features = false, features = ["stream"] }

# Or if you don't need async
sse-core = { version = "0.2", default-features = false }
```
