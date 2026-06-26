# Server-Sent Events (SSE) for Rust

A high-performance, robust, and ergonomically designed Server-Sent Events (SSE)
ecosystem for Rust.

This repository is split into two distinct crates to provide both low-level
control for custom network stacks and a high-level, "plug-and-play" client for
standard application development.

## The Crates

| Crate                                        | Description                                                        | Environment           |
| -------------------------------------------- | ------------------------------------------------------------------ | --------------------- |
| [`sse-core`](./sse-core)                     | A zero-I/O, highly efficient state-machine parser for SSE.         | `no_std` compatible   |
| [`sse-reqwest-client`](./sse-reqwest-client) | A fully-featured, auto-reconnecting SSE client built on `reqwest`. | `std` (Tokio/Reqwest) |

### Which one should I use?

- **Use `sse-reqwest-client`** if you are building a standard async Rust
  application, web scraper, or backend service and just want to consume an SSE
  API effortlessly.
- **Use `sse-core`** if you are building for embedded systems (`no_std`),
  writing a custom HTTP/TCP stack, or need absolute control over memory buffers
  and parsing execution.

---

## Quick Start: The High-Level Client

If you just want to connect to an SSE endpoint and start receiving events, use
`sse-reqwest-client`. It handles all the underlying network complexity,
including automatic reconnections, exponential backoff, and tracking the
`Last-Event-ID`.

Add this to your `Cargo.toml`:

```toml
[dependencies]
sse-reqwest-client = "0.3"
reqwest = { version = "0.12", features = ["stream"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures-util = "0.3"
```

And connect using the extension trait:

```rust
use sse_reqwest_client::RequestBuilderExt;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        // Highly recommended to configure TCP keepalive for SSE streams
        .tcp_keepalive(std::time::Duration::from_secs(15))
        .build()?;

    // Convert a standard reqwest request into an auto-reconnecting EventSource
    let mut stream = client.get("https://example.com/api/events")
        .into_event_source();

    while let Some(event) = stream.next().await {
        match event? {
            sse_reqwest_client::Event::Open => println!("Connection established!"),
            sse_reqwest_client::Event::Message(msg) => {
                println!("Received event: {}", msg.event);
                println!("Payload: {}", msg.data);
            }
            sse_reqwest_client::Event::Error(err) => {
                eprintln!("Connection dropped, attempting to reconnect: {}", err);
            }
        }
    }

    Ok(())
}
```

## Architecture & Design Philosophy

This project strictly adheres to separation of concerns to provide the most
reliable parsing possible without sacrificing ergonomics.

1. **Zero-I/O Parsing:** The core parser (`SseDecoder`) never touches a socket.
   It reads from byte buffers and yields events. This ensures the parsing logic
   is completely isolated from network transport errors.
2. **"Make Invalid States Unrepresentable":** The API is specifically designed
   to enforce correct SSE behavior. For example, injecting a `Last-Event-ID`
   manually can only be done while physically attaching a new stream, preventing
   accidental state desyncs between the client and server.
3. **Resilience by Default:** The client crate implements exponential backoff
   with jitter to respect server load, and handles `text/event-stream`
   validation automatically.

## License

This project is licensed under the [MIT License](LICENSE-MIT) or
[Apache 2.0 License](LICENSE-APACHE), at your option.
