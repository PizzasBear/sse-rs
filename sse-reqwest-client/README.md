# sse-reqwest-client

A robust, auto-reconnecting Server-Sent Events (SSE) client built on top of
`reqwest`.

This crate provides a high-level, ergonomic API for consuming SSE streams. It
fully implements standard SSE behaviors, including automatic network
reconnections, exponential backoff, and tracking the `Last-Event-ID` to resume
dropped streams without missing a beat.

## Features

- **Ergonomic API:** Use the `.into_event_source()` extension trait to turn any
  `reqwest::RequestBuilder` into a stream of events instantly.
- **Auto-Reconnection:** Automatically handles dropped network connections, and
  timeouts.
- **Smart Backoff:** Implements exponential backoff with jitter to respect
  server load, automatically adapting to `retry` delays requested by the server.
- **Standards Compliant:** Automatically handles `text/event-stream` validation
  and attaches `Last-Event-ID` headers upon reconnection.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
sse-reqwest-client = "0.1"
reqwest = { version = "0.12", features = ["stream"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures-util = "0.3"
```

### Quick Start

The easiest way to start listening to events is via the `RequestBuilderExt`
trait:

```rust,no_run
use futures_util::StreamExt;
use sse_reqwest_client::{RequestBuilderExt, SseEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    // Convert a standard reqwest request into an EventSource
    let mut stream = client.get("https://example.com/api/events")
        .into_event_source();

    while let Some(event) = stream.next().await {
        match event? {
            SseEvent::Open => println!("Connection established!"),
            SseEvent::Message(msg) => {
                println!("Received event: {}", msg.event);
                println!("Payload: {}", msg.data);
            }
            SseEvent::Error(err) => {
                eprintln!("Connection dropped, attempting to reconnect: {}", err);
            }
        }
    }

    Ok(())
}
```

### Advanced Configuration

If you need to configure payload limits or custom backoff strategies, use the
`EventSourceBuilder`:

```rust,no_run
use sse_reqwest_client::{RequestBuilderExt, SseRetryConfig};
use std::{num::NonZeroUsize, time::Duration};

let client = reqwest::Client::new();
let req = client.get("https://example.com/api/events");

let retry_config = SseRetryConfig {
    max_retries: 10,
    backoff_multiplier: 1.5,
    ..Default::default()
};

let mut stream = req.into_event_source_builder()
    .retry_config(retry_config)
    .initial_reconnection_time(Duration::from_secs(5))
    .max_payload_size(NonZeroUsize::new(1024 * 1024).unwrap()) // 1MB limit
    .build();
```

## Important Note on Timeouts

When configuring your `reqwest::Client`, **do not use
`reqwest::ClientBuilder::timeout()`**. That method enforces a strict time limit
on the _entire lifespan_ of the HTTP request, which will forcefully terminate
your persistent SSE stream.

Instead, to handle dead sockets, use TCP Keepalive:

```rust,ignore
let client = reqwest::Client::builder()
    .tcp_keepalive(std::time::Duration::from_secs(15))
    .build()?;
```
