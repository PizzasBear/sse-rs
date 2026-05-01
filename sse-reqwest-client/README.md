# sse-reqwest-client

A robust, auto-reconnecting Server-Sent Events (SSE) client built on top of
`reqwest`.

This crate provides a high-level, ergonomic API for consuming SSE streams. It
implements standard
[WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html)
behaviors out of the box, including automatic network reconnections, handling
`Last-Event-ID` tracking, and the recommended exponential backoff.

## Features

- **Ergonomic API:** Use the `.into_event_source()` extension trait to turn any
  `reqwest::RequestBuilder` into a stream of events instantly.
- **Standards Compliant:** Automatically handles `text/event-stream` validation,
  strictly adheres to HTTP closure rules, and attaches `Last-Event-ID` headers
  upon reconnection.
- **Auto-Reconnection:** Seamlessly recovers from dropped network connections
  and clean EOFs without losing your place in the stream.
- **Transient Error Recovery:** Includes opt-in support for automatically
  retrying recoverable proxy or server errors (e.g., 502, 503, 504, 429).
- **Smart Backoff:** Implements exponential backoff with jitter, automatically
  adapting to the delays requested by the server via `retry` events.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
sse-reqwest-client = "0.1"
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

If you need to configure payload limits, custom backoff strategies, or enable
transient error retries, use the `EventSourceBuilder`:

```rust,no_run
use std::{num::NonZeroUsize, time::Duration};

use sse_reqwest_client::{RequestBuilderExt, SseRetryConfig};

let client = reqwest::Client::new();

let mut stream = client
    .get("https://example.com/api/events")
    .into_event_source_builder()
    .retry_transient_errors(true) // Automatically retry on 502/503/504
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
