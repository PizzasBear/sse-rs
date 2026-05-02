#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Duration,
};

use bytes::Bytes;
use futures_core::stream::Stream;
use reqwest::{RequestBuilder, StatusCode, header::HeaderValue};
use thiserror::Error;
use tokio::time::{Instant, Sleep, sleep};

pub use sse_core::SseRetryConfig;
use sse_core::{
    MessageEvent, PayloadTooLargeError, SseDecoder, SseEvent as SseEventCore, SseStream,
    SseStreamError,
};

type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + Sync>>;
type ConnectFuture =
    Pin<Box<dyn Future<Output = Result<reqwest::Response, reqwest::Error>> + Send + Sync>>;

/// An alias for [`Result`] with the error defaulting to [`Error`](enum@Error).
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that can occur during the lifecycle of an [`EventSource`] connection.
#[derive(Debug, Error)]
pub enum Error {
    /// The server responded with a non-200 HTTP status code.
    #[error("unexpected HTTP status code: {0}")]
    Status(StatusCode),
    /// The [`RequestBuilder`] could not be cloned (e.g., it contains a streaming body).
    #[error("request builder could not be cloned (e.g., non-restartable body stream)")]
    UncloneableRequest,
    /// The server's response lacked the `text/event-stream` Content-Type.
    #[error("invalid response HTTP Content-Type")]
    InvalidContentType,
    /// The server's response did not contain a Content-Type header.
    #[error("response HTTP Content-Type missing")]
    MissingContentType,
    /// The client exhausted all retry attempts without successfully reconnecting.
    #[error("couldn't reconnect to SSE server in {0} attempts: {1}")]
    Timeout(u32, SseErrorEvent),
    /// The server sent an event payload that exceeded the configured buffer limit.
    #[error("server sent an oversized payload exceeding the allotted buffer")]
    PayloadTooLarge(#[from] PayloadTooLargeError),
    /// The `Last-Event-ID` provided by the server contains bytes that cannot be
    /// safely converted into a valid HTTP header.
    #[error("Last-Event-ID cannot be converted to a valid HTTP header: {0}")]
    InvalidLastEventId(reqwest::header::InvalidHeaderValue),
}

/// The connection state mapping to the JavaScript [`EventSource`](https://developer.mozilla.org/en-US/docs/Web/API/EventSource) API [`readyState`](https://developer.mozilla.org/en-US/docs/Web/API/EventSource/readyState).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum ReadyState {
    /// The connection has not yet been established, or it was closed and the client is reconnecting.
    Connecting = 0,
    /// The connection is open and ready to receive events.
    Open = 1,
    /// The connection is permanently closed and will not reconnect.
    Closed = 2,
}

enum State {
    Disconnected,
    Connecting(ConnectFuture),
    Open,
    Sleeping(Pin<Box<Sleep>>),
    Closed,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Disconnected => f.write_str("Disconnected"),
            State::Connecting(_) => f.write_str("Connecting(_)"),
            State::Open => f.write_str("Open"),
            State::Sleeping(fut) => f.debug_tuple("Sleeping").field(fut).finish(),
            State::Closed => f.write_str("Closed"),
        }
    }
}

/// Transient errors that cause the stream to drop and trigger an automatic reconnection.
#[derive(Debug, Error)]
pub enum SseErrorEvent {
    /// The server gracefully closed the TCP connection (EOF) while the stream was active.
    #[error("server cleanly closed the connection (EOF)")]
    Eof,

    /// The server responded with an HTTP status code designated as retryable (e.g., 502, 503).
    #[error("transient HTTP error: {0}")]
    Http(StatusCode),

    /// A network-level error occurred, such as a dropped socket, DNS failure, or read timeout.
    #[error("network or transport error: {0}")]
    Network(#[from] reqwest::Error),
}

/// High-level events emitted by the [`EventSource`] stream.
#[derive(Debug)]
pub enum SseEvent {
    /// Emitted when the underlying HTTP connection is successfully established.
    Open,
    /// A parsed message event from the server.
    Message(MessageEvent),
    /// Emitted when the connection drops but the client is actively attempting to reconnect.
    ///
    /// This gives the application a chance to log the interruption or update UI state
    /// while the exponential backoff handles the recovery in the background.
    Error(SseErrorEvent),
}

impl SseEvent {
    /// Consumes the event and returns the underlying [`MessageEvent`] if this is a standard message.
    ///
    /// This is particularly useful in stream combinators:
    /// ```no_run
    /// # use futures_util::TryStreamExt;
    /// # use sse_reqwest_client::*;
    /// # tokio_test::block_on(async {
    /// # let client = reqwest::Client::new();
    /// # let mut stream = client.get("https://example.com/events").into_event_source();
    /// let messages: Vec<_> = stream
    ///     .try_filter_map(async |res| Ok(res.into_message()))
    ///     .try_collect()
    ///     .await?;
    /// # Result::<()>::Ok(())
    /// # });
    /// ```
    pub fn into_message(self) -> Option<MessageEvent> {
        match self {
            Self::Message(msg) => Some(msg),
            Self::Open | Self::Error(_) => None,
        }
    }

    /// Returns a reference to the underlying [`MessageEvent`] if this is a standard message.
    pub fn as_message(&self) -> Option<&MessageEvent> {
        match self {
            Self::Message(msg) => Some(msg),
            Self::Open | Self::Error(_) => None,
        }
    }

    /// Returns a mutable reference to the underlying [`MessageEvent`] if this is a standard message.
    pub fn as_message_mut(&mut self) -> Option<&mut MessageEvent> {
        match self {
            Self::Message(msg) => Some(msg),
            Self::Open | Self::Error(_) => None,
        }
    }
}

impl From<MessageEvent> for SseEvent {
    fn from(event: MessageEvent) -> Self {
        Self::Message(event)
    }
}

impl From<SseErrorEvent> for SseEvent {
    fn from(err: SseErrorEvent) -> Self {
        Self::Error(err)
    }
}

/// Error indicating that an [`SseEvent`] could not be converted into a [`MessageEvent`].
#[derive(Debug, Error)]
#[error("couldn't convert Event::{} into a MessageEvent", match .0 {
    SseEvent::Open => "Open",
    SseEvent::Message(_) => "Message",
    SseEvent::Error(_) => "Error"
})]
pub struct FromMessageEventError(pub SseEvent);

impl TryFrom<SseEvent> for MessageEvent {
    type Error = FromMessageEventError;

    fn try_from(ev: SseEvent) -> Result<Self, Self::Error> {
        match ev {
            SseEvent::Message(msg) => Ok(msg),
            ev => Err(FromMessageEventError(ev)),
        }
    }
}

/// A builder for configuring an [`EventSource`] connection.
///
/// # Example
/// ```rust,no_run
/// use std::{num::NonZeroUsize, time::Duration};
/// use sse_reqwest_client::{RequestBuilderExt, EventSourceBuilder, SseRetryConfig};
///
/// # #[tokio::main]
/// # async fn main() {
/// let client = reqwest::Client::new();
/// let req = client.get("https://api.example.com/stream");
///
/// // Create a stream with a strict 1MB payload limit and a custom retry delay
/// let stream = req.into_event_source_builder()
///     .retry_config(SseRetryConfig::new())
///     .initial_reconnection_time(Duration::from_secs(5))
///     .max_payload_size(NonZeroUsize::new(1024 * 1024).unwrap())
///     .build();
/// # }
/// ```
#[derive(Debug)]
pub struct EventSourceBuilder {
    req: RequestBuilder,
    retry_config: SseRetryConfig,
    reconnection_time_ms: u32,
    max_payload_size: Option<NonZeroUsize>,
    last_event_id: Option<Arc<str>>,
    retry_transient_errors: bool,
    successful_connection_threshold: Duration,
}

impl EventSourceBuilder {
    /// Creates a new builder wrapping the given [`reqwest::RequestBuilder`].
    #[must_use]
    pub fn new(req: RequestBuilder) -> Self {
        Self {
            req,
            reconnection_time_ms: 3000, // Default per SSE Spec
            retry_config: SseRetryConfig::new(),
            max_payload_size: None, // use default
            last_event_id: None,
            retry_transient_errors: false,
            successful_connection_threshold: Duration::from_secs(5),
        }
    }

    /// Applies a custom retry configuration for automatic reconnections.
    #[inline]
    #[must_use]
    pub fn retry_config(mut self, retry_config: SseRetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Sets the base delay to wait before attempting to reconnect.
    ///
    /// This delay may be overridden by the server using `retry` events.
    #[inline]
    #[must_use]
    pub fn initial_reconnection_time(mut self, reconnection_time: Duration) -> Self {
        self.reconnection_time_ms = reconnection_time
            .as_millis()
            .try_into()
            .expect("Read duration too long");
        self
    }

    /// Configures the maximum allowed byte size for a single event payload.
    #[inline]
    #[must_use]
    pub fn max_payload_size(mut self, max_payload_size: NonZeroUsize) -> Self {
        self.max_payload_size = Some(max_payload_size);
        self
    }

    /// Sets the initial `Last-Event-ID` to send with the first connection request.
    ///
    /// This is useful for resuming a dropped stream from a previously saved state.
    #[inline]
    #[must_use]
    pub fn last_event_id(mut self, id: impl Into<Arc<str>>) -> Self {
        self.last_event_id = Some(id.into());
        self
    }

    /// Enables automatic retries for transient HTTP status codes.
    ///
    /// By default, the [`EventSource`] strictly follows the WHATWG specification and will
    /// permanently close the stream on any non-200 HTTP response.
    ///
    /// Setting this to `true` allows the client to automatically back off and retry when
    /// encountering temporary proxy or server issues. The following status codes are
    /// considered transient:
    /// * `408 Request Timeout`
    /// * `429 Too Many Requests`
    /// * `502 Bad Gateway`
    /// * `503 Service Unavailable`
    /// * `504 Gateway Timeout`
    #[inline]
    #[must_use]
    pub fn retry_transient_errors(mut self, retry: bool) -> Self {
        self.retry_transient_errors = retry;
        self
    }

    /// Sets the minimum duration a connection must remain open to be considered "successful"
    /// and reset the exponential backoff counter. Defaults to 5 seconds.
    #[inline]
    #[must_use]
    pub fn successful_connection_threshold(mut self, threshold: Duration) -> Self {
        self.successful_connection_threshold = threshold;
        self
    }

    /// Consumes the builder and returns the configured [`EventSource`].
    #[must_use]
    pub fn build(self) -> EventSource {
        let mut decoder = match self.max_payload_size {
            Some(max_payload_size) => SseDecoder::with_limit(max_payload_size),
            None => SseDecoder::new(),
        };
        decoder.reconnect_with_id(self.last_event_id);

        EventSource {
            req: (self.req)
                .header(reqwest::header::ACCEPT, "text/event-stream")
                .header(reqwest::header::CACHE_CONTROL, "no-store"),
            reconnection_time_ms: self.reconnection_time_ms,
            connection_attempt: 0,
            connected_since: None,
            retry_config: self.retry_config,
            retry_transient_errors: self.retry_transient_errors,
            successful_connection_threshold: self.successful_connection_threshold,
            stream: SseStream::with_decoder(decoder),
            state: State::Disconnected,
        }
    }
}

/// A reconnecting stream of Server-Sent Events.
pub struct EventSource {
    req: RequestBuilder,
    reconnection_time_ms: u32,
    connection_attempt: u32,
    connected_since: Option<Instant>,
    retry_config: SseRetryConfig,
    retry_transient_errors: bool,
    successful_connection_threshold: Duration,
    stream: SseStream<ByteStream>,
    state: State,
}

impl fmt::Debug for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSource")
            .field("req", &self.req)
            .field("reconnection_time_ms", &self.reconnection_time_ms)
            .field("connection_attempt", &self.connection_attempt)
            .field("retry_config", &self.retry_config)
            .field("retry_transient_errors", &self.retry_transient_errors)
            .field("state", &self.state)
            .field(
                "stream.last_event_id()",
                &self.stream.last_event_id().map(|id| &**id),
            )
            .field("stream.is_closed()", &self.stream.is_closed())
            .finish_non_exhaustive()
    }
}

impl EventSource {
    /// Creates a new [`EventSource`] from the given request with default configurations.
    #[must_use]
    pub fn new(req: RequestBuilder) -> Self {
        Self::builder(req).build()
    }

    /// Creates a builder to customize the [`EventSource`] before connecting.
    #[must_use]
    pub fn builder(req: RequestBuilder) -> EventSourceBuilder {
        EventSourceBuilder::new(req)
    }

    /// Closes the underlying SSE connection.
    ///
    /// This method terminates the active HTTP request, effectively dropping the
    /// inner stream. Calling [`close`](Self::close) is idempotent; if the connection is already
    /// closed, this does nothing and is perfectly safe to call multiple times.
    ///
    /// While this halts all incoming events and stops the automatic reconnection
    /// loop, it does not consume the [`EventSource`]. You can manually restart the
    /// stream and initiate a fresh connection at any time by calling
    /// [`force_reconnect()`](Self::force_reconnect).
    ///
    /// # Example
    /// ```rust,no_run
    /// # use futures_util::StreamExt;
    /// # use sse_reqwest_client::{RequestBuilderExt, ReadyState};
    /// # #[tokio::main]
    /// # async fn main() {
    /// let client = reqwest::Client::new();
    /// let mut stream = client.get("https://api.example.com/stream").into_event_source();
    ///
    /// // ... later, to gracefully stop listening to events (e.g., app goes to background):
    /// stream.close();
    /// assert_eq!(stream.ready_state(), ReadyState::Closed);
    ///
    /// // The stream will now yield None
    /// assert!(stream.next().await.is_none());
    /// # }
    /// ```
    pub fn close(&mut self) {
        self.stream.close();
        self.state = State::Closed;
    }

    /// Returns the current connection state.
    #[inline]
    #[must_use]
    pub fn ready_state(&self) -> ReadyState {
        match &self.state {
            State::Disconnected | State::Connecting(_) | State::Sleeping(_) => {
                ReadyState::Connecting
            }
            State::Open => ReadyState::Open,
            State::Closed => ReadyState::Closed,
        }
    }

    /// Returns the most recently received `Last-Event-ID`, if any.
    #[inline]
    #[must_use]
    pub fn last_event_id(&self) -> Option<&Arc<str>> {
        self.stream.last_event_id()
    }

    /// Terminates the current connection and immediately attempts to reconnect.
    ///
    /// Because [`EventSource`] automatically handles network drops and reconnections, you typically
    /// do not need to call this manually. However, it is useful in specific scenarios, such as:
    ///
    /// * **Bypassing Backoff:** The server state has heavily desynced, and you want to bypass the
    ///   current exponential backoff timer to reconnect instantly.
    /// * **Manual Revival:** You previously called [`close()`](Self::close) to pause the stream,
    ///   and now want to resume listening for events.
    ///
    /// This method resets the connection attempt counter, meaning the next connection attempt will
    /// happen immediately without any retry delay. And will continue with the exponential backoff
    /// reset.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sse_reqwest_client::{RequestBuilderExt, ReadyState};
    /// # #[tokio::main]
    /// # async fn main() {
    /// let client = reqwest::Client::new();
    /// let mut stream = client.get("https://api.example.com/stream").into_event_source();
    ///
    /// // If your application detects via the OS that network connectivity was restored,
    /// // you can manually trigger an immediate reconnect to bypass active backoff delays.
    /// stream.force_reconnect();
    /// assert_eq!(stream.ready_state(), ReadyState::Connecting);
    /// # }
    /// ```
    #[inline]
    pub fn force_reconnect(&mut self) {
        self.stream.close();
        self.connection_attempt = 0;
        self.state = State::Disconnected;
    }

    /// Terminates the current connection and immediately attempts to reconnect,
    /// explicitly overriding the `Last-Event-ID` sent to the server.
    ///
    /// This is useful if your application state has desynced and you need to
    /// force the server to rewind or fast-forward to a specific point in the stream.
    ///
    /// See [force_reconnect()](Self::force_reconnect) for more info.
    #[inline]
    pub fn force_reconnect_with_id(&mut self, id: Option<Arc<str>>) {
        self.stream.close_with_id(id);
        self.connection_attempt = 0;
        self.state = State::Disconnected;
    }

    fn go_to_sleep(&mut self, cause: SseErrorEvent) -> Result<SseEvent> {
        if let Some(connected_since) = self.connected_since.take() {
            if self.successful_connection_threshold <= connected_since.elapsed() {
                self.connection_attempt = 0;
            }
        }

        let wait_dur = (self.retry_config)
            .calculate_backoff(self.reconnection_time_ms, self.connection_attempt);
        self.connection_attempt += 1;
        if let Some(dur) = wait_dur {
            self.state = State::Sleeping(Box::pin(sleep(dur)));
            Ok(SseEvent::Error(cause))
        } else {
            self.close();
            Err(Error::Timeout(self.connection_attempt, cause))
        }
    }
}

impl Stream for EventSource {
    type Item = Result<SseEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let slf = &mut *self;

        loop {
            match &mut slf.state {
                State::Disconnected => {
                    let Some(mut req) = slf.req.try_clone() else {
                        slf.close();
                        return Poll::Ready(Some(Err(Error::UncloneableRequest)));
                    };

                    // TODO: Maybe we should error if the provided RequestBuilder already had a
                    //       Last-Event-ID header.
                    if let Some(last_event_id) = slf.stream.last_event_id() {
                        match HeaderValue::from_str(last_event_id) {
                            Ok(val) => req = req.header("Last-Event-ID", val),
                            Err(err) => {
                                slf.close();
                                return Poll::Ready(Some(Err(Error::InvalidLastEventId(err))));
                            }
                        }
                    }

                    let fut = Box::pin(req.send());
                    slf.state = State::Connecting(fut);
                }

                State::Connecting(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(res) => {
                        let status = res.status();

                        if matches!(status, StatusCode::NO_CONTENT) {
                            slf.close();
                            return Poll::Ready(None);
                        }

                        let is_transient_error = matches!(
                            status,
                            StatusCode::REQUEST_TIMEOUT
                                | StatusCode::TOO_MANY_REQUESTS
                                | StatusCode::BAD_GATEWAY
                                | StatusCode::SERVICE_UNAVAILABLE
                                | StatusCode::GATEWAY_TIMEOUT
                        );

                        if slf.retry_transient_errors && is_transient_error {
                            return Poll::Ready(Some(slf.go_to_sleep(SseErrorEvent::Http(status))));
                        } else if status != StatusCode::OK {
                            slf.close();
                            return Poll::Ready(Some(Err(Error::Status(status))));
                        }

                        let Some(content_type) = res
                            .headers()
                            .get(reqwest::header::CONTENT_TYPE)
                            .map(|v| v.as_bytes())
                        else {
                            slf.close();
                            return Poll::Ready(Some(Err(Error::MissingContentType)));
                        };

                        const MIME_EVENT_STREAM: &str = "text/event-stream";
                        if !(content_type.starts_with(MIME_EVENT_STREAM.as_bytes())
                            && matches!(
                                content_type.get(MIME_EVENT_STREAM.len()),
                                None | Some(b';' | b' ' | b'\t')
                            ))
                        {
                            slf.close();
                            return Poll::Ready(Some(Err(Error::InvalidContentType)));
                        }

                        slf.state = State::Open;
                        slf.connected_since = Some(Instant::now());
                        slf.stream.attach(Box::pin(res.bytes_stream()));

                        return Poll::Ready(Some(Ok(SseEvent::Open)));
                    }
                    Err(err) => {
                        slf.close();
                        return Poll::Ready(Some(slf.go_to_sleep(err.into())));
                    }
                },

                State::Open => match ready!(Pin::new(&mut slf.stream).poll_next(cx)) {
                    Some(Ok(raw_event)) => match raw_event {
                        SseEventCore::Retry(ms) => slf.reconnection_time_ms = ms,
                        SseEventCore::Message(event) => return Poll::Ready(Some(Ok(event.into()))),
                    },
                    Some(Err(SseStreamError::PayloadTooLarge(err))) => {
                        slf.close();
                        return Poll::Ready(Some(Err(Error::PayloadTooLarge(err))));
                    }
                    Some(Err(SseStreamError::Inner(err))) => {
                        return Poll::Ready(Some(slf.go_to_sleep(err.into())));
                    }
                    None => return Poll::Ready(Some(slf.go_to_sleep(SseErrorEvent::Eof))),
                },

                State::Sleeping(sleep_fut) => {
                    ready!(sleep_fut.as_mut().poll(cx));
                    slf.state = State::Disconnected;
                }

                State::Closed => return Poll::Ready(None),
            }
        }
    }
}

mod sealed {
    pub trait Sealed {}
}

/// An extension trait for [`reqwest::RequestBuilder`] to ergonomically create SSE streams.
pub trait RequestBuilderExt: sealed::Sealed {
    /// Converts this request builder into an active [`EventSource`] with default settings.
    fn into_event_source(self) -> EventSource;
    /// Converts this request builder into an [`EventSourceBuilder`] for further configuration.
    fn into_event_source_builder(self) -> EventSourceBuilder;
}

impl sealed::Sealed for RequestBuilder {}
impl RequestBuilderExt for RequestBuilder {
    fn into_event_source(self) -> EventSource {
        EventSource::new(self)
    }
    fn into_event_source_builder(self) -> EventSourceBuilder {
        EventSourceBuilder::new(self)
    }
}
