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
use futures_core::stream::{FusedStream, Stream};
use reqwest::{RequestBuilder, Response, StatusCode, header::HeaderValue};
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
    ///
    /// The [`Response`] is attached rather than just its status, so the body —
    /// which often carries the server's own description of the failure — can
    /// still be read.
    #[error("unexpected HTTP status code: {}", .0.status())]
    Status(Box<Response>),
    /// The [`RequestBuilder`] could not be cloned (e.g., it contains a streaming body).
    #[error("request builder could not be cloned (e.g., non-restartable body stream)")]
    UncloneableRequest,
    /// The server's response lacked the `text/event-stream` Content-Type.
    ///
    /// The [`Response`] is attached so the offending Content-Type, and the body
    /// the server actually sent, can still be inspected.
    #[error("invalid response HTTP Content-Type")]
    InvalidContentType(Box<Response>),
    /// The server's response did not contain a Content-Type header.
    ///
    /// The [`Response`] is attached so the rest of the response can still be
    /// inspected.
    #[error("response HTTP Content-Type missing")]
    MissingContentType(Box<Response>),
    /// The client exhausted all retry attempts without successfully reconnecting.
    #[error("couldn't reconnect to SSE server in {0} attempts: {1}")]
    Timeout(u32, SseErrorEvent),
    /// The server sent an event payload that exceeded the configured buffer limit.
    ///
    /// By default the [`EventSource`] never produces this: an oversized event is
    /// reported as the recoverable [`SseEvent::Discarded`] instead, because the
    /// connection survives it. This variant is produced only when the stream was
    /// built with [`fail_on_oversized_event(true)`], for applications that consider
    /// a dropped event fatal:
    ///
    /// ```rust,no_run
    /// # use sse_reqwest_client::RequestBuilderExt;
    /// # let client = reqwest::Client::new();
    /// let stream = client.get("https://example.com/events")
    ///     .into_event_source_builder()
    ///     .fail_on_oversized_event(true)
    ///     .build();
    /// ```
    ///
    /// Prefer that over escalating [`SseEvent::Discarded`] by hand in a stream
    /// combinator. Doing it by hand yields an [`Err`] while the [`EventSource`]
    /// itself stays [`Open`](ReadyState::Open), which contradicts this crate's rule
    /// that every [`Err`] item is terminal and leaves the HTTP response attached;
    /// the builder flag closes the stream first.
    ///
    /// [`fail_on_oversized_event(true)`]: EventSourceBuilder::fail_on_oversized_event
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
    ///
    /// This mirrors the [`error` event] of the JavaScript `EventSource` API, and like it
    /// this is strictly about the *connection*. A message that arrives intact but cannot
    /// be kept is reported as [`Discarded`](Self::Discarded) instead.
    ///
    /// [`error` event]: https://developer.mozilla.org/en-US/docs/Web/API/EventSource/error_event
    Error(SseErrorEvent),
    /// Emitted when an event exceeded [`max_payload_size`] and was thrown away.
    ///
    /// The connection is **still open** and still healthy: the decoder enforced the
    /// limit you configured, dropped the offending event rather than buffering past
    /// that limit, and resynchronized at the next event boundary. Exactly one event
    /// was lost, and the events on either side of it are unaffected. Ignoring this
    /// variant is a valid choice; the stream simply continues.
    ///
    /// This has no counterpart in the JavaScript `EventSource` API, which has no payload
    /// limit to enforce, so it is deliberately kept separate from
    /// [`Error`](Self::Error) rather than widening what that variant means.
    ///
    /// # Choosing a reaction
    ///
    /// If a dropped event is unacceptable for your application, build the stream with
    /// [`fail_on_oversized_event(true)`] and this variant is never emitted: the stream
    /// closes and yields [`Error::PayloadTooLarge`] instead. You can also stop the
    /// stream at any point yourself with [`EventSource::close()`].
    ///
    /// **Do not reconnect in response to this.** An oversized event never advances the
    /// `Last-Event-ID`, because the decoder rolls back the discarded event's `id:` to
    /// the last one it actually delivered. A server that honours `Last-Event-ID` will
    /// therefore replay from *before* the oversized event and send it again, which is
    /// rejected again — [`force_reconnect()`](EventSource::force_reconnect) here can
    /// spin indefinitely. Raise [`max_payload_size`] instead if you need the event.
    ///
    /// [`max_payload_size`]: EventSourceBuilder::max_payload_size
    /// [`fail_on_oversized_event(true)`]: EventSourceBuilder::fail_on_oversized_event
    Discarded(PayloadTooLargeError),
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
            Self::Open | Self::Error(_) | Self::Discarded(_) => None,
        }
    }

    /// Returns a reference to the underlying [`MessageEvent`] if this is a standard message.
    pub fn as_message(&self) -> Option<&MessageEvent> {
        match self {
            Self::Message(msg) => Some(msg),
            Self::Open | Self::Error(_) | Self::Discarded(_) => None,
        }
    }

    /// Returns a mutable reference to the underlying [`MessageEvent`] if this is a standard message.
    pub fn as_message_mut(&mut self) -> Option<&mut MessageEvent> {
        match self {
            Self::Message(msg) => Some(msg),
            Self::Open | Self::Error(_) | Self::Discarded(_) => None,
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

impl From<PayloadTooLargeError> for SseEvent {
    fn from(err: PayloadTooLargeError) -> Self {
        Self::Discarded(err)
    }
}

/// Error indicating that an [`SseEvent`] could not be converted into a [`MessageEvent`].
#[derive(Debug, Error)]
#[error("couldn't convert Event::{} into a MessageEvent", match .0 {
    SseEvent::Open => "Open",
    SseEvent::Message(_) => "Message",
    SseEvent::Error(_) => "Error",
    SseEvent::Discarded(_) => "Discarded"
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
    fail_on_oversized_event: bool,
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
            fail_on_oversized_event: false,
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

    /// Treats an event that exceeds [`max_payload_size`] as a fatal error.
    ///
    /// By default the limit is enforced without ending the subscription: the
    /// oversized event is dropped, the decoder resynchronizes at the next event
    /// boundary, and the loss is reported as [`SseEvent::Discarded`] while the
    /// connection stays open. Exactly one event is lost and the memory bound —
    /// the reason the limit exists — has already held by that point.
    ///
    /// Setting this to `true` instead closes the stream and yields
    /// [`Err(Error::PayloadTooLarge)`](Error::PayloadTooLarge), for applications
    /// where silently continuing past a lost event would be worse than stopping.
    /// [`SseEvent::Discarded`] is then never emitted. As with every other [`Err`]
    /// from this stream the closure is terminal: [`ready_state`] becomes
    /// [`Closed`](ReadyState::Closed) and subsequent polls yield [`None`].
    ///
    /// Note that the trigger is server-controlled while the threshold is yours, so
    /// enabling this lets a single large event from an otherwise healthy server end
    /// a long-lived subscription. That asymmetry is why it is opt-in.
    ///
    /// # Recovering
    ///
    /// Do **not** simply call [`force_reconnect()`]. An oversized event never
    /// advances the `Last-Event-ID` — the decoder rolls it back to the last event
    /// it actually delivered — so a server that honours `Last-Event-ID` replays
    /// from *before* the oversized event and sends it again, failing again.
    ///
    /// Instead, keep the resume point and rebuild with a limit that fits:
    ///
    /// ```rust,no_run
    /// # use std::num::NonZeroUsize;
    /// # use sse_reqwest_client::{EventSource, RequestBuilderExt};
    /// # let client = reqwest::Client::new();
    /// # let req = client.get("https://example.com/events");
    /// # let stream = req.into_event_source();
    /// // ... after the stream ended with `Error::PayloadTooLarge`:
    /// let resume_from = stream.last_event_id().cloned();
    ///
    /// let mut builder = client.get("https://example.com/events")
    ///     .into_event_source_builder()
    ///     .fail_on_oversized_event(true)
    ///     .max_payload_size(NonZeroUsize::new(4 * 1024 * 1024).unwrap());
    ///
    /// if let Some(id) = resume_from {
    ///     builder = builder.last_event_id(id);
    /// }
    ///
    /// let stream = builder.build();
    /// ```
    ///
    /// [`max_payload_size`]: Self::max_payload_size
    /// [`ready_state`]: EventSource::ready_state
    /// [`force_reconnect()`]: EventSource::force_reconnect
    #[inline]
    #[must_use]
    pub fn fail_on_oversized_event(mut self, fail: bool) -> Self {
        self.fail_on_oversized_event = fail;
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
            fail_on_oversized_event: self.fail_on_oversized_event,
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
    /// When the *currently live* connection opened, used to decide whether it
    /// lasted long enough to reset the backoff counter. Must be cleared by every
    /// path that abandons the connection, or a stale timestamp from an older
    /// connection can wrongly reset the backoff of a later one.
    connected_since: Option<Instant>,
    retry_config: SseRetryConfig,
    retry_transient_errors: bool,
    fail_on_oversized_event: bool,
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
            .field("fail_on_oversized_event", &self.fail_on_oversized_event)
            .field("connected_since", &self.connected_since)
            .field("state", &self.state)
            .field("stream", &self.stream)
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
        self.connected_since = None;
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
        self.connected_since = None;
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
        self.connected_since = None;
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
            self.stream.close();
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
                            let err = Error::Status(Box::new(res));
                            return Poll::Ready(Some(Err(err)));
                        }

                        let Some(content_type) = res
                            .headers()
                            .get(reqwest::header::CONTENT_TYPE)
                            .map(|v| v.as_bytes())
                        else {
                            slf.close();
                            let err = Error::MissingContentType(Box::new(res));
                            return Poll::Ready(Some(Err(err)));
                        };

                        if !is_event_stream(content_type) {
                            slf.close();
                            let err = Error::InvalidContentType(Box::new(res));
                            return Poll::Ready(Some(Err(err)));
                        }

                        slf.state = State::Open;
                        slf.connected_since = Some(Instant::now());
                        slf.stream.attach(Box::pin(res.bytes_stream()));

                        return Poll::Ready(Some(Ok(SseEvent::Open)));
                    }
                    Err(err) => return Poll::Ready(Some(slf.go_to_sleep(err.into()))),
                },

                State::Open => match ready!(Pin::new(&mut slf.stream).poll_next(cx)) {
                    Some(Ok(raw_event)) => match raw_event {
                        SseEventCore::Retry(ms) => slf.reconnection_time_ms = ms,
                        SseEventCore::Message(event) => return Poll::Ready(Some(Ok(event.into()))),
                    },
                    // The limit has already done its job: the decoder refused to grow
                    // its buffer, dropped the offending event and resynchronized, so
                    // the connection is still healthy and memory is still bounded.
                    // Tearing the subscription down by default would add no protection
                    // and would hand any server a way to end a long-lived stream with a
                    // single large event — hence the opt-in.
                    Some(Err(SseStreamError::PayloadTooLarge(err))) => {
                        if slf.fail_on_oversized_event {
                            // Every `Err` from this stream is terminal, so close before
                            // yielding one: that drops the response body and keeps
                            // `ready_state`/`is_terminated` consistent with the item.
                            slf.close();
                            return Poll::Ready(Some(Err(Error::PayloadTooLarge(err))));
                        }
                        return Poll::Ready(Some(Ok(SseEvent::Discarded(err))));
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

/// Returns whether a `Content-Type` header value names the `text/event-stream`
/// media type.
///
/// Media types are case-insensitive (RFC 9110 §8.3.1), so `Text/Event-Stream` is
/// just as valid as the lowercase spelling. Whatever follows the type must begin a
/// parameter (`;charset=utf-8`) or be optional whitespace, so that near-misses like
/// `text/event-streamx` are still rejected.
fn is_event_stream(content_type: &[u8]) -> bool {
    const MIME_EVENT_STREAM: &str = "text/event-stream";

    let Some((essence, rest)) = content_type.split_at_checked(MIME_EVENT_STREAM.len()) else {
        return false;
    };

    essence.eq_ignore_ascii_case(MIME_EVENT_STREAM.as_bytes())
        && matches!(rest.first(), None | Some(b';' | b' ' | b'\t'))
}

#[test]
fn test_is_event_stream() {
    for ct in [
        "text/event-stream",
        "Text/Event-Stream",
        "TEXT/EVENT-STREAM",
        "text/event-stream;charset=utf-8",
        "Text/Event-Stream; charset=utf-8",
        "text/event-stream\tx",
        "text/event-stream ",
    ] {
        assert!(is_event_stream(ct.as_bytes()), "rejected {ct:?}");
    }

    for ct in [
        "",
        "text/plain",
        "text/event",
        "text/event-strea",
        "text/event-streamx",
        "application/json",
        " text/event-stream",
    ] {
        assert!(!is_event_stream(ct.as_bytes()), "accepted {ct:?}");
    }
}

/// An [`EventSource`] is terminated exactly when it is [`ReadyState::Closed`], which
/// is the only state in which [`poll_next`](Stream::poll_next) yields [`None`].
///
/// Note that termination is not permanent: like [`SseStream`], a closed
/// [`EventSource`] can be revived with
/// [`force_reconnect()`](EventSource::force_reconnect).
impl FusedStream for EventSource {
    #[inline]
    fn is_terminated(&self) -> bool {
        matches!(self.state, State::Closed)
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    use super::*;

    /// Serves `response` once over a raw socket, then closes. The returned handle
    /// must be joined so the test doesn't outlive its own server thread.
    fn serve(response: String) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let _ = sock.read(&mut [0u8; 4096]);
            let _ = sock.write_all(response.as_bytes());
            let _ = sock.flush();
            std::thread::sleep(Duration::from_millis(50));
        });
        (format!("http://{addr}/"), handle)
    }

    /// The response used by both oversized-event tests: a deliverable event, one
    /// that blows past the limit, then another deliverable one.
    fn serve_oversized_event() -> (String, std::thread::JoinHandle<()>) {
        serve(format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/event-stream\r\n\
             Connection: close\r\n\r\n\
             id: 1\ndata: before\n\n\
             id: 2\ndata: {}\n\n\
             id: 3\ndata: after\n\n",
            "x".repeat(64),
        ))
    }

    /// An oversized event must not take the connection down with it: the events
    /// around it still arrive, in order, on the very same connection.
    #[tokio::test]
    async fn oversized_event_is_discarded_without_dropping_the_connection() {
        use futures_util::StreamExt;

        let (url, server) = serve_oversized_event();

        let mut es = reqwest::Client::new()
            .get(&url)
            .into_event_source_builder()
            .max_payload_size(NonZeroUsize::new(16).unwrap())
            .build();

        let mut seen = vec![];
        let mut id_at_discard = None;
        while let Some(event) = es.next().await {
            match event.unwrap() {
                SseEvent::Open => seen.push("open".to_owned()),
                SseEvent::Message(msg) => seen.push(format!("msg:{}", msg.data)),
                SseEvent::Discarded(_) => {
                    seen.push("discarded".to_owned());
                    id_at_discard = es.last_event_id().map(|id| id.to_string());
                }
                // Reached once the server hangs up; stop before it reconnects.
                SseEvent::Error(_) => break,
            }
        }

        assert_eq!(seen, ["open", "msg:before", "discarded", "msg:after"]);

        // The discarded event's own `id: 2` is rolled back rather than committed, so
        // at that moment the resume point is still the last *delivered* event. This
        // is precisely why `SseEvent::Discarded` documents that reconnecting can spin:
        // a replaying server would rewind to 1 and resend the oversized event forever.
        assert_eq!(id_at_discard.as_deref(), Some("1"));

        // Parsing resynchronizes, so the following event commits its ID normally.
        assert_eq!(es.last_event_id().map(|id| &**id), Some("3"));

        // Backing off, not terminated: only `close()` (or an `Err`) ends the stream.
        assert!(!es.is_terminated());
        es.close();
        assert!(es.is_terminated());

        server.join().unwrap();
    }

    /// With `fail_on_oversized_event`, the same stream stops at the oversized event
    /// instead of resynchronizing past it — and stops *properly*, closing itself so
    /// the terminal `Err` doesn't leave an `Open` stream behind it.
    #[tokio::test]
    async fn oversized_event_is_fatal_when_opted_in() {
        use futures_util::StreamExt;

        let (url, server) = serve_oversized_event();

        let mut es = reqwest::Client::new()
            .get(&url)
            .into_event_source_builder()
            .max_payload_size(NonZeroUsize::new(16).unwrap())
            .fail_on_oversized_event(true)
            .build();

        let mut seen = vec![];
        let err = loop {
            match es.next().await.expect("stream ended without an error") {
                Ok(SseEvent::Open) => seen.push("open".to_owned()),
                Ok(SseEvent::Message(msg)) => seen.push(format!("msg:{}", msg.data)),
                Ok(SseEvent::Discarded(_)) => panic!("Discarded is not emitted when opted in"),
                Ok(SseEvent::Error(err)) => panic!("unexpected connection error: {err}"),
                Err(err) => break err,
            }
        };

        // `id: 3` is never reached: the stream stops at the oversized event rather
        // than resynchronizing to the one after it.
        assert_eq!(seen, ["open", "msg:before"]);
        assert!(matches!(err, Error::PayloadTooLarge(_)), "got {err:?}");

        // The `Err` is genuinely terminal — this is what a caller-side `map` over
        // `SseEvent::Discarded` cannot do, since it can't close the `EventSource`.
        assert!(es.is_terminated());
        assert_eq!(es.ready_state(), ReadyState::Closed);
        assert!(es.next().await.is_none());

        // The resume point is still the last *delivered* event, so reconnecting
        // as-is would replay the oversized event forever. Recovery means rebuilding
        // with a larger limit from this ID, per `fail_on_oversized_event`'s docs.
        assert_eq!(es.last_event_id().map(|id| &**id), Some("1"));

        server.join().unwrap();
    }
}
