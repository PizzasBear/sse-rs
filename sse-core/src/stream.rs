use alloc::sync::Arc;
use core::{
    pin::Pin,
    str,
    task::{self, ready, Poll},
};
use thiserror::Error;

use bytes::Buf;
use futures_core::{
    stream::{FusedStream, Stream},
    TryStream,
};
use pin_project_lite::pin_project;

use crate::{PayloadTooLargeError, SseDecoder, SseEvent};

pin_project! {
    /// An asynchronous stream wrapper that parses SSE events from an underlying byte stream.
    #[derive(Debug, Clone, Default)]
    pub struct SseStream<T: TryStream> {
        #[pin]
        inner: Option<T>,
        buf: Option<T::Ok>,

        decoder: SseDecoder,
    }
}

impl<T: TryStream> SseStream<T> {
    /// Creates a new, disconnected [`SseStream`].
    ///
    /// A disconnected stream will immediately yield [`None`] (terminated) if polled.
    /// This constructor is primarily useful when you need to store the [`SseStream`]
    /// inside a struct before the network connection is established.
    ///
    /// To make the stream active, you must attach an inner stream using
    /// [`attach()`](Self::attach).
    ///
    /// # Example
    /// ```
    /// # use futures_core::stream::Stream;
    /// # use sse_core::*;
    /// # async fn fetch_http_stream() -> impl Stream<Item = Result<&'static [u8], ()>> {
    /// #     tokio_test::stream_mock::StreamMockBuilder::new().build()
    /// # }
    /// # tokio_test::block_on(async {
    /// let mut stream = SseStream::disconnected();
    ///
    /// // ... later, when the network is ready:
    /// let byte_stream = fetch_http_stream().await;
    /// stream.attach(byte_stream);
    /// # })
    /// ```
    #[inline]
    #[must_use]
    pub fn disconnected() -> Self {
        Self::with_decoder(SseDecoder::new())
    }

    /// Creates a disconnected stream initialized with a custom decoder.
    ///
    /// See the [`disconnected()`](Self::disconnected) function for more information.
    #[inline]
    #[must_use]
    pub fn with_decoder(decoder: SseDecoder) -> Self {
        Self {
            inner: None,
            buf: None,
            decoder,
        }
    }

    /// Creates a new [`SseStream`] wrapping the provided inner stream.
    #[inline]
    #[must_use]
    pub fn new(inner: T) -> Self {
        let mut slf = Self::disconnected();
        slf.inner = Some(inner);
        slf
    }

    /// Consumes the stream and returns the underlying state-machine decoder.
    #[inline]
    pub fn take_decoder(self) -> SseDecoder {
        let Self { mut decoder, .. } = self;
        decoder.reconnect();
        decoder
    }

    /// Returns `true` if the stream is currently disconnected.
    #[inline]
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_none()
    }

    /// Returns the current `Last-Event-ID` parsed by the underlying decoder.
    #[inline]
    #[must_use]
    pub fn last_event_id(&self) -> Option<&Arc<str>> {
        self.decoder.last_event_id()
    }

    /// Disconnects the inner stream while retaining the underlying parser's state.
    ///
    /// This drops the active network connection but safely preserves the most
    /// recently parsed `Last-Event-ID` within the decoder. This is the standard
    /// method to temporarily pause a stream or handle a dropped connection,
    /// allowing you to later resume exactly where you left off.
    ///
    /// * To close the stream and **inject** a new ID for the next connection, use [`close_with_id()`](Self::close_with_id).
    /// * To close the stream and completely **wipe** the session state, use [`close_and_clear()`](Self::close_and_clear).
    #[inline]
    pub fn close(&mut self) {
        self.decoder.reconnect();
        self.clear_bufs();
    }

    /// Disconnects the stream and completely purges the underlying parser's state.
    ///
    /// This drops the inner stream, clears all internal byte buffers, and
    /// permanently drops the currently tracked `Last-Event-ID`. It effectively
    /// returns the `SseStream` to the exact state it was in when initially
    /// created via [`disconnected()`](Self::disconnected).
    ///
    /// * To close the stream and **keep** the current ID, use [`close()`](Self::close).
    /// * To close the stream and **inject** a new ID, use [`close_with_id()`](Self::close_with_id).
    #[inline]
    pub fn close_and_clear(&mut self) {
        self.decoder.clear();
        self.clear_bufs();
    }

    /// Disconnects the inner stream and explicitly overrides the underlying
    /// decoder's `Last-Event-ID` in preparation for a future connection.
    ///
    /// This is particularly useful in async contexts where you must drop the
    /// active stream, inject a new ID, and then yield back to the runtime before
    /// establishing a new network connection. The injected ID will be available
    /// immediately via [`last_event_id()`](Self::last_event_id).
    ///
    /// * To close the stream and **keep** the current ID, use [`close()`](Self::close).
    /// * To close the stream and completely **wipe** the session state, use [`close_and_clear()`](Self::close_and_clear).
    #[inline]
    pub fn close_with_id(&mut self, id: Option<Arc<str>>) {
        self.decoder.reconnect_with_id(id);
        self.clear_bufs();
    }

    /// Attaches a new inner stream to resume processing events.
    ///
    /// This method resets the underlying parser's buffers but safely retains the most
    /// recently parsed `Last-Event-ID`. It is the standard way to recover from
    /// a dropped network connection, allowing you to resume exactly where you left off.
    ///
    /// * To attach a stream and **inject** a new ID, use [`attach_with_id()`](Self::attach_with_id).
    /// * To attach a stream and completely **wipe** the session state, use [`clear_and_attach()`](Self::clear_and_attach).
    #[inline]
    pub fn attach(&mut self, inner: T) {
        self.close();
        self.inner = Some(inner);
    }

    /// Attaches a new inner stream and completely purges the underlying parser's state.
    ///
    /// This method is used when you want to reuse an existing `SseStream` allocation
    /// for a completely fresh connection or a different server. It clears all internal
    /// byte buffers and permanently drops the currently tracked `Last-Event-ID`.
    ///
    /// * To attach a stream and **keep** the current ID, use [`attach()`](Self::attach).
    /// * To attach a stream and **inject** a new ID, use [`attach_with_id()`](Self::attach_with_id).
    #[inline]
    pub fn clear_and_attach(&mut self, inner: T) {
        self.close_and_clear();
        self.inner = Some(inner);
    }

    /// Attaches a new inner stream to resume processing, explicitly overriding
    /// the `Last-Event-ID` in the underlying decoder.
    ///
    /// This method is primarily used when recovering an offline session where
    /// you need to initialize the stream with a saved ID (e.g., from a local database)
    /// right as you provide the new HTTP response stream.
    ///
    /// * To attach a stream and **keep** the current ID, use [`attach()`](Self::attach).
    /// * To attach a stream and completely **wipe** the session state, use [`clear_and_attach()`](Self::clear_and_attach).
    #[inline]
    pub fn attach_with_id(&mut self, inner: T, id: Option<Arc<str>>) {
        self.close_with_id(id);
        self.inner = Some(inner);
    }

    #[inline]
    fn clear_bufs(&mut self) {
        self.inner = None;
        self.buf = None;
    }
}

/// An alias for [`Result`] with the error set to [`SseStreamError<E>`].
pub type SseStreamResult<T, E> = Result<T, SseStreamError<E>>;

/// Errors that can occur while reading from an [`SseStream`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum SseStreamError<T> {
    /// A single field (e.g., data or Last-Event-ID) exceeded the configured byte limit.
    #[error("{0}")]
    PayloadTooLarge(PayloadTooLargeError),
    /// An error propagated from the inner [`TryStream`].
    #[error("{0}")]
    Inner(#[from] T),
}

impl<T: TryStream> Stream for SseStream<T>
where
    T::Ok: Buf,
{
    type Item = SseStreamResult<SseEvent, T::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut slf = self.project();

        let Some(mut inner) = slf.inner.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };

        loop {
            if let Some(event) = (slf.buf.as_mut())
                .and_then(|buf| slf.decoder.next(buf))
                .transpose()
                .map_err(SseStreamError::PayloadTooLarge)?
            {
                return Poll::Ready(Some(Ok(event)));
            };

            *slf.buf = ready!(inner.as_mut().try_poll_next(cx)?);
            if slf.buf.is_none() {
                slf.inner.set(None);
                return Poll::Ready(None);
            }
        }
    }
}

impl<T: TryStream> FusedStream for SseStream<T>
where
    T::Ok: Buf,
{
    fn is_terminated(&self) -> bool {
        self.is_closed()
    }
}

#[test]
fn hard_parse() -> Result<(), PayloadTooLargeError> {
    use crate::MessageEvent;
    use std::slice;
    use tokio_stream::StreamExt;

    tokio_test::block_on(async {
        // Source: https://github.com/jpopesculian/eventsource-stream/blob/v0.2.3/tests/eventsource-stream.rs
        let bytes = "

:

event: my-event\r
data:line1
data: line2
:
id: my-id
:should be ignored too\rretry:42
retry:

data:second

";

        let mut inner = tokio_test::stream_mock::StreamMockBuilder::new();
        for b in bytes.as_bytes() {
            inner = inner.next(Ok(slice::from_ref(b)));
        }
        inner = inner
            .next(Err(()))
            .next(Ok(b"data: hello\n\ndata:ignored\n"));

        let id = Some("my-id".into());

        let mut stream = SseStream::new(inner.build());
        let events: Vec<_> = (&mut stream).collect().await;

        assert_eq!(
            events,
            &[
                Ok(SseEvent::Retry(42)),
                Ok(SseEvent::Message(MessageEvent {
                    event: "my-event".into(),
                    data: "line1\nline2".into(),
                    last_event_id: id.clone()
                })),
                Ok(SseEvent::Message(MessageEvent {
                    event: "message".into(),
                    data: "second".into(),
                    last_event_id: id.clone()
                })),
                Err(SseStreamError::Inner(())),
                Ok(SseEvent::Message(MessageEvent {
                    event: "message".into(),
                    data: "hello".into(),
                    last_event_id: id.clone()
                })),
            ]
        );

        assert!(stream.is_closed());

        Ok(())
    })
}
