use alloc::{borrow::Cow, string::String, sync::Arc, vec, vec::Vec};
use core::{
    fmt, iter,
    num::{NonZeroU8, NonZeroUsize},
    str,
};
use thiserror::Error;

use bytes::Buf;
use memchr::{memchr, memchr2};

/// Represents a single Server-Sent Event message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct MessageEvent {
    /// The event name (defaults to `"message"`).
    pub event: Cow<'static, str>,
    /// The payload data.
    pub data: String,
    /// The `Last-Event-ID` sent by the server, if any.
    pub last_event_id: Option<Arc<str>>,
}

/// Commands and payloads yielded by the SSE stream.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum SseEvent {
    /// A standard data message.
    Message(MessageEvent),
    /// A server request to change the client's reconnect time (in milliseconds).
    Retry(u32),
}

/// Error indicating that a parsed field exceeded the maximum allowed buffer size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash, Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("payload exceeded the allotted buffer size limit")]
pub struct PayloadTooLargeError;

const MAX_DEBUG_SIZE: usize = 200;

struct ShowBigStr<'a>(&'a str);

impl fmt::Debug for ShowBigStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut end = self.0.len().min(MAX_DEBUG_SIZE);
        while !self.0.is_char_boundary(end) {
            end -= 1;
        }
        let s = &self.0[..end];

        fmt::Debug::fmt(s, f)?;
        if end < self.0.len() {
            write!(f, "... ({} bytes total)", self.0.len())?;
        }

        Ok(())
    }
}

struct ShowBigBuf<'a>(&'a [u8]);

impl fmt::Debug for ShowBigBuf<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (buf, truncated) = match self.0.len() {
            ..=MAX_DEBUG_SIZE => (self.0, false),
            _ => (&self.0[..MAX_DEBUG_SIZE], true),
        };

        let mut chunks = buf.utf8_chunks().peekable();

        f.write_str("\"")?;
        while let Some(chunk) = chunks.next() {
            fmt::Display::fmt(&chunk.valid().escape_debug(), f)?;

            let invalid = chunk.invalid();
            if invalid.is_empty() {
                continue;
            }

            // If we truncated and this is the very last chunk, the invalid bytes
            // are almost certainly just a sliced multi-byte UTF-8 character.
            if truncated && chunks.peek().is_none() {
                break;
            }

            for &byte in invalid {
                write!(f, "\\x{byte:02X}")?;
            }
        }

        if truncated {
            write!(f, "\"... ({} bytes total)", self.0.len())?;
        } else {
            f.write_str("\"")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum ValueMode {
    Data,
    Event,
    Retry,
    Id,
}

impl ValueMode {
    pub const fn field_name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Event => "event",
            Self::Retry => "retry",
            Self::Id => "id",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Bom { bytes_read: u8 },
    Field(Option<(ValueMode, NonZeroU8)>),
    Value(ValueMode),
    Ignore,
    PostCr,
    PostColon(ValueMode),
}

/// The core state-machine parser for SSE.
///
/// This decoder does not perform any I/O. It consumes bytes from a given buffer
/// and yields parsed [`SseEvent`]s. It is suitable for `no_std` environments.
#[derive(Clone)]
pub struct SseDecoder {
    mode: Mode,
    last_event_id: Option<Arc<str>>,
    staged_last_event_id: Option<Arc<str>>,
    last_event_id_buf: Vec<u8>,
    event_buf: Vec<u8>,
    data_buf: Vec<u8>,
    retry_buf: Option<u32>,
    max_payload_size: NonZeroUsize,
    corrupted: bool,
}

impl SseDecoder {
    /// Creates a new decoder with the default payload size limit of 512KiB.
    ///
    /// # Example
    /// ```rust
    /// # use bytes::{Buf, Bytes};
    /// # use sse_core::{SseDecoder, SseEvent};
    /// # fn main() -> Result<(), sse_core::PayloadTooLargeError> {
    /// let mut decoder = SseDecoder::new();
    /// let mut buf = Bytes::from("data: standard stream\n\n");
    ///
    /// let event = decoder.next(&mut buf).transpose()?;
    /// assert!(event.is_some());
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::with_limit(NonZeroUsize::new(512 * 1024).unwrap())
    }

    /// Creates a new decoder with a custom maximum payload size limit.
    ///
    /// This is useful in memory-constrained environments or when connecting to
    /// untrusted servers to prevent memory exhaustion from unbounded input.
    ///
    /// The limit applies independently to each of the three values an event
    /// accumulates — its data, its name and its ID — so peak memory is a small
    /// multiple of `max_payload_size` rather than exactly that. See
    /// [`next()`](Self::next) for what each of them accumulates.
    ///
    /// # Example
    /// ```rust
    /// # use core::num::NonZeroUsize;
    /// # use bytes::Bytes;
    /// # use sse_core::{SseDecoder, SseEvent};
    /// # fn main() -> Result<(), sse_core::PayloadTooLargeError> {
    /// // Create a strict decoder that rejects payloads over 1024 bytes
    /// let limit = NonZeroUsize::new(1024).unwrap();
    /// let mut decoder = SseDecoder::with_limit(limit);
    ///
    /// let mut buf = Bytes::from("data: small payload\n\n");
    /// let Some(event) = decoder.next(&mut buf) else {
    ///     panic!();
    /// };
    /// assert!(event.is_ok());
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn with_limit(max_payload_size: NonZeroUsize) -> Self {
        Self {
            mode: Mode::Bom { bytes_read: 0 },
            last_event_id: None,
            staged_last_event_id: None,
            last_event_id_buf: vec![],
            event_buf: vec![],
            data_buf: vec![],
            retry_buf: None,
            max_payload_size,
            corrupted: false,
        }
    }

    /// Returns the current `Last-Event-ID` known to the decoder, if any.
    #[inline]
    #[must_use]
    pub fn last_event_id(&self) -> Option<&Arc<str>> {
        self.last_event_id.as_ref()
    }

    /// Resets the decoder state for a new connection, explicitly overriding
    /// the currently tracked `Last-Event-ID`.
    ///
    /// This method clears all internal byte buffers and resets the parser, but
    /// instead of keeping the previous ID (like [`reconnect()`](Self::reconnect))
    /// or dropping it (like [`clear()`](Self::clear)), it injects the provided ID.
    ///
    /// It is typically used to prime the state machine with a known ID
    /// (e.g., from a local database) right before feeding the decoder bytes
    /// from a newly established connection.
    #[inline]
    pub fn reconnect_with_id(&mut self, id: Option<Arc<str>>) {
        self.last_event_id = id;
        self.reconnect();
    }

    /// Resets the decoder state completely, dropping the current `Last-Event-ID`.
    ///
    /// This clears all internal byte buffers and purges the parser's state,
    /// effectively starting fresh. Because it drops the `Last-Event-ID`, the
    /// next connection will start from the present moment rather than resuming.
    ///
    /// * To reset the state but **keep** the current ID, use [`reconnect()`](Self::reconnect).
    /// * To reset the state and **inject** a specific ID, use [`reconnect_with_id()`](Self::reconnect_with_id).
    #[inline]
    pub fn clear(&mut self) {
        self.reconnect_with_id(None);
    }

    /// Resets the buffer state for a new connection while retaining the `Last-Event-ID`.
    ///
    /// This clears the internal byte buffers to prepare for a fresh stream of data,
    /// but safely preserves the most recently parsed `Last-Event-ID`. This ensures
    /// that when you reconnect to the server, you can resume exactly where you left off.
    ///
    /// * To reset the state and **drop** the ID, use [`clear()`](Self::clear).
    /// * To reset the state and **override** the ID, use [`reconnect_with_id()`](Self::reconnect_with_id).
    #[inline]
    pub fn reconnect(&mut self) {
        self.mode = Mode::Bom { bytes_read: 0 };
        self.clear_bufs();
        self.corrupted = false;
    }

    fn mark_corrupted(&mut self) {
        self.clear_bufs();
        self.corrupted = true
    }

    /// Appends the newline separating two `data` lines, subject to the same limit
    /// as the line contents. `consume_until_newline` never sees these bytes, so
    /// without this a stream of empty `data` lines would grow `data_buf` unbounded.
    ///
    /// A *trailing* separator is allowed to sit one byte past the limit, since it is
    /// popped at dispatch rather than delivered; anything appended after it is
    /// checked against the full length and so still errors.
    fn push_data_newline(&mut self) -> Result<(), PayloadTooLargeError> {
        if self.max_payload_size.get() < self.data_buf.len() {
            self.mark_corrupted();
            return Err(PayloadTooLargeError);
        }
        self.data_buf.push(b'\n');
        Ok(())
    }

    #[inline]
    fn clear_bufs(&mut self) {
        self.data_buf.clear();
        self.event_buf.clear();
        self.last_event_id_buf.clear();
        self.staged_last_event_id = self.last_event_id.clone();
    }

    fn dispatch(&mut self, cr: bool) -> Option<SseEvent> {
        self.mode = match cr {
            true => Mode::PostCr,
            false => Mode::Field(None),
        };

        if self.corrupted {
            self.corrupted = false;
            // bufs should be clear already
            return None;
        }

        self.last_event_id = self.staged_last_event_id.clone();

        match self.data_buf.last() {
            Some(b'\n') => {
                self.data_buf.pop();
            }
            Some(_) => {}
            None => {
                self.event_buf.clear();
                return None;
            }
        }

        let data = String::from_utf8_lossy(&self.data_buf).into_owned();
        self.data_buf.clear();

        let event = match &*self.event_buf {
            b"" => Cow::Borrowed("message"),
            event_buf => Cow::Owned(String::from_utf8_lossy(event_buf).into_owned()),
        };
        self.event_buf.clear();

        Some(SseEvent::Message(MessageEvent {
            data,
            event,
            last_event_id: self.last_event_id.clone(),
        }))
    }

    /// Consumes bytes from the provided buffer and attempts to yield an event.
    ///
    /// The decoder does not store unparsed bytes internally. It reads directly
    /// from the provided buffer, advancing the buffer's cursor only for the bytes
    /// it successfully parses.
    ///
    /// If `Ok(None)` is returned, the provided buffer has been exhausted and
    /// more bytes are needed to complete the current event. You should fetch more
    /// data, append it to your buffer, and call `next()` again.
    ///
    /// # Example
    /// ```
    /// use bytes::{Buf, Bytes};
    /// # use sse_core::{SseDecoder, SseEvent};
    ///
    /// let mut decoder = SseDecoder::new();
    /// let mut buffer = Bytes::from("data: hello\n\n");
    ///
    /// // Call next() in a loop to drain all available events
    /// while let Some(event) = decoder.next(&mut buffer) {
    ///     println!("Received: {event:?}");
    /// }
    ///
    /// // When next() returns None, the decoder is waiting for more data.
    /// assert!(buffer.is_empty());
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`PayloadTooLargeError`] when an event's accumulated data, name
    /// or ID would exceed the maximum payload size configured for this decoder.
    /// The limit is *not* per line:
    ///
    /// * `data` accumulates every `data:` line of the current event, joined by
    ///   newlines, so the limit bounds the whole event payload and is only reset
    ///   once the event is dispatched.
    /// * `event` and `id` are reset by each `event:` / `id:` field, so for those
    ///   the limit does bound a single line.
    ///
    /// The offending event is discarded rather than silently truncated: the
    /// decoder skips the rest of the current event and resynchronizes at the next
    /// event boundary, so it stays usable after the error. `retry:` fields are
    /// still honoured while the discarded event is being skipped.
    pub fn next(&mut self, buf: &mut impl Buf) -> Option<Result<SseEvent, PayloadTooLargeError>> {
        // # 9.2.5 Parsing an event stream
        //
        // stream        = [ bom ] *event
        // event         = *( comment / field ) end-of-line
        // comment       = colon *any-char end-of-line
        // field         = 1*name-char [ colon [ space ] *any-char ] end-of-line
        // end-of-line   = ( cr lf / cr / lf )
        //
        // ; characters
        // lf            = %x000A ; U+000A LINE FEED (LF)
        // cr            = %x000D ; U+000D CARRIAGE RETURN (CR)
        // space         = %x0020 ; U+0020 SPACE
        // colon         = %x003A ; U+003A COLON (:)
        // bom           = %xFEFF ; U+FEFF BYTE ORDER MARK
        // name-char     = %x0000-0009 / %x000B-000C / %x000E-0039 / %x003B-10FFFF
        //                 ; a scalar value other than U+000A LINE FEED (LF), U+000D CARRIAGE RETURN (CR), or U+003A COLON (:)
        // any-char      = %x0000-0009 / %x000B-000C / %x000E-10FFFF
        //                 ; a scalar value other than U+000A LINE FEED (LF) or U+000D CARRIAGE RETURN (CR)

        loop {
            let chunk = buf.chunk();
            if chunk.is_empty() {
                return None;
            }

            match &mut self.mode {
                Mode::Bom { bytes_read } => {
                    let b0 = chunk[0];

                    const BOM: &[u8; 3] = b"\xef\xbb\xbf";

                    if b0 != BOM[*bytes_read as usize] {
                        self.mode = match *bytes_read {
                            0 => Mode::Field(None),
                            _ => Mode::Ignore,
                        };
                        continue;
                    }

                    buf.advance(1);
                    *bytes_read += 1;

                    if BOM.len() <= *bytes_read as usize {
                        self.mode = Mode::Field(None);
                    }
                }
                Mode::Field(None) => {
                    let b0 = chunk[0];
                    buf.advance(1);
                    let mode = match b0 {
                        b'd' if !self.corrupted => ValueMode::Data,
                        b'e' if !self.corrupted => ValueMode::Event,
                        b'i' if !self.corrupted => ValueMode::Id,
                        b'r' => ValueMode::Retry,

                        b'\n' | b'\r' => match self.dispatch(b0 == b'\r') {
                            Some(ev) => return Some(Ok(ev)),
                            None => continue,
                        },

                        _ => {
                            self.mode = Mode::Ignore;
                            continue;
                        }
                    };
                    self.mode = Mode::Field(Some((mode, NonZeroU8::new(1).unwrap())));
                }
                &mut Mode::Field(Some((mode, ref mut len))) => {
                    let cmp = &mode.field_name().as_bytes()[len.get() as usize..];
                    if iter::zip(chunk, cmp).any(|(ch0, ch1)| ch0 != ch1) {
                        self.mode = Mode::Ignore;
                        continue;
                    }
                    let Some(&b_post) = chunk.get(cmp.len()) else {
                        *len = NonZeroU8::new(len.get() + chunk.len() as u8).unwrap();
                        buf.advance(chunk.len());
                        continue;
                    };
                    buf.advance(cmp.len() + 1);

                    match b_post {
                        b'\n' => self.mode = Mode::Field(None),
                        b'\r' => self.mode = Mode::PostCr,
                        b':' => {
                            match mode {
                                ValueMode::Data => {}
                                ValueMode::Event => self.event_buf.clear(),
                                ValueMode::Id => self.last_event_id_buf.clear(),
                                ValueMode::Retry => self.retry_buf = None,
                            }

                            self.mode = Mode::PostColon(mode);
                            continue;
                        }
                        _ => {
                            self.mode = Mode::Ignore;
                            continue;
                        }
                    }

                    // A field name with no colon carries the empty string as its
                    // value, so the target buffer must be reset, not left alone.
                    // `retry` is the exception: it accumulates nothing, and the
                    // `retry:` form already clears `retry_buf` before parsing into it.
                    match mode {
                        ValueMode::Data => {
                            if let Err(err) = self.push_data_newline() {
                                return Some(Err(err));
                            }
                        }
                        ValueMode::Event => self.event_buf.clear(),
                        ValueMode::Id => {
                            self.last_event_id_buf.clear();
                            self.staged_last_event_id = None;
                        }
                        ValueMode::Retry => {}
                    }
                }
                Mode::Value(ValueMode::Retry) => {
                    let mut advanced = 0;
                    let mut return_event = false;

                    for &b in chunk {
                        advanced += 1;
                        match b {
                            b'0'..=b'9' => {
                                let digit = (b & 0xf) as _;

                                let retry_buf = self.retry_buf.unwrap_or(0);
                                let Some(retry_buf) = retry_buf.checked_mul(10) else {
                                    self.mode = Mode::Ignore;
                                    break;
                                };
                                let Some(retry_buf) = retry_buf.checked_add(digit) else {
                                    self.mode = Mode::Ignore;
                                    break;
                                };
                                self.retry_buf = Some(retry_buf);
                            }
                            b'\r' => {
                                self.mode = Mode::PostCr;
                                return_event = true;
                                break;
                            }
                            b'\n' => {
                                self.mode = Mode::Field(None);
                                return_event = true;
                                break;
                            }
                            _ => {
                                self.mode = Mode::Ignore;
                                break;
                            }
                        }
                    }

                    buf.advance(advanced);

                    if let (true, Some(retry_buf)) = (return_event, self.retry_buf) {
                        return Some(Ok(SseEvent::Retry(retry_buf)));
                    }
                }
                Mode::Value(ValueMode::Data) => {
                    match consume_until_newline(
                        &mut self.mode,
                        Some(&mut self.data_buf),
                        self.max_payload_size,
                        buf,
                    ) {
                        Ok(true) => {
                            if let Err(err) = self.push_data_newline() {
                                return Some(Err(err));
                            }
                        }
                        Ok(false) => {}
                        Err(err) => {
                            self.mark_corrupted();
                            return Some(Err(err));
                        }
                    }
                }
                Mode::Value(ValueMode::Event) => {
                    if let Err(err) = consume_until_newline(
                        &mut self.mode,
                        Some(&mut self.event_buf),
                        self.max_payload_size,
                        buf,
                    ) {
                        self.mark_corrupted();
                        return Some(Err(err));
                    }
                }
                Mode::Value(ValueMode::Id) => {
                    match consume_until_newline(
                        &mut self.mode,
                        Some(&mut self.last_event_id_buf),
                        self.max_payload_size,
                        buf,
                    ) {
                        Ok(true) => {
                            if memchr(0, &self.last_event_id_buf).is_none() {
                                self.staged_last_event_id = match &*self.last_event_id_buf {
                                    [] => None,
                                    buf => Some(String::from_utf8_lossy(buf).into()),
                                };
                            }
                            self.last_event_id_buf.clear();
                        }
                        Ok(false) => {}
                        Err(err) => {
                            self.mark_corrupted();
                            return Some(Err(err));
                        }
                    }
                }
                Mode::Ignore => {
                    consume_until_newline(&mut self.mode, None, self.max_payload_size, buf)
                        .expect("there should be no payload to grow too large");
                }
                Mode::PostCr => {
                    if chunk[0] == b'\n' {
                        buf.advance(1);
                    }
                    self.mode = Mode::Field(None);
                }
                Mode::PostColon(value) => {
                    if chunk[0] == b' ' {
                        buf.advance(1);
                    }
                    self.mode = Mode::Value(*value);
                }
            }
        }
    }
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SseDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseDecoder")
            .field("mode", &self.mode)
            .field(
                "last_event_id",
                &self.last_event_id.as_deref().map(ShowBigStr),
            )
            .field(
                "staged_last_event_id",
                &self.staged_last_event_id.as_deref().map(ShowBigStr),
            )
            .field("last_event_id_buf", &ShowBigBuf(&self.last_event_id_buf))
            .field("event_buf", &ShowBigBuf(&self.event_buf))
            .field("data_buf", &ShowBigBuf(&self.data_buf))
            .field("retry_buf", &self.retry_buf)
            .field("max_payload_size", &self.max_payload_size)
            .finish()
    }
}

fn consume_until_newline(
    mode: &mut Mode,
    mut out: Option<&mut Vec<u8>>,
    max_size: NonZeroUsize,
    buf: &mut impl Buf,
) -> Result<bool, PayloadTooLargeError> {
    loop {
        let chunk = buf.chunk();
        if chunk.is_empty() {
            return Ok(false);
        };

        let Some(i) = memchr2(b'\r', b'\n', chunk) else {
            if let Some(out) = out.as_deref_mut() {
                if max_size.get() < out.len() + chunk.len() {
                    out.clear();
                    *mode = Mode::Ignore;
                    return Err(PayloadTooLargeError);
                }
                out.extend_from_slice(chunk);
            }
            buf.advance(chunk.len());
            continue;
        };

        if let Some(out) = out.as_deref_mut() {
            if max_size.get() < out.len() + i {
                out.clear();
                *mode = Mode::Ignore;
                return Err(PayloadTooLargeError);
            }
            out.extend_from_slice(&chunk[..i]);
        }

        *mode = match chunk[i] {
            b'\r' => Mode::PostCr,
            b'\n' => Mode::Field(None),
            _ => unreachable!(),
        };

        buf.advance(i + 1);

        return Ok(true);
    }
}

#[test]
fn hard_parse() -> Result<(), PayloadTooLargeError> {
    use core::slice;

    // Based on https://github.com/jpopesculian/eventsource-stream/blob/v0.2.3/tests/eventsource-stream.rs
    let bytes = "\u{FEFF}data: x

:

event: my-event\r
data:line1
data: line2
data
:
id: my-id
:should be ignored too\rretry:42
retry:

data:second

data:ignored
";

    let mut decoder = SseDecoder::new();

    let events = bytes
        .bytes()
        .filter_map(|b| decoder.next(&mut slice::from_ref(&b)))
        .collect::<Result<Vec<_>, PayloadTooLargeError>>()?;

    let id = Some("my-id".into());

    assert_eq!(
        events,
        &[
            SseEvent::Message(MessageEvent {
                event: "message".into(),
                data: "x".into(),
                last_event_id: None
            }),
            SseEvent::Retry(42),
            SseEvent::Message(MessageEvent {
                event: "my-event".into(),
                data: "line1\nline2\n".into(),
                last_event_id: id.clone()
            }),
            SseEvent::Message(MessageEvent {
                event: "message".into(),
                data: "second".into(),
                last_event_id: id.clone()
            })
        ]
    );
    Ok(())
}

/// Per the spec, a line with no colon is a field whose value is the empty string,
/// so a bare `event` / `id` must *reset* its buffer rather than leave the previous
/// value in place. Regression test: `id` leaking across events also leaks into the
/// `Last-Event-ID` sent on reconnect.
#[test]
fn test_valueless_fields() {
    fn messages(bytes: &str) -> Vec<(String, String, Option<String>)> {
        let mut decoder = SseDecoder::new();
        let mut buf = bytes.as_bytes();
        let mut out = vec![];
        while let Some(SseEvent::Message(msg)) = decoder.next(&mut buf).transpose().unwrap() {
            out.push((
                msg.event.into_owned(),
                msg.data,
                msg.last_event_id.map(|id| id.to_string()),
            ));
        }
        out
    }

    // A bare `event` resets the event type back to the "message" default.
    assert_eq!(
        messages("event: custom\nevent\ndata: x\n\n"),
        [("message".into(), "x".into(), None)],
    );

    // A bare `id` clears the ID, exactly like the `id:` form does.
    for bytes in [
        "id: abc\ndata: 1\n\nid\ndata: 2\n\n",
        "id: abc\ndata: 1\n\nid:\ndata: 2\n\n",
    ] {
        assert_eq!(
            messages(bytes),
            [
                ("message".into(), "1".into(), Some("abc".into())),
                ("message".into(), "2".into(), None),
            ],
            "for {bytes:?}",
        );
    }

    // A bare `data` still appends an empty line rather than resetting.
    assert_eq!(
        messages("data: a\ndata\ndata: b\n\n"),
        [("message".into(), "a\n\nb".into(), None)],
    );
}

/// Valueless `data` lines never reach `consume_until_newline`, so the separator each
/// one appends has to be counted on its own. Regression test: without that check a
/// server could grow `data_buf` a byte at a time, with no bound and no error, and
/// have the oversized event delivered anyway.
#[test]
fn test_valueless_data_respects_the_limit() {
    let limit = NonZeroUsize::new(10).unwrap();

    // A payload made only of separators is still bounded by the limit.
    let input = "data\n".repeat(100) + "\n";
    let mut buf = input.as_bytes();
    let mut decoder = SseDecoder::with_limit(limit);
    assert_eq!(decoder.next(&mut buf), Some(Err(PayloadTooLargeError)));

    // The rest of the event is skipped as a unit, leaving the decoder usable.
    assert_eq!(decoder.next(&mut buf), None);
    assert!(buf.is_empty());

    let mut buf: &[u8] = b"data: after\n\n";
    assert_eq!(
        decoder.next(&mut buf),
        Some(Ok(SseEvent::Message(MessageEvent {
            event: "message".into(),
            data: "after".into(),
            last_event_id: None,
        }))),
    );

    // A payload that exactly fills the limit is still legal, whatever it is made of.
    let input = "data\n".repeat(limit.get() + 1) + "\n";
    let mut buf = input.as_bytes();
    let mut decoder = SseDecoder::with_limit(limit);
    assert_eq!(
        decoder.next(&mut buf),
        Some(Ok(SseEvent::Message(MessageEvent {
            event: "message".into(),
            data: "\n".repeat(limit.get()),
            last_event_id: None,
        }))),
    );
}

#[test]
fn test_reconnect() {
    let mut stream1: &[u8] = b"
id: my-id

event: my-event
data:line1
:
data: line2
id: ignored1
";

    let mut stream2: &[u8] = b"

data: data

id: final

id: ignored2
";

    let my_id = Some("my-id".into());

    let mut decoder = SseDecoder::new();

    assert_eq!(decoder.next(&mut stream1), None);
    assert_eq!(decoder.last_event_id(), my_id.as_ref());
    assert!(stream1.is_empty());

    decoder.reconnect();

    // Check that the buffer was cleared
    assert_eq!(
        decoder.next(&mut stream2),
        Some(Ok(SseEvent::Message(MessageEvent {
            event: "message".into(),
            data: "data".into(),
            last_event_id: my_id,
        }))),
    );
    assert_eq!(decoder.next(&mut stream2), None);
    assert_eq!(decoder.last_event_id().map(|id| &**id), Some("final"));
    assert!(stream2.is_empty());
}

#[test]
fn test_limits() {
    let mut stream: &[u8] = b"
data: 0123456789
id: my-id

id: 01234567890
data: thing
event: ev

data: mid

event: jojo
id: ignored
data: 01234
data: 56789
retry: 10

event: final
data

";

    let my_id = Some("my-id".into());

    let mut decoder = SseDecoder::with_limit(NonZeroUsize::new(10).unwrap());

    assert_eq!(
        decoder.next(&mut stream),
        Some(Ok(SseEvent::Message(MessageEvent {
            event: "message".into(),
            data: "0123456789".into(),
            last_event_id: my_id.clone(),
        })))
    );
    assert_eq!(decoder.next(&mut stream), Some(Err(PayloadTooLargeError)));
    assert_eq!(
        decoder.next(&mut stream),
        Some(Ok(SseEvent::Message(MessageEvent {
            event: "message".into(),
            data: "mid".into(),
            last_event_id: my_id.clone()
        })))
    );
    assert_eq!(decoder.next(&mut stream), Some(Err(PayloadTooLargeError)));
    assert_eq!(decoder.next(&mut stream), Some(Ok(SseEvent::Retry(10))));
    assert_eq!(
        decoder.next(&mut stream),
        Some(Ok(SseEvent::Message(MessageEvent {
            event: "final".into(),
            data: "".into(),
            last_event_id: my_id.clone()
        })))
    );
    assert!(stream.is_empty());
}
