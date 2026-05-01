use core::time::Duration;

/// Configuration for exponential backoff and jitter during stream reconnections.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct SseRetryConfig {
    /// The maximum number of consecutive connection attempts before giving up.
    pub max_retries: u32,
    /// The absolute maximum wait time between connection attempts in milliseconds.
    pub max_backoff_ms: u32,
    /// The absolute minimum wait time between connection attempts in milliseconds.
    pub min_sleep_ms: u32,
    /// The multiplier applied to the delay after each failed attempt.
    pub backoff_multiplier: f32,
    /// Whether to apply randomness (jitter) to the reconnect delay.
    pub jitter: bool,
}

impl SseRetryConfig {
    /// Creates a new retry configuration with sensible defaults.
    ///
    /// The default configuration applies an exponential backoff multiplier of `2.0`, caps the
    /// maximum delay at `60,000` milliseconds (1 minute), caps the number of retries to 20, and
    /// enables jitter to prevent thundering herd scenarios.
    ///
    /// # Example
    /// ```rust
    /// use sse_core::SseRetryConfig;
    ///
    /// let config = SseRetryConfig::new();
    /// assert_eq!(config.max_backoff_ms, 60_000);
    /// assert!(config.jitter);
    /// ```
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_retries: 20,
            max_backoff_ms: 60_000,
            min_sleep_ms: 200,
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }

    /// Creates a retry configuration that disables all automatic retries.
    #[inline]
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            max_retries: 0,
            ..Self::new()
        }
    }

    /// Calculates the delay duration for the next reconnection attempt.
    ///
    /// Returns [`None`] if the `attempt` count exceeds [`Self::max_retries`].
    #[must_use]
    pub fn calculate_backoff_with_factor(
        &self,
        reconnect_time_ms: u32,
        attempt: u32,
        jitter_factor: f32,
    ) -> Option<Duration> {
        if self.max_retries <= attempt {
            return None;
        }

        let reconnect_time_ms = reconnect_time_ms.max(self.min_sleep_ms) as f32;
        let mut sleep_ms =
            match self.backoff_multiplier.is_finite() && 1.0 <= self.backoff_multiplier {
                true => reconnect_time_ms * self.backoff_multiplier.powi(attempt as _),
                false => reconnect_time_ms,
            };

        if !sleep_ms.is_finite() || (self.max_backoff_ms as f32) <= sleep_ms {
            sleep_ms = self.max_backoff_ms as _;
        }

        if self.jitter && reconnect_time_ms < sleep_ms {
            let jitter_factor =
                match jitter_factor.is_finite() && (0.0..=1.0).contains(&jitter_factor) {
                    true => jitter_factor,
                    false => 1.0,
                };
            sleep_ms = reconnect_time_ms + jitter_factor * (sleep_ms - reconnect_time_ms)
        }

        Some(Duration::from_millis(sleep_ms as _))
    }

    /// Calculates the delay duration for the next reconnection attempt.
    ///
    /// Returns [`None`] if the `attempt` count exceeds [`Self::max_retries`].
    #[must_use]
    #[cfg(feature = "fastrand")]
    pub fn calculate_backoff(&self, reconnect_time_ms: u32, attempt: u32) -> Option<Duration> {
        self.calculate_backoff_with_factor(reconnect_time_ms, attempt, fastrand::f32())
    }
}

impl Default for SseRetryConfig {
    fn default() -> Self {
        Self::new()
    }
}
