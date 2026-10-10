use std::time::Duration;

use thiserror::Error;
use tokio::time::{Instant, sleep_until};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RateLimitError {
    #[error("messages per second must be greater than zero")]
    ZeroRate,
    #[error("messages per second is too high for a non-zero nanosecond interval")]
    IntervalTooShort,
}

pub struct GlobalRateLimiter {
    period: Duration,
    next_permit_at: Instant,
}

impl GlobalRateLimiter {
    pub fn new(messages_per_second: u64) -> Result<Self, RateLimitError> {
        if messages_per_second == 0 {
            return Err(RateLimitError::ZeroRate);
        }

        let period = Duration::from_secs_f64(1.0 / messages_per_second as f64);
        if period.is_zero() {
            return Err(RateLimitError::IntervalTooShort);
        }

        Ok(Self {
            period,
            next_permit_at: Instant::now(),
        })
    }

    pub async fn acquire(&mut self) {
        let now = Instant::now();
        if self.next_permit_at > now {
            sleep_until(self.next_permit_at).await;
        }

        self.next_permit_at = Instant::now() + self.period;
    }
}

#[cfg(test)]
mod tests {
    use super::GlobalRateLimiter;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_spaces_global_permits() {
        let mut limiter = GlobalRateLimiter::new(10).unwrap();
        limiter.acquire().await;

        let second = limiter.acquire();
        tokio::pin!(second);
        assert!(futures::poll!(&mut second).is_pending());

        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(futures::poll!(&mut second).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_does_not_issue_second_permit_early() {
        let mut limiter = GlobalRateLimiter::new(10).unwrap();
        limiter.acquire().await;

        tokio::time::advance(Duration::from_millis(99)).await;
        let second = limiter.acquire();
        tokio::pin!(second);
        assert!(futures::poll!(&mut second).is_pending());

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(futures::poll!(&mut second).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_delays_after_a_missed_tick_instead_of_bursting() {
        let mut limiter = GlobalRateLimiter::new(10).unwrap();
        limiter.acquire().await;

        tokio::time::advance(Duration::from_millis(350)).await;
        limiter.acquire().await;

        let next = limiter.acquire();
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(futures::poll!(&mut next).is_pending());
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(futures::poll!(&mut next).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_does_not_burst_when_late_by_less_than_five_periods() {
        let mut limiter = GlobalRateLimiter::new(1_000).unwrap();

        {
            let first = limiter.acquire();
            tokio::pin!(first);
            assert!(futures::poll!(&mut first).is_ready());
        }

        tokio::time::advance(Duration::from_micros(3_500)).await;
        limiter.acquire().await;

        let next = limiter.acquire();
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
        tokio::time::advance(Duration::from_millis(1)).await;
        next.as_mut().await;
    }

    #[test]
    fn rate_limiter_rejects_zero_rate() {
        assert!(GlobalRateLimiter::new(0).is_err());
    }

    #[test]
    fn rate_limiter_rejects_rates_with_zero_nanosecond_period() {
        assert!(GlobalRateLimiter::new(u64::MAX).is_err());
    }
}
