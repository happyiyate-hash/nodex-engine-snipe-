use std::collections::VecDeque;

const MAX_WINDOW_MS: i64 = 300_000;
const MAX_SAMPLES: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TradeSample {
    timestamp_ms: i64,
    side: TradeSide,
    quote_amount_raw: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WindowVolumeMetrics {
    pub buy_volume_raw: u128,
    pub sell_volume_raw: u128,
    pub total_volume_raw: u128,
    pub buy_pressure_pct: Option<f64>,
    pub buy_trade_count: u64,
    pub sell_trade_count: u64,
}

/// Rolling quote-asset volume for one verified pool.
/// The sample queue is ordered by on-chain event timestamp so backfilled events
/// can be inserted without corrupting the 30s / 1m / 5m windows.
#[derive(Debug, Clone, Default)]
pub struct RollingTradeVolume {
    samples: VecDeque<TradeSample>,
}

impl RollingTradeVolume {
    /// Record a trade if it has a non-zero amount and falls within the rolling
    /// five-minute horizon. Timestamps more than five seconds in the future are rejected.
    pub fn record(
        &mut self,
        timestamp_ms: i64,
        side: TradeSide,
        quote_amount_raw: u64,
        now_ms: i64,
    ) -> bool {
        if quote_amount_raw == 0
            || timestamp_ms <= 0
            || timestamp_ms > now_ms.saturating_add(5_000)
            || now_ms.saturating_sub(timestamp_ms) > MAX_WINDOW_MS
        {
            self.prune(now_ms);
            return false;
        }

        self.prune(now_ms);
        let sample = TradeSample {
            timestamp_ms,
            side,
            quote_amount_raw,
        };
        let insert_at = self
            .samples
            .iter()
            .position(|existing| existing.timestamp_ms > timestamp_ms)
            .unwrap_or(self.samples.len());
        self.samples.insert(insert_at, sample);

        while self.samples.len() > MAX_SAMPLES {
            self.samples.pop_front();
        }
        true
    }

    pub fn metrics(&self, now_ms: i64, window_seconds: u64) -> WindowVolumeMetrics {
        let window_ms = i64::try_from(window_seconds)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000);
        let cutoff = now_ms.saturating_sub(window_ms);
        let mut result = WindowVolumeMetrics::default();

        for sample in self
            .samples
            .iter()
            .filter(|sample| sample.timestamp_ms >= cutoff && sample.timestamp_ms <= now_ms)
        {
            match sample.side {
                TradeSide::Buy => {
                    result.buy_volume_raw = result
                        .buy_volume_raw
                        .saturating_add(u128::from(sample.quote_amount_raw));
                    result.buy_trade_count = result.buy_trade_count.saturating_add(1);
                }
                TradeSide::Sell => {
                    result.sell_volume_raw = result
                        .sell_volume_raw
                        .saturating_add(u128::from(sample.quote_amount_raw));
                    result.sell_trade_count = result.sell_trade_count.saturating_add(1);
                }
            }
        }

        result.total_volume_raw = result.buy_volume_raw.saturating_add(result.sell_volume_raw);
        if result.total_volume_raw > 0 {
            result.buy_pressure_pct =
                Some(result.buy_volume_raw as f64 / result.total_volume_raw as f64 * 100.0);
        }
        result
    }

    fn prune(&mut self, now_ms: i64) {
        let cutoff = now_ms.saturating_sub(MAX_WINDOW_MS);
        self.samples.retain(|sample| sample.timestamp_ms >= cutoff);
    }

    #[cfg(test)]
    fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_pressure_is_weighted_by_quote_volume_not_trade_count() {
        let now = 1_000_000;
        let mut volume = RollingTradeVolume::default();
        assert!(volume.record(now - 2_000, TradeSide::Buy, 900, now));
        assert!(volume.record(now - 1_500, TradeSide::Sell, 100, now));
        assert!(volume.record(now - 1_000, TradeSide::Sell, 100, now));
        assert!(volume.record(now - 500, TradeSide::Sell, 100, now));

        let metrics = volume.metrics(now, 30);
        assert_eq!(metrics.buy_trade_count, 1);
        assert_eq!(metrics.sell_trade_count, 3);
        assert_eq!(metrics.buy_volume_raw, 900);
        assert_eq!(metrics.sell_volume_raw, 300);
        assert_eq!(metrics.total_volume_raw, 1_200);
        assert_eq!(metrics.buy_pressure_pct, Some(75.0));
    }

    #[test]
    fn computes_independent_30s_1m_and_5m_windows() {
        let now = 2_000_000;
        let mut volume = RollingTradeVolume::default();
        volume.record(now - 10_000, TradeSide::Buy, 10, now);
        volume.record(now - 45_000, TradeSide::Sell, 20, now);
        volume.record(now - 240_000, TradeSide::Buy, 30, now);

        assert_eq!(volume.metrics(now, 30).total_volume_raw, 10);
        assert_eq!(volume.metrics(now, 60).total_volume_raw, 30);
        assert_eq!(volume.metrics(now, 300).total_volume_raw, 60);
    }

    #[test]
    fn accepts_out_of_order_backfill_and_keeps_samples_sorted() {
        let now = 3_000_000;
        let mut volume = RollingTradeVolume::default();
        volume.record(now - 1_000, TradeSide::Buy, 10, now);
        volume.record(now - 3_000, TradeSide::Sell, 20, now);
        assert_eq!(volume.metrics(now, 30).total_volume_raw, 30);
        assert_eq!(volume.sample_count(), 2);
    }

    #[test]
    fn rejects_zero_stale_and_far_future_samples() {
        let now = 4_000_000;
        let mut volume = RollingTradeVolume::default();
        assert!(!volume.record(now, TradeSide::Buy, 0, now));
        assert!(!volume.record(now - 300_001, TradeSide::Buy, 10, now));
        assert!(!volume.record(now + 5_001, TradeSide::Buy, 10, now));
        assert_eq!(volume.sample_count(), 0);
    }
}
