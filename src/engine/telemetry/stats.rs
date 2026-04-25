use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencySummary {
    pub count: usize,
    pub p50_ms: Option<f64>,
    pub p90_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub max_ms: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct LatencyStatsAccumulator {
    values_ms: Vec<f64>,
}

impl LatencyStatsAccumulator {
    pub fn record(&mut self, value_ms: f64) {
        if value_ms.is_finite() {
            self.values_ms.push(value_ms);
        }
    }

    pub fn summary(&self) -> LatencySummary {
        if self.values_ms.is_empty() {
            return LatencySummary::default();
        }

        let mut sorted = self.values_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        LatencySummary {
            count: sorted.len(),
            p50_ms: percentile(&sorted, 0.50),
            p90_ms: percentile(&sorted, 0.90),
            p99_ms: percentile(&sorted, 0.99),
            max_ms: sorted.last().copied(),
        }
    }
}

fn percentile(sorted: &[f64], ratio: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() - 1) as f64 * ratio).round() as usize;
    sorted.get(idx).copied()
}
