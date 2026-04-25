#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub t_event: f64,
    pub t_cut: f64,
    pub t_cancel: f64,
    pub t_cancel_confirm: f64,
    pub s: f64,
    pub q_share_min: f64,
    pub m_order_min: f64,
    pub delta_q: f64,
    pub delta_p: f64,
    /// Fixed absolute inventory tilt that starts single-sided adjustment.
    pub q_allow_abs: f64,
    /// Entry gate observation window after event start, in seconds.
    pub t_pre_entry_window: f64,
    /// UP-side entry reference price lower bound.
    pub p_up_gate_min: f64,
    /// UP-side entry reference price upper bound.
    pub p_up_gate_max: f64,
    /// DOWN-side entry reference price lower bound.
    pub p_down_gate_min: f64,
    /// DOWN-side entry reference price upper bound.
    pub p_down_gate_max: f64,
    /// Maximum acceptable sum of realized average UP and DOWN costs.
    pub k_cost_sum: f64,
    /// Inventory difference considered sufficiently balanced after adjustment.
    pub q_balance_band: f64,
    /// Acceptable reverse imbalance caused by minimum order overshoot.
    pub q_flip_stop_band: f64,
    /// Remaining share size that is small enough to treat as dust during reconcile.
    pub dust_share_threshold: f64,
    /// Remaining notional that is small enough to treat as dust during reconcile.
    pub dust_notional_threshold: f64,
    pub g_target: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            t_event: 300.0,
            t_cut: 300.0,
            t_cancel:2.0,
            t_cancel_confirm: 2.0,
            s: 0.97,
            q_share_min: 5.0,
            m_order_min: 1.0,
            delta_q: 0.01,
            delta_p: 0.01,
            q_allow_abs: 4.5,
            t_pre_entry_window: 80.0,
            p_up_gate_min: 0.20,
            p_up_gate_max: 0.30,
            p_down_gate_min: 0.2,
            p_down_gate_max: 0.30,
            k_cost_sum: 1.0,
            q_balance_band: 1.0,
            q_flip_stop_band: 1.0,
            dust_share_threshold: 0.01,
            dust_notional_threshold: 0.01,
            g_target: 0.2,
        }
    }
}

impl StrategyConfig {
    pub fn legalize_price_to_tick(&self, price: f64) -> Option<f64> {
        if price <= 0.0 || self.delta_p <= 0.0 {
            return None;
        }

        let clipped = price.clamp(self.delta_p, 1.0 - self.delta_p);
        let ticks = ((clipped / self.delta_p) + 1e-9).floor();
        Some((ticks * self.delta_p * 100.0).round() / 100.0)
    }

    pub fn legalize_share_to_step(&self, size: f64) -> Option<f64> {
        if size <= 0.0 || self.delta_q <= 0.0 {
            return None;
        }

        let steps = ((size / self.delta_q) - 1e-9).ceil();
        Some(((steps * self.delta_q) * 100.0).round() / 100.0)
    }

    pub fn q_min_at_price(&self, price: f64) -> Option<f64> {
        if price <= 0.0 {
            return None;
        }

        let base = self.q_share_min.max(self.m_order_min / price);
        self.legalize_share_to_step(base)
    }

    pub fn q_allow(&self) -> f64 {
        self.q_allow_abs.max(0.0)
    }

    pub fn bind_price_tolerance(&self) -> f64 {
        (self.delta_p / 2.0).max(1e-9)
    }

    pub fn bind_size_tolerance(&self) -> f64 {
        (self.delta_q / 2.0).max(1e-9)
    }

    pub fn price_in_gate_range(&self, price: f64, min: f64, max: f64) -> bool {
        price > min && price < max
    }

    pub fn up_gate_price_ok(&self, price: f64) -> bool {
        self.price_in_gate_range(price, self.p_up_gate_min, self.p_up_gate_max)
    }

    pub fn down_gate_price_ok(&self, price: f64) -> bool {
        self.price_in_gate_range(price, self.p_down_gate_min, self.p_down_gate_max)
    }

    pub fn is_dust_remaining(&self, remaining_size: f64, price: f64) -> bool {
        if remaining_size <= 0.0 {
            return true;
        }

        remaining_size <= self.dust_share_threshold.max(0.0)
            || (remaining_size * price.max(0.0)) <= self.dust_notional_threshold.max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::StrategyConfig;

    #[test]
    fn price_is_floored_to_cent_grid() {
        let config = StrategyConfig::default();

        assert_eq!(config.legalize_price_to_tick(0.487).unwrap(), 0.48);
        assert_eq!(config.legalize_price_to_tick(0.999).unwrap(), 0.99);
    }

    #[test]
    fn share_is_ceiled_to_step_grid() {
        let config = StrategyConfig::default();

        assert_eq!(config.legalize_share_to_step(5.001).unwrap(), 5.01);
        assert_eq!(config.legalize_share_to_step(5.0).unwrap(), 5.0);
    }

    #[test]
    fn q_min_respects_share_and_notional_floor() {
        let config = StrategyConfig::default();
        let q_min = config.q_min_at_price(0.19).unwrap();

        assert!((q_min - 5.27).abs() < 1e-9);
    }

    #[test]
    fn q_allow_is_fixed_absolute_threshold() {
        let config = StrategyConfig::default();

        assert_eq!(config.q_allow(), config.q_allow_abs);
        assert_eq!(config.q_allow(), 4.5);
    }

    #[test]
    fn entry_gate_uses_open_price_interval() {
        let config = StrategyConfig::default();

        assert!(config.up_gate_price_ok(0.25));
        assert!(!config.up_gate_price_ok(0.20));
        assert!(!config.up_gate_price_ok(0.30));
        assert!(config.down_gate_price_ok(0.29));
    }

    #[test]
    fn dust_remaining_uses_share_and_notional_thresholds() {
        let config = StrategyConfig::default();

        assert!(config.is_dust_remaining(0.002, 0.79));
        assert!(!config.is_dust_remaining(0.02, 0.79));
    }
}
