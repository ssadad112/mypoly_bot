use chrono::{DateTime, Utc};

use super::params::StrategyConfig;
use super::state::{ImbalanceSign, MarketState};

#[derive(Debug, Clone, PartialEq)]
pub struct RiskSnapshot {
    pub elapsed_secs: f64,
    pub seconds_to_start: f64,
    pub q_allow: f64,
    pub q_diff: f64,
    pub abs_q_diff: f64,
    pub should_enter_adjustment: bool,
    pub inventory_balanced: bool,
    pub flip_overshoot_acceptable: bool,
    pub avg_cost_sum_ok: bool,
    pub profit_target_reached: bool,
    pub needs_loss_reduction: bool,
}

pub fn evaluate(state: &MarketState, config: &StrategyConfig, now: DateTime<Utc>) -> RiskSnapshot {
    let elapsed_secs = state.elapsed_secs(now);
    let seconds_to_start = state.seconds_to_start(now);
    let q_allow = config.q_allow();
    let q_diff = state.q_diff;
    let abs_q_diff = q_diff.abs();

    RiskSnapshot {
        elapsed_secs,
        seconds_to_start,
        q_allow,
        q_diff,
        abs_q_diff,
        should_enter_adjustment: should_enter_adjustment(q_diff, config),
        inventory_balanced: is_inventory_balanced(q_diff, config),
        flip_overshoot_acceptable: is_flip_overshoot_acceptable(
            q_diff,
            state.adjustment_started_sign,
            config,
        ),
        avg_cost_sum_ok: is_cost_sum_ok(state.avg_cost_sum, config),
        profit_target_reached: profit_target_reached(state.profit_min, config),
        needs_loss_reduction: needs_loss_reduction(state.profit_min, config),
    }
}

pub fn is_entry_gate_window(
    state: &MarketState,
    config: &StrategyConfig,
    now: DateTime<Utc>,
) -> bool {
    let elapsed_secs = now
        .signed_duration_since(state.start_time)
        .num_milliseconds() as f64
        / 1000.0;
    elapsed_secs >= 0.0 && elapsed_secs <= config.t_pre_entry_window.max(0.0)
}

pub fn entry_price_gate_ok(state: &MarketState, config: &StrategyConfig) -> bool {
    state.entry_gate_price_ok(config)
}

pub fn should_enter_adjustment(q_diff: f64, config: &StrategyConfig) -> bool {
    q_diff.abs() >= config.q_allow()
}

pub fn is_inventory_balanced(q_diff: f64, config: &StrategyConfig) -> bool {
    q_diff.abs() <= config.q_balance_band.max(0.0)
}

pub fn is_flip_overshoot_acceptable(
    q_diff: f64,
    started_sign: Option<ImbalanceSign>,
    config: &StrategyConfig,
) -> bool {
    let Some(started_sign) = started_sign else {
        return false;
    };
    let Some(current_sign) = ImbalanceSign::from_q_diff(q_diff) else {
        return true;
    };

    current_sign != started_sign && q_diff.abs() <= config.q_flip_stop_band.max(0.0)
}

pub fn is_cost_sum_ok(avg_cost_sum: Option<f64>, config: &StrategyConfig) -> bool {
    avg_cost_sum
        .map(|sum| sum <= config.k_cost_sum)
        .unwrap_or(false)
}

pub fn profit_target_reached(profit_min: f64, config: &StrategyConfig) -> bool {
    profit_min >= config.g_target
}

pub fn needs_loss_reduction(profit_min: f64, config: &StrategyConfig) -> bool {
    profit_min < config.g_target
}

pub fn should_cancel_for_timeout(
    sent_at: DateTime<Utc>,
    now: DateTime<Utc>,
    config: &StrategyConfig,
) -> bool {
    now.signed_duration_since(sent_at).num_milliseconds() as f64 / 1000.0 >= config.t_cancel
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn sample_state() -> MarketState {
        let start = Utc.with_ymd_and_hms(2026, 4, 19, 0, 0, 0).unwrap();
        let mut state = MarketState::new(1000.0, start, "up", "down");
        state.q_up = 5.0;
        state.q_down = 0.0;
        state.q_diff = 5.0;
        state.cost_total = 2.4;
        state.profit_if_up = 2.6;
        state.profit_if_down = -2.4;
        state.profit_min = -2.4;
        state
    }

    #[test]
    fn fixed_q_allow_drives_adjustment_entry() {
        let mut config = StrategyConfig::default();
        config.q_allow_abs = 5.0;

        assert!(should_enter_adjustment(5.0, &config));
        assert!(!should_enter_adjustment(4.99, &config));
    }

    #[test]
    fn inventory_balance_uses_balance_band() {
        let mut config = StrategyConfig::default();
        config.q_balance_band = 0.5;

        assert!(is_inventory_balanced(0.49, &config));
        assert!(!is_inventory_balanced(0.51, &config));
    }

    #[test]
    fn flip_overshoot_stops_small_reverse_imbalance() {
        let mut config = StrategyConfig::default();
        config.q_flip_stop_band = 3.0;

        assert!(is_flip_overshoot_acceptable(
            -2.0,
            Some(ImbalanceSign::UpHeavy),
            &config
        ));
        assert!(!is_flip_overshoot_acceptable(
            -4.0,
            Some(ImbalanceSign::UpHeavy),
            &config
        ));
        assert!(!is_flip_overshoot_acceptable(
            2.0,
            Some(ImbalanceSign::UpHeavy),
            &config
        ));
    }

    #[test]
    fn profit_target_and_reduction_use_g_target_only() {
        let mut config = StrategyConfig::default();
        config.g_target = 0.5;

        assert!(profit_target_reached(0.5, &config));
        assert!(!profit_target_reached(0.49, &config));
        assert!(needs_loss_reduction(0.49, &config));
        assert!(!needs_loss_reduction(0.5, &config));
        assert!(!needs_loss_reduction(0.6, &config));
    }

    #[test]
    fn timeout_check_uses_t_cancel() {
        let now = Utc.with_ymd_and_hms(2026, 4, 19, 0, 0, 10).unwrap();
        let sent_at = now - Duration::seconds(10);

        assert!(should_cancel_for_timeout(
            sent_at,
            now,
            &StrategyConfig::default()
        ));
    }

    #[test]
    fn entry_gate_window_uses_elapsed_secs_after_event_start() {
        let start = Utc.with_ymd_and_hms(2026, 4, 19, 0, 0, 0).unwrap();
        let state = MarketState::new(1000.0, start, "up", "down");
        let config = StrategyConfig::default();

        assert!(is_entry_gate_window(
            &state,
            &config,
            start + Duration::seconds(60)
        ));
        assert!(!is_entry_gate_window(
            &state,
            &config,
            start + Duration::seconds(180)
        ));
        assert!(!is_entry_gate_window(
            &state,
            &config,
            start - Duration::seconds(60)
        ));
    }

    #[test]
    fn evaluate_reports_fixed_threshold_snapshot() {
        let state = sample_state();
        let config = StrategyConfig::default();
        let now = state.start_time + Duration::seconds(5);
        let snapshot = evaluate(&state, &config, now);

        assert_eq!(snapshot.q_allow, config.q_allow_abs);
        assert!(snapshot.should_enter_adjustment);
        assert!(snapshot.needs_loss_reduction);
    }
}
