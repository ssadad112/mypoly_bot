use super::inventory::OutcomeSide;
use super::params::StrategyConfig;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TheoreticalQuote {
    pub delta: f64,
    pub up_price: f64,
    pub down_price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DoubleSidedQuote {
    pub stronger_side: OutcomeSide,
    pub theoretical: TheoreticalQuote,
    pub up_price: f64,
    pub down_price: f64,
}

pub fn midpoint(bid: f64, ask: f64) -> Option<f64> {
    if bid <= 0.0 || ask <= 0.0 {
        return None;
    }

    Some((bid + ask) / 2.0)
}

pub fn theoretical_quote(
    up_bid: f64,
    up_ask: f64,
    down_bid: f64,
    down_ask: f64,
    config: &StrategyConfig,
) -> Option<TheoreticalQuote> {
    let mid_up = midpoint(up_bid, up_ask)?;
    let mid_down = midpoint(down_bid, down_ask)?;
    let delta = mid_up - mid_down;
    let up_price = config.legalize_price_to_tick((config.s + delta) / 2.0)?;
    let down_price = config.legalize_price_to_tick((config.s - delta) / 2.0)?;

    Some(TheoreticalQuote {
        delta,
        up_price,
        down_price,
    })
}

pub fn build_double_sided_quote(
    up_bid: f64,
    up_ask: f64,
    down_bid: f64,
    down_ask: f64,
    config: &StrategyConfig,
) -> Option<DoubleSidedQuote> {
    let theoretical = theoretical_quote(up_bid, up_ask, down_bid, down_ask, config)?;
    let stronger_side = if theoretical.delta >= 0.0 {
        OutcomeSide::Up
    } else {
        OutcomeSide::Down
    };

    let up_bid = config.legalize_price_to_tick(up_bid)?;
    let down_bid = config.legalize_price_to_tick(down_bid)?;

    let (up_price, down_price) = match stronger_side {
        OutcomeSide::Up => {
            let up_price = up_bid;
            let down_target = config.legalize_price_to_tick(config.s - up_price)?;
            let down_price = if down_target <= down_bid {
                down_target
            } else {
                down_bid
            };
            (up_price, down_price)
        }
        OutcomeSide::Down => {
            let down_price = down_bid;
            let up_target = config.legalize_price_to_tick(config.s - down_price)?;
            let up_price = if up_target <= up_bid {
                up_target
            } else {
                up_bid
            };
            (up_price, down_price)
        }
    };

    Some(DoubleSidedQuote {
        stronger_side,
        theoretical,
        up_price,
        down_price,
    })
}

pub fn single_sided_adjustment_price(
    side: OutcomeSide,
    up_bid: f64,
    down_bid: f64,
    config: &StrategyConfig,
) -> Option<f64> {
    match side {
        OutcomeSide::Up => config.legalize_price_to_tick(up_bid),
        OutcomeSide::Down => config.legalize_price_to_tick(down_bid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn theoretical_quote_uses_sum_target_and_mid_delta() {
        let config = StrategyConfig::default();
        let quote = theoretical_quote(0.52, 0.54, 0.43, 0.45, &config).unwrap();

        assert!((quote.delta - 0.09).abs() < 1e-9);
        assert!((quote.up_price + quote.down_price - config.s).abs() < 1e-9);
    }

    #[test]
    fn stronger_side_stays_at_front_bid_when_sum_target_is_feasible() {
        let config = StrategyConfig::default();
        let quote = build_double_sided_quote(0.51, 0.53, 0.46, 0.48, &config).unwrap();

        assert_eq!(quote.stronger_side, OutcomeSide::Up);
        assert!((quote.up_price - 0.51).abs() < 1e-9);
        assert!((quote.down_price - 0.46).abs() < 1e-9);
    }

    #[test]
    fn weaker_side_falls_back_to_its_best_bid_when_sum_target_is_not_reachable() {
        let config = StrategyConfig::default();
        let quote = build_double_sided_quote(0.60, 0.62, 0.20, 0.22, &config).unwrap();

        assert_eq!(quote.stronger_side, OutcomeSide::Up);
        assert!((quote.up_price - 0.60).abs() < 1e-9);
        assert!((quote.down_price - 0.20).abs() < 1e-9);
    }

    #[test]
    fn single_sided_adjustment_uses_current_best_bid() {
        let config = StrategyConfig::default();

        assert_eq!(
            single_sided_adjustment_price(OutcomeSide::Up, 0.487, 0.51, &config).unwrap(),
            0.48
        );
        assert_eq!(
            single_sided_adjustment_price(OutcomeSide::Down, 0.49, 0.513, &config).unwrap(),
            0.51
        );
    }
}
