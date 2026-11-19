"""Headline table and the phase 3 gate, from the grid's parquet output.

Headline: each ordering rule at B = 250 ms (B = 0 orders the rules identically), 3 snipers,
speed gap 0.5, weekday and Monday open. Every money metric is reported twice, side by side: in
bps of the span's volume and in dollars over the span. The span is the main chart's: the whole
hour on the weekday, the first W = 5 min after the reopen on the Monday open.

The main chart (main_chart.py) plots the bps column of "LP loss to snipers". The two units can
rank the rules differently, because the bps denominator (volume) itself depends on the rule:
see the "rank" columns and docs/DECISIONS.md, entries 59 and 82.

Gate (spec section 15): the 95% bootstrap intervals of the paired differences in LP loss
separate every pair of rules in a B > 0 group.

    analysis/.venv/bin/python analysis/summary.py [--block-ms 250]
"""

import argparse
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
RULES = ["fcfs", "pga", "cancel_priority"]

# (quantity, bps metric, dollar metric). Dollars are over the span, not per day.
MONEY = [
    ("LP loss to snipers (main chart: bps)", "lp_loss_bps", "lp_loss_span_usd"),
    ("LP race cost (loss + its own bids)", "lp_cost_span_bps", "lp_cost_span_usd"),
    ("sniper take (net of fees)", "sniper_take_span_bps", "sniper_take_span_usd"),
    ("chain take (all priority fees)", "chain_take_span_bps", "chain_take_span_usd"),
]
# Unitless or single-unit metrics.
OTHER = [
    ("lp_race_win_rate", "LP race win rate (whole run)"),
    ("monday_take_share", "Monday: snipers' share of the gap prize, gross"),
    ("monday_take_share_net", "Monday: snipers' share, net of their fees"),
    ("monday_chain_share", "Monday: chain's share (sniper and LP bids)"),
    ("monday_lp_kept_share", "Monday: LP's share (cancelled in time)"),
    ("monday_prize_usd", "Monday: gap prize at the reopen, $"),
    ("chain_take_lp_span_usd", "LP's own priority bids over the span, $"),
    ("fill_delay_median_ms", "fill delay median, ms"),
    ("fill_delay_p99_ms", "fill delay p99, ms"),
    ("regular_cost_bps", "regular-trader cost, bps"),
    ("half_spread_bps", "half-spread, bps"),
]


def fmt(mean: float, lo: float, hi: float, nd: int) -> str:
    if mean != mean:  # NaN
        return "n/a"
    return f"{mean:.{nd}f} [{lo:.{nd}f}, {hi:.{nd}f}]"


def rank(vals: dict[str, float]) -> str:
    """Rules from lowest to highest mean, e.g. 'cancel_priority < fcfs < pga'."""
    return " < ".join(r for r, _ in sorted(vals.items(), key=lambda kv: kv[1]))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--block-ms", type=int, default=250)
    ap.add_argument("--gap", type=float, default=0.5)
    ap.add_argument("--snipers", type=int, default=3)
    ap.add_argument("--data", type=Path, default=HERE / "data")
    ap.add_argument("--out", type=Path, default=HERE / "out")
    a = ap.parse_args()
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_width_chars(250)
    pl.Config.set_fmt_str_lengths(60)

    s = pl.read_parquet(a.data / "grid_summary.parquet").filter(
        (pl.col("block_ms") == a.block_ms)
        & ((pl.col("speed_gap") - a.gap).abs() < 1e-9)
        & (pl.col("snipers") == a.snipers)
    )
    pga = s.filter(pl.col("rule") == "pga").select(["alpha_level_bps", "alpha_bps", "beta_bps"])
    pga = pga.unique()
    if pga.height != 1:
        raise SystemExit(f"expected one PGA bidding setting in the grid, found {pga.height}")
    alpha_level, alpha, beta = pga.row(0)
    pga_note = (
        f"PGA bids: each sniper alpha = {alpha / 1e4:g} of the prize it sees "
        f"(combined level {alpha_level / 1e4:g} split over {a.snipers} snipers), "
        f"LP beta = {beta / 1e4:g} of its exposure"
    )
    get = {
        (r["regime"], r["rule"], r["metric"]): (r["mean"], r["ci_lo"], r["ci_hi"])
        for r in s.iter_rows(named=True)
    }

    rows = []
    for regime in ["weekday", "monday_open"]:
        for label, mb, mu in MONEY:
            row = {"regime": regime, "quantity": label}
            for unit, m, nd in [("bps", mb, 3), ("usd", mu, 2)]:
                for rule in RULES:
                    row[f"{rule}_{unit}"] = fmt(*get[(regime, rule, m)], nd)
            for unit, m in [("bps", mb), ("usd", mu)]:
                row[f"rank_{unit}"] = rank({r: get[(regime, r, m)][0] for r in RULES})
            rows.append(row)
    money = pl.DataFrame(rows)
    span = "weekday: the hour; Monday open: the first 5 min after the reopen"
    print(f"Headline: B = {a.block_ms} ms, gap {a.gap}, {a.snipers} snipers, mean [95% CI]")
    print(pga_note)
    print(f"Money metrics in bps of the span's volume and in $ over the span ({span}).")
    print("The main chart plots LP loss to snipers in bps.\n")
    for regime in ["weekday", "monday_open"]:
        print(money.filter(pl.col("regime") == regime).drop("regime"))

    rows = []
    for regime in ["weekday", "monday_open"]:
        for m, label in OTHER:
            if regime == "weekday" and m.startswith("monday_"):
                continue
            row = {"regime": regime, "quantity": label}
            for rule in RULES:
                row[rule] = fmt(*get[(regime, rule, m)], 3)
            rows.append(row)
    other = pl.DataFrame(rows)
    print(other)

    a.out.mkdir(parents=True, exist_ok=True)
    money.with_columns(pl.lit(pga_note).alias("pga_bids")).write_csv(
        a.out / f"headline_b{a.block_ms}.csv"
    )
    other.write_csv(a.out / f"headline_b{a.block_ms}_other.csv")

    p = pl.read_parquet(a.data / "grid_pairs.parquet")
    head_pairs = p.filter(
        (pl.col("block_ms") == a.block_ms)
        & ((pl.col("speed_gap") - a.gap).abs() < 1e-9)
        & (pl.col("snipers") == a.snipers)
        & pl.col("metric").is_in(["lp_loss_bps", "lp_loss_span_usd"])
    ).sort(["regime", "metric", "rule_a", "rule_b"])
    print("\nPaired differences in LP loss at the headline cells (a - b), bps and $:")
    print(head_pairs.select(["regime", "metric", "rule_a", "rule_b", "mean_diff", "ci_lo",
                             "ci_hi", "separated"]))

    p = p.filter((pl.col("metric") == "lp_loss_bps") & (pl.col("block_ms") > 0))
    groups = p.group_by(["regime", "block_ms", "speed_gap", "snipers"]).agg(
        pl.col("separated").all().alias("ranked"), pl.col("separated").sum().alias("pairs_sep")
    )
    n_ranked = groups["ranked"].sum()
    print(
        f"\nGate: {p['separated'].sum()}/{p.height} rule pairs separated on LP loss; "
        f"{n_ranked}/{groups.height} B > 0 groups fully ranked."
    )
    print("Groups not fully ranked, with their unseparated pairs:")
    bad = p.filter(~pl.col("separated")).sort(["regime", "block_ms", "speed_gap", "snipers"])
    print(bad.select(["regime", "block_ms", "speed_gap", "snipers", "rule_a", "rule_b",
                      "mean_diff", "ci_lo", "ci_hi", "n"]))
    main_cells = groups.filter(
        ((pl.col("speed_gap") - a.gap).abs() < 1e-9) & (pl.col("snipers") == a.snipers)
    ).sort(["regime", "block_ms"])
    print("\nMain-chart cells:")
    print(main_cells)

    # Monday share of the gap: range check over every cell (docs/DECISIONS.md, entry 80).
    allc = pl.read_parquet(a.data / "grid_summary.parquet").filter(
        (pl.col("regime") == "monday_open") & (pl.col("metric") == "monday_take_share")
    )
    print(
        f"\nMonday share of the gap over all {allc.height} cells: mean from "
        f"{allc['mean'].min():.3f} to {allc['mean'].max():.3f}; seeds with a prize per cell "
        f"from {allc['n'].min()} to {allc['n'].max()} of 200."
    )


if __name__ == "__main__":
    main()
