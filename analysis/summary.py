"""Headline table and the phase 3 gate, from the grid's parquet output.

Headline: each ordering rule at B = 250 ms (B = 0 orders the rules identically), 3 snipers,
speed gap 0.5, weekday and Monday open. Gate (spec section 15): the 95% bootstrap intervals of
the paired differences in LP loss separate every pair of rules in a B > 0 group.

    analysis/.venv/bin/python analysis/summary.py [--block-ms 250]
"""

import argparse
from pathlib import Path

import random

import polars as pl

HERE = Path(__file__).resolve().parent
METRICS = [
    ("lp_loss_bps", "LP loss to snipers, bps"),
    ("sniper_take_bps", "sniper take, bps"),
    ("sniper_take_usd_day", "sniper take, $/day"),
    ("chain_take_bps", "chain take, bps"),
    ("lp_race_win_rate", "LP race win rate"),
    ("fill_delay_median_ms", "fill delay median, ms"),
    ("fill_delay_p99_ms", "fill delay p99, ms"),
    ("regular_cost_bps", "regular-trader cost, bps"),
    ("half_spread_bps", "half-spread, bps"),
    ("monday_take_share", "Monday-open take, share of gap"),
]


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
    pl.Config.set_tbl_width_chars(200)

    s = pl.read_parquet(a.data / "grid_summary.parquet")
    head = (
        s.filter(
            (pl.col("block_ms") == a.block_ms)
            & ((pl.col("speed_gap") - a.gap).abs() < 1e-9)
            & (pl.col("snipers") == a.snipers)
            & pl.col("metric").is_in([m for m, _ in METRICS])
        )
        .with_columns(
            pl.format(
                "{} [{}, {}]",
                pl.col("mean").round(3),
                pl.col("ci_lo").round(3),
                pl.col("ci_hi").round(3),
            ).alias("value")
        )
        .pivot(on="rule", index=["regime", "metric"], values="value")
        .sort(["regime", "metric"])
    )
    print(f"Headline: B = {a.block_ms} ms, gap {a.gap}, {a.snipers} snipers (mean [95% CI])")
    print(head)
    a.out.mkdir(parents=True, exist_ok=True)
    head.write_csv(a.out / f"headline_b{a.block_ms}.csv")

    # Dollar LP loss (not in the summary file): the bps value divides by a volume that itself
    # depends on the rule, so the dollars are the check on the ranking.
    r = pl.read_parquet(a.data / "grid_runs.parquet").filter(
        (pl.col("block_ms") == a.block_ms)
        & ((pl.col("speed_gap") - a.gap).abs() < 1e-9)
        & (pl.col("snipers") == a.snipers)
    )
    print("\nLP loss to snipers in dollars (weekday: the hour; Monday: the first W), mean [95% CI]:")
    for (regime, rule), g in sorted(r.group_by(["regime", "rule"]), key=lambda x: x[0]):
        col = "lp_loss_usd" if regime == "weekday" else "monday_lp_loss_usd"
        xs = g[col].to_list()
        rng = random.Random(7)
        ms = sorted(sum(rng.choice(xs) for _ in xs) / len(xs) for _ in range(2000))
        print(f"  {regime:<12} {rule:<16} ${sum(xs) / len(xs):8.2f} [{ms[50]:.2f}, {ms[1949]:.2f}]")

    p = pl.read_parquet(a.data / "grid_pairs.parquet").filter(
        (pl.col("metric") == "lp_loss_bps") & (pl.col("block_ms") > 0)
    )
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


if __name__ == "__main__":
    main()
