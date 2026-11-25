"""The PGA bidding sweep: who captures the stale-quote prize for each sniper bid α and LP bid β.

Reads a grid run made with several --alpha-bps / --beta-bps levels, for example

    target/release/grid --seeds 200 --out analysis/data/pga_sweep --blocks-ms 250 --gaps 0.5 \
        --snipers 3 --alpha-bps 250,500,750,1000,1500,2500,5000 --beta-bps 0,2500,5000,10000

and writes analysis/out/pga_sweep_<tag>.csv: per regime, sniper count and (α, β), the LP's loss
to snipers and its race cost, the LP race win rate, and how the LP's loss splits: what the
snipers keep (net of fees), what the chain gets from the snipers' bids, and the protocol's taker
fees. The LP's own bids, which also go to the chain, are a separate column. Money is in dollars
over the span (the weekday hour; the first 5 min after the Monday reopen) and, for the
headline quantities, in bps of the span's volume.

    analysis/.venv/bin/python analysis/pga_sweep.py [--data analysis/data/pga_sweep] [--tag b250]
"""

import argparse
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, default=HERE / "data" / "pga_sweep")
    ap.add_argument("--out", type=Path, default=HERE / "out")
    ap.add_argument("--tag", default="b250")
    a = ap.parse_args()
    pl.Config.set_tbl_rows(200)
    pl.Config.set_tbl_cols(30)
    pl.Config.set_tbl_width_chars(250)

    s = pl.read_parquet(a.data / "grid_summary.parquet")
    keys = ["regime", "block_ms", "speed_gap", "snipers", "rule", "alpha_level_bps",
            "alpha_bps", "beta_bps"]
    wide = s.pivot(on="metric", index=keys, values="mean")
    lo = s.pivot(on="metric", index=keys, values="ci_lo")
    hi = s.pivot(on="metric", index=keys, values="ci_hi")
    ci = lo.select(keys + [pl.col("sniper_take_span_bps").alias("st_lo")]).join(
        hi.select(keys + [pl.col("sniper_take_span_bps").alias("st_hi")]), on=keys
    )
    t = (
        wide.join(ci, on=keys)
        .with_columns(
            alpha=pl.col("alpha_bps") / 1e4,
            # The snipers' combined bid: N x each sniper's alpha.
            alpha_combined=pl.col("alpha_bps") * pl.col("snipers") / 1e4,
            beta=pl.col("beta_bps") / 1e4,
            # LP loss = sniper take + sniper bids + protocol taker fees (snipers only trade
            # against LP quotes), so the chain's part from snipers is the chain take less the
            # LP's own bids.
            chain_from_snipers_usd=pl.col("chain_take_span_usd") - pl.col("chain_take_lp_span_usd"),
        )
        .with_columns(
            protocol_usd=pl.col("lp_loss_span_usd") - pl.col("sniper_take_span_usd")
            - pl.col("chain_from_snipers_usd"),
            snipers_keep=pl.col("sniper_take_span_usd") / pl.col("lp_loss_span_usd"),
            chain_gets=pl.col("chain_from_snipers_usd") / pl.col("lp_loss_span_usd"),
        )
        .select(
            "regime", "snipers", "rule", "alpha", "alpha_combined", "beta",
            pl.col("lp_loss_bps").round(3).alias("lp_loss_bps"),
            pl.col("lp_loss_span_usd").round(2).alias("lp_loss_usd"),
            pl.col("lp_cost_span_usd").round(2).alias("lp_cost_usd"),
            pl.col("lp_race_win_rate").round(3).alias("lp_wins"),
            pl.col("sniper_take_span_bps").round(3).alias("sniper_bps"),
            pl.col("st_lo").round(3).alias("sniper_bps_lo"),
            pl.col("st_hi").round(3).alias("sniper_bps_hi"),
            pl.col("sniper_take_span_usd").round(2).alias("sniper_usd"),
            pl.col("chain_take_span_bps").round(3).alias("chain_bps"),
            pl.col("chain_from_snipers_usd").round(2).alias("chain_from_snipers_usd"),
            pl.col("chain_take_lp_span_usd").round(2).alias("chain_from_lp_usd"),
            pl.col("protocol_usd").round(2),
            pl.col("snipers_keep").round(3),
            pl.col("chain_gets").round(3),
            pl.col("monday_take_share").round(3).alias("gap_share"),
            pl.col("monday_take_share_net").round(3).alias("gap_share_net"),
            pl.col("monday_chain_share").round(3).alias("gap_chain"),
            pl.col("monday_lp_kept_share").round(3).alias("gap_lp_kept"),
        )
        .sort(["regime", "snipers", "rule", "beta", "alpha"], descending=[True, False, False,
                                                                         False, False])
    )
    print(t)
    a.out.mkdir(parents=True, exist_ok=True)
    t.write_csv(a.out / f"pga_sweep_{a.tag}.csv")

    # The largest α with sniper take ≥ 0 (its CI's upper end above 0) per regime, N and β.
    print("\nLargest alpha with mean sniper take >= 0, per regime, snipers and beta:")
    ok = t.filter((pl.col("rule") == "pga") & (pl.col("sniper_bps") >= 0))
    print(ok.group_by(["regime", "snipers", "beta"]).agg(pl.col("alpha").max())
          .sort(["regime", "snipers", "beta"]))
    print(f"wrote {a.out / f'pga_sweep_{a.tag}.csv'}")


if __name__ == "__main__":
    main()
