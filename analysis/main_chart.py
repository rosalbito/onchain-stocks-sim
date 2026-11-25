"""The talk's main chart: LP loss to snipers (bps of volume) by ordering rule across block
length B, for the weekday hour and the Monday open, with 95% bootstrap intervals.

The unit is bps of the volume traded over the same span, not dollars. The two can rank FCFS and
PGA differently on the Monday open (docs/DECISIONS.md, entries 59 and 82); summary.py and
analysis/out/headline_b250.csv give both side by side.

Reads analysis/data/grid_summary.parquet (written by `cargo run --release -p grid`).
Writes analysis/out/main_chart.png and .svg.

    analysis/.venv/bin/python analysis/main_chart.py [--gap 0.5] [--snipers 3]
"""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import font_manager  # noqa: E402
import polars as pl  # noqa: E402

HERE = Path(__file__).resolve().parent

# rosalbito.dev identity
BG = "#050507"
INK = "#edebe6"
INK_MUTED = "#8d8b86"
HAIRLINE = "#2a2a32"
ACCENT = "#7b61ff"

# One highlighted series (cancel priority) in the accent; the others in muted greys, told
# apart by marker and line style and labelled directly at the line end.
SERIES = {
    "fcfs": dict(label="First come, first served", color="#a8a6a1", marker="o", ls="-"),
    "pga": dict(label="Priority gas auction", color="#75736f", marker="s", ls="--"),
    "cancel_priority": dict(label="Cancel priority", color=ACCENT, marker="D", ls="-"),
}
PANELS = [
    ("weekday", "Weekday hour", "bps of the hour's volume"),
    ("monday_open", "Monday open, first 5 min", "bps of the window's volume"),
]


def pick_font() -> str:
    names = {f.name for f in font_manager.fontManager.ttflist}
    for name in ["Geist", "Inter", "Helvetica Neue", "Avenir Next", "Helvetica", "DejaVu Sans"]:
        if name in names:
            return name
    return "sans-serif"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gap", type=float, default=0.5, help="speed gap (sniper ÷ LP latency)")
    ap.add_argument("--snipers", type=int, default=3)
    ap.add_argument("--data", type=Path, default=HERE / "data" / "grid_summary.parquet")
    ap.add_argument("--out", type=Path, default=HERE / "out")
    args = ap.parse_args()

    s = pl.read_parquet(args.data).filter(
        (pl.col("metric") == "lp_loss_bps")
        & ((pl.col("speed_gap") - args.gap).abs() < 1e-9)
        & (pl.col("snipers") == args.snipers)
    )
    if s.is_empty():
        raise SystemExit("no rows for that speed gap and sniper count")
    seeds = int(s["n"].min())
    bid = s.filter(pl.col("rule") == "pga").select(["alpha_bps", "beta_bps"]).unique()
    if bid.height != 1:
        raise SystemExit("expected one PGA bidding setting in the grid summary")
    alpha, beta = (x / 1e4 for x in bid.row(0))
    blocks = sorted(s["block_ms"].unique().to_list())
    xs = list(range(len(blocks)))

    font = pick_font()
    plt.rcParams.update(
        {
            "font.family": font,
            "font.size": 11,
            "text.color": INK,
            "axes.labelcolor": INK_MUTED,
            "xtick.color": INK_MUTED,
            "ytick.color": INK_MUTED,
            "axes.edgecolor": HAIRLINE,
            "svg.fonttype": "none",
        }
    )
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.6), facecolor=BG)
    fig.subplots_adjust(left=0.07, right=0.84, top=0.72, bottom=0.14, wspace=0.42)

    for ax, (regime, title, unit) in zip(axes, PANELS):
        ax.set_facecolor(BG)
        for side in ["top", "right", "left"]:
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(HAIRLINE)
        ax.grid(axis="y", color=HAIRLINE, linewidth=0.8)
        ax.set_axisbelow(True)
        ax.tick_params(length=0, pad=6)
        d = s.filter(pl.col("regime") == regime)
        ymax = float(d["ci_hi"].max())
        for rule, st in SERIES.items():
            r = d.filter(pl.col("rule") == rule).sort("block_ms")
            mean, lo, hi = r["mean"].to_list(), r["ci_lo"].to_list(), r["ci_hi"].to_list()
            hl = rule == "cancel_priority"
            ax.fill_between(xs, lo, hi, color=st["color"], alpha=0.22 if hl else 0.16, lw=0)
            ax.plot(
                xs, mean, color=st["color"], ls=st["ls"], lw=2.4 if hl else 2,
                marker=st["marker"], ms=8, mec=BG, mew=2, zorder=3 if hl else 2,
            )
            ax.annotate(
                st["label"], (xs[-1], mean[-1]), xytext=(12, 0), textcoords="offset points",
                va="center", fontsize=10, color=INK if hl else INK_MUTED, annotation_clip=False,
            )
        ax.set_xticks(xs, [f"{b} ms" if b < 1000 else f"{b // 1000} s" for b in blocks])
        ax.set_xlabel("Block length B", labelpad=8)
        ax.set_ylim(0, ymax * 1.12)
        ax.set_xlim(-0.25, xs[-1] + 0.25)
        ax.set_title(title, loc="left", fontsize=13, color=INK, pad=22, fontweight="light")
        ax.text(0, 1.03, unit.upper(), transform=ax.transAxes, fontsize=8, color=INK_MUTED,
                family="monospace")

    fig.text(0.07, 0.955, "LP losses to snipers, by sequencer ordering rule", fontsize=17,
             color=INK, fontweight="light")
    fig.text(
        0.07, 0.865,
        f"Mean over {seeds} seeds per point, bands are 95% bootstrap intervals. {args.snipers} "
        f"snipers, sniper latency {args.gap:g}× the LP's. At B = 0 the three rules order "
        "identically.\n"
        f"Unit: bps of the span's volume, not dollars (dollars are in headline_b250.csv). "
        f"Under PGA each sniper bids {alpha:g}× the prize it sees"
        + (f", the LP {beta:g}× its exposure." if beta else "; the LP does not bid."),
        fontsize=9.5, color=INK_MUTED,
    )
    args.out.mkdir(parents=True, exist_ok=True)
    stem = "main_chart" if (args.gap, args.snipers) == (0.5, 3) else (
        f"main_chart_gap{args.gap:g}_n{args.snipers}")
    for ext in ["png", "svg"]:
        fig.savefig(args.out / f"{stem}.{ext}", facecolor=BG, dpi=200)
    print(f"wrote {args.out / stem}.png and .svg (font: {font})")


if __name__ == "__main__":
    main()
