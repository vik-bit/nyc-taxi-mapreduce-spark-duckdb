"""Build Task 4 scaling plot from results/timings.csv."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.machine_config import RESULTS_DIR, TIMINGS_CSV  # noqa: E402


def main() -> None:
    if not TIMINGS_CSV.exists():
        raise FileNotFoundError(TIMINGS_CSV)

    df = pd.read_csv(TIMINGS_CSV)
    # Prefer task4 rows; also accept task1 1m as partial if needed
    sub = df[(df["task"] == "task4") & (df["condition"] == "aggregation")].copy()
    if sub.empty:
        raise RuntimeError("No task4 aggregation rows in timings.csv")

    # Median of OK runs; timeouts plotted at 900
    plot_rows = []
    for (system, rows), g in sub.groupby(["system", "dataset_rows"]):
        ok = g[g["status"] == "OK"]["time_seconds"]
        timed_out = (g["status"] == "TIMEOUT_900").any()
        if timed_out and ok.empty:
            med = 900.0
            is_timeout = True
        elif not ok.empty:
            med = float(ok.median())
            is_timeout = timed_out
        else:
            med = float(g["time_seconds"].median())
            is_timeout = timed_out
        plot_rows.append(
            {"system": system, "dataset_rows": rows, "median_seconds": med, "timeout": is_timeout}
        )

    plot_df = pd.DataFrame(plot_rows)
    fig, ax = plt.subplots(figsize=(8, 5))
    markers = {"mapreduce": "o", "spark": "s", "duckdb": "^"}
    for system, g in plot_df.groupby("system"):
        g = g.sort_values("dataset_rows")
        ax.plot(
            g["dataset_rows"],
            g["median_seconds"],
            marker=markers.get(system, "o"),
            label=system,
        )
        to = g[g["timeout"]]
        if not to.empty:
            ax.scatter(
                to["dataset_rows"],
                to["median_seconds"],
                marker="x",
                s=80,
                c="red",
                zorder=5,
                label=f"{system} timeout" if False else None,
            )

    ax.set_xlabel("Dataset rows")
    ax.set_ylabel("Median execution time (seconds)")
    ax.set_title("Task 4 scaling: aggregation on CSV")
    ax.legend()
    ax.grid(True, alpha=0.3)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / "scaling_plot.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
