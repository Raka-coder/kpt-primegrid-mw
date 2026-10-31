"""Build benchmark charts from the raw CSV files in results/."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _group(rows: list[dict[str, str]], key: str, value: str) -> dict[int, float]:
    grouped: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        grouped[int(row[key])].append(float(row[value]))
    return {int(item): statistics.fmean(values) for item, values in grouped.items()}


def plot_scenario_a(path: Path, output: Path) -> bool:
    rows = _rows(path)
    if not rows:
        return False
    workers = _group(rows, "workers", "elapsed_s")
    speedups = _group(rows, "workers", "speedup")
    counts = sorted(workers)
    baseline = workers.get(1, max(workers.values()) if workers else 1.0)
    elapsed = [workers[count] for count in counts]
    speedup = [speedups.get(count, baseline / workers[count]) for count in counts]

    figure, (left, right) = plt.subplots(1, 2, figsize=(11, 4.2))
    left.plot(counts, elapsed, marker="o", color="#1f77b4")
    left.set_xlabel("Workers")
    left.set_ylabel("Elapsed time (s)")
    left.set_title("Scenario A - elapsed time")
    left.set_xticks(counts)
    left.grid(alpha=0.3)

    right.plot(counts, speedup, marker="o", label="measured", color="#2ca02c")
    right.plot(counts, counts, linestyle="--", label="ideal linear", color="#888888")
    right.set_xlabel("Workers")
    right.set_ylabel("Speedup (T1 / TN)")
    right.set_title("Scenario A - speedup")
    right.set_xticks(counts)
    right.legend()
    right.grid(alpha=0.3)
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)
    return True


def plot_scenario_b(job_path: Path, status_path: Path, output: Path) -> bool:
    job_rows = _rows(job_path)
    status_rows = _rows(status_path)
    if not job_rows and not status_rows:
        return False

    figure, axes = plt.subplots(2, 2, figsize=(11, 8))
    if job_rows:
        clients = sorted({int(row["clients"]) for row in job_rows})
        throughput = _group(job_rows, "clients", "throughput_jobs_s")
        latency = _group(job_rows, "clients", "latency_mean_s")
        axes[0][0].plot(clients, [throughput[item] for item in clients], marker="o")
        axes[0][0].set_title("Scenario B - job throughput")
        axes[0][0].set_ylabel("jobs/s")
        axes[0][0].set_xticks(clients)

        axes[0][1].plot(clients, [latency[item] for item in clients], marker="o")
        axes[0][1].set_title("Scenario B - job latency")
        axes[0][1].set_ylabel("seconds")
        axes[0][1].set_xticks(clients)
    if status_rows:
        clients = sorted({int(row["clients"]) for row in status_rows})
        throughput = _group(status_rows, "clients", "throughput_req_s")
        latency = _group(status_rows, "clients", "latency_mean_s")
        axes[1][0].plot(clients, [throughput[item] for item in clients], marker="o")
        axes[1][0].set_title("Scenario B - GetJobStatus throughput")
        axes[1][0].set_ylabel("requests/s")
        axes[1][0].set_xlabel("Concurrent clients")
        axes[1][0].set_xticks(clients)

        axes[1][1].plot(clients, [latency[item] for item in clients], marker="o")
        axes[1][1].set_title("Scenario B - GetJobStatus latency")
        axes[1][1].set_ylabel("seconds")
        axes[1][1].set_xlabel("Concurrent clients")
        axes[1][1].set_xticks(clients)
    for row in axes:
        for axis in row:
            axis.grid(alpha=0.3)
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="results")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output) if args.output else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    wrote = False
    if plot_scenario_a(input_dir / "scenario_a.csv", output_dir / "scenario_a.png"):
        print(f"wrote {output_dir / 'scenario_a.png'}")
        wrote = True
    if plot_scenario_b(
        input_dir / "scenario_b_jobs.csv",
        input_dir / "scenario_b_status.csv",
        output_dir / "scenario_b.png",
    ):
        print(f"wrote {output_dir / 'scenario_b.png'}")
        wrote = True
    if not wrote:
        print("no benchmark CSV files found")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
