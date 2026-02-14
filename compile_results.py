#!/usr/bin/env python3
"""
Compile verification results from abcrown and luna tools into CSVs.

Usage:
    python compile_results.py <luna_results_dir> <abcrown_results_dir>

Each results directory should contain benchmark subdirectories with slurm-* folders.

Generates two CSVs per tool:
1. Per-instance results (one row per slurm job)
2. Aggregated results by benchmark (averages and totals)

Only includes instances where BOTH tools have results (either timeout or computed bounds).
"""

import argparse
import re
import csv
from pathlib import Path
from collections import defaultdict


def parse_args_line(content):
    """Extract onnx and vnnlib filenames from 'c args:' line."""
    onnx_file = None
    vnnlib_file = None

    match = re.search(r"^c args:\s+(.+)$", content, re.MULTILINE)
    if match:
        args = match.group(1).strip().split()
        for arg in args:
            if arg.endswith(".onnx"):
                onnx_file = Path(arg).name
            elif arg.endswith(".vnnlib"):
                vnnlib_file = Path(arg).name

    return onnx_file, vnnlib_file


def parse_abcrown_run_out(filepath):
    """Parse abcrown run.out file for bounds, result, and time."""
    result = {"lower_bounds": None, "upper_bounds": None, "status": None, "time": None, "onnx_file": None, "vnnlib_file": None}

    try:
        with open(filepath, "r") as f:
            content = f.read()
    except Exception:
        return result

    # Extract onnx and vnnlib filenames
    result["onnx_file"], result["vnnlib_file"] = parse_args_line(content)

    # Extract result status (unsat, timeout, unknown)
    match = re.search(r"^Result:\s*(\w+)", content, re.MULTILINE)
    if match:
        status = match.group(1).lower()
        # Normalize to verified/unverified
        if status == "unsat":
            result["status"] = "verified"
        else:
            result["status"] = status  # timeout or unknown

    # Extract time
    match = re.search(r"^Time:\s*([\d.]+)", content, re.MULTILINE)
    if match:
        result["time"] = float(match.group(1))

    # Extract final alpha-crown bounds (prefer these over initial CROWN bounds)
    # Format: initial alpha-crown lower bounds: [val1, val2, ...]
    lower_match = re.search(r"initial alpha-crown lower bounds:\s*\[([-\d.,\s]+)\]", content)
    upper_match = re.search(r"initial alpha-crown upper bounds:\s*\[([-\d.,\s]+)\]", content)

    # Fallback to initial CROWN bounds if alpha-crown not found
    if not lower_match:
        lower_match = re.search(r"initial CROWN lower bounds:\s*\[([-\d.,\s]+)\]", content)
    if not upper_match:
        upper_match = re.search(r"initial CROWN upper bounds:\s*\[([-\d.,\s]+)\]", content)

    if lower_match:
        try:
            result["lower_bounds"] = [float(x.strip()) for x in lower_match.group(1).split(",")]
        except ValueError:
            pass

    if upper_match:
        try:
            result["upper_bounds"] = [float(x.strip()) for x in upper_match.group(1).split(",")]
        except ValueError:
            pass

    return result


def parse_luna_run_out(filepath):
    """Parse luna run.out file for bounds and result."""
    result = {"lower_bounds": None, "upper_bounds": None, "status": None, "time": None, "onnx_file": None, "vnnlib_file": None}

    try:
        with open(filepath, "r") as f:
            content = f.read()
    except Exception:
        return result

    # Extract onnx and vnnlib filenames
    result["onnx_file"], result["vnnlib_file"] = parse_args_line(content)

    # Extract result status - Luna outputs: "Result: unsat", "Result: sat", "Result: unknown"
    # unsat = property verified (no counterexample exists)
    # sat = counterexample found (property violated/disproved)
    # Both count as "verified" since the property was resolved
    match = re.search(r"^Result:\s*(\w+)", content, re.MULTILINE)
    if match:
        status = match.group(1).lower()
        if status in ("unsat", "sat"):
            result["status"] = "verified"
        else:
            result["status"] = "unknown"

    # Fallback: check for older "Property status:" format
    if result["status"] is None:
        match = re.search(r"^Property status:\s*(\w+)", content, re.MULTILINE)
        if match:
            status = match.group(1).upper()
            if status in ("VERIFIED", "VIOLATED"):
                result["status"] = "verified"
            else:
                result["status"] = "unknown"

    # Extract output bounds
    # Format: [lower1, upper1] [lower2, upper2] ...
    match = re.search(r"^Output Bounds:\s*\n(.+)", content, re.MULTILINE)
    if match:
        bounds_line = match.group(1).strip()
        # Parse all [lower, upper] pairs
        pairs = re.findall(r"\[([-\d.]+),\s*([-\d.]+)\]", bounds_line)
        if pairs:
            result["lower_bounds"] = [float(p[0]) for p in pairs]
            result["upper_bounds"] = [float(p[1]) for p in pairs]

    return result


def parse_output_log(filepath):
    """Parse output.log file for runlim wall clock time and timeout status.

    Returns:
        dict with 'wall_time' (float or None) and 'timed_out' (bool)
    """
    result = {"wall_time": None, "timed_out": False}

    try:
        with open(filepath, "r") as f:
            content = f.read()
    except Exception:
        return result

    # Extract real (wall clock) time from runlim output
    # Format: [runlim] real:			11.46 seconds
    match = re.search(r"\[runlim\]\s*real:\s*([\d.]+)\s*seconds", content)
    if match:
        result["wall_time"] = float(match.group(1))

    # Check for timeout status
    # Format: [runlim] status:		out of time
    status_match = re.search(r"\[runlim\]\s*status:\s*(.+)", content)
    if status_match:
        status = status_match.group(1).strip().lower()
        result["timed_out"] = (status == "out of time")

    return result


def compute_bound_width(lower_bounds, upper_bounds):
    """Compute average width of bound intervals."""
    if not lower_bounds or not upper_bounds:
        return None
    if len(lower_bounds) != len(upper_bounds):
        return None

    widths = [u - l for l, u in zip(lower_bounds, upper_bounds)]
    return sum(widths) / len(widths)


def collect_results_for_tool(tool_name, tool_path):
    """Collect all results for a given tool from a directory.

    Args:
        tool_name: "abcrown" or "luna"
        tool_path: Path to directory containing benchmark subdirectories
    """
    results = []

    if not tool_path.exists():
        print(f"Warning: {tool_path} does not exist")
        return results

    # Iterate through benchmarks
    for benchmark_dir in sorted(tool_path.iterdir()):
            if not benchmark_dir.is_dir():
                continue
            if benchmark_dir.name == "options":
                continue

            benchmark_name = benchmark_dir.name

            # Iterate through slurm directories
            for slurm_dir in sorted(benchmark_dir.iterdir(), key=lambda x: int(x.name.split("-")[1]) if x.name.startswith("slurm-") else 0):
                if not slurm_dir.is_dir():
                    continue
                if not slurm_dir.name.startswith("slurm-"):
                    continue

                slurm_id = slurm_dir.name.split("-")[1]
                run_out = slurm_dir / "run.out"
                output_log = slurm_dir / "output.log"

                if not run_out.exists():
                    continue

                # Parse output.log for wall clock time and timeout status
                log_data = parse_output_log(output_log) if output_log.exists() else {"wall_time": None, "timed_out": False}

                # Parse based on tool
                if tool_name == "abcrown":
                    data = parse_abcrown_run_out(run_out)
                else:  # luna
                    data = parse_luna_run_out(run_out)

                # Compute bound width
                bound_width = compute_bound_width(data["lower_bounds"], data["upper_bounds"])

                # Determine if this instance has valid results:
                # Either timed out OR has computed bounds
                has_bounds = data["lower_bounds"] is not None and data["upper_bounds"] is not None
                has_result = log_data["timed_out"] or has_bounds

                results.append({
                    "tool": tool_name,
                    "benchmark": benchmark_name,
                    "slurm_id": slurm_id,
                    "onnx_file": data["onnx_file"],
                    "vnnlib_file": data["vnnlib_file"],
                    "status": data["status"],
                    "wall_time": log_data["wall_time"],
                    "timed_out": log_data["timed_out"],
                    "has_result": has_result,
                    "bound_width": bound_width,
                    "lower_bounds": data["lower_bounds"],
                    "upper_bounds": data["upper_bounds"],
                })

    return results


def write_instance_csv(results, tool_name, output_path):
    """Write per-instance CSV."""
    fieldnames = [
        "tool", "benchmark", "slurm_id", "onnx_file", "vnnlib_file",
        "status", "timed_out", "wall_time", "bound_width",
        "lower_bounds", "upper_bounds"
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in results:
            row = {
                "tool": r["tool"],
                "benchmark": r["benchmark"],
                "slurm_id": r["slurm_id"],
                "onnx_file": r["onnx_file"] or "",
                "vnnlib_file": r["vnnlib_file"] or "",
                "status": r["status"] or "",
                "timed_out": "TO" if r["timed_out"] else "",
                "wall_time": f"{r['wall_time']:.4f}" if r["wall_time"] else "",
                "bound_width": f"{r['bound_width']:.6f}" if r["bound_width"] is not None else "--",
                "lower_bounds": str(r["lower_bounds"]) if r["lower_bounds"] else "--",
                "upper_bounds": str(r["upper_bounds"]) if r["upper_bounds"] else "--",
            }
            writer.writerow(row)

    print(f"Wrote {len(results)} instances to {output_path}")


def compute_aggregates(results, common_bounds_instances=None, common_finished_instances=None):
    """Compute aggregated statistics by benchmark.

    Args:
        results: List of result dictionaries for a single tool
        common_bounds_instances: Optional set of (benchmark, slurm_id) tuples
            where both tools computed bounds. If provided, avg_bound_width,
            avg_lower_bound, and avg_upper_bound only include these instances
            for fair comparison.
        common_finished_instances: Optional set of (benchmark, slurm_id) tuples
            where both tools finished (no timeout). If provided, avg_runtime
            only includes these instances for fair comparison.
    """
    by_benchmark = defaultdict(list)
    for r in results:
        by_benchmark[r["benchmark"]].append(r)

    aggregates = []
    for benchmark, instances in sorted(by_benchmark.items()):
        total = len(instances)
        # Solved = bounds were computed
        solved = sum(1 for i in instances if i["bound_width"] is not None)
        # Timeout = no bounds computed
        timeout = sum(1 for i in instances if i["bound_width"] is None)
        # Verified = tool returned UNSAT
        verified = sum(1 for i in instances if i["status"] == "verified")

        # Filter instances for bound statistics - only where BOTH tools have bounds
        if common_bounds_instances is not None:
            common_bounds = [
                i for i in instances
                if (i["benchmark"], i["slurm_id"]) in common_bounds_instances
            ]
        else:
            # Fallback: use all instances with valid bounds (old behavior)
            common_bounds = [i for i in instances if i["bound_width"] is not None and not i["timed_out"]]

        # Average bound width
        widths = [i["bound_width"] for i in common_bounds if i["bound_width"] is not None]
        avg_width = sum(widths) / len(widths) if widths else None

        # Average lower bound (mean of per-instance mean lower bounds)
        lower_bounds = [
            sum(i["lower_bounds"]) / len(i["lower_bounds"])
            for i in common_bounds
            if i["lower_bounds"]
        ]
        avg_lower = sum(lower_bounds) / len(lower_bounds) if lower_bounds else None

        # Average upper bound (mean of per-instance mean upper bounds)
        upper_bounds = [
            sum(i["upper_bounds"]) / len(i["upper_bounds"])
            for i in common_bounds
            if i["upper_bounds"]
        ]
        avg_upper = sum(upper_bounds) / len(upper_bounds) if upper_bounds else None

        # Average runtime - only for instances where BOTH tools finished
        if common_finished_instances is not None:
            common_finished = [
                i for i in instances
                if (i["benchmark"], i["slurm_id"]) in common_finished_instances
            ]
        else:
            # Fallback: use all instances with valid time (old behavior)
            common_finished = [i for i in instances if i["wall_time"] is not None]

        times = [i["wall_time"] for i in common_finished if i["wall_time"] is not None]
        avg_time = sum(times) / len(times) if times else None

        aggregates.append({
            "benchmark": benchmark,
            "total_instances": total,
            "solved_count": solved,
            "solved_pct": (solved / total * 100) if total > 0 else 0,
            "timeout_count": timeout,
            "timeout_pct": (timeout / total * 100) if total > 0 else 0,
            "verified_count": verified,
            "verified_pct": (verified / total * 100) if total > 0 else 0,
            "avg_bound_width": avg_width,
            "avg_lower_bound": avg_lower,
            "avg_upper_bound": avg_upper,
            "avg_runtime": avg_time,
        })

    return aggregates


def write_aggregate_csv(aggregates, tool_name, output_path):
    """Write aggregated CSV."""
    fieldnames = [
        "benchmark", "total_instances",
        "solved_count", "solved_pct",
        "timeout_count", "timeout_pct",
        "verified_count", "verified_pct",
        "avg_bound_width", "avg_lower_bound", "avg_upper_bound", "avg_runtime"
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for a in aggregates:
            row = {
                "benchmark": a["benchmark"],
                "total_instances": a["total_instances"],
                "solved_count": a["solved_count"],
                "solved_pct": f"{a['solved_pct']:.2f}",
                "timeout_count": a["timeout_count"],
                "timeout_pct": f"{a['timeout_pct']:.2f}",
                "verified_count": a["verified_count"],
                "verified_pct": f"{a['verified_pct']:.2f}",
                "avg_bound_width": f"{a['avg_bound_width']:.6f}" if a["avg_bound_width"] is not None else "--",
                "avg_lower_bound": f"{a['avg_lower_bound']:.6f}" if a["avg_lower_bound"] is not None else "--",
                "avg_upper_bound": f"{a['avg_upper_bound']:.6f}" if a["avg_upper_bound"] is not None else "--",
                "avg_runtime": f"{a['avg_runtime']:.4f}" if a["avg_runtime"] is not None else "--",
            }
            writer.writerow(row)

    print(f"Wrote {len(aggregates)} benchmark aggregates to {output_path}")


def filter_common_instances(abcrown_results, luna_results):
    """Filter to only include instances where BOTH tools have results.

    An instance has results if it either timed out or computed bounds.
    Returns filtered lists for both tools.
    """
    # Build sets of (benchmark, slurm_id) tuples with valid results
    abcrown_valid = {
        (r["benchmark"], r["slurm_id"])
        for r in abcrown_results
        if r["has_result"]
    }
    luna_valid = {
        (r["benchmark"], r["slurm_id"])
        for r in luna_results
        if r["has_result"]
    }

    # Find common instances
    common = abcrown_valid & luna_valid

    print(f"ABCrown instances with results: {len(abcrown_valid)}")
    print(f"Luna instances with results: {len(luna_valid)}")
    print(f"Common instances (both tools have results): {len(common)}")

    # Filter results to only common instances
    abcrown_filtered = [
        r for r in abcrown_results
        if (r["benchmark"], r["slurm_id"]) in common
    ]
    luna_filtered = [
        r for r in luna_results
        if (r["benchmark"], r["slurm_id"]) in common
    ]

    return abcrown_filtered, luna_filtered


def get_common_bounds_instances(abcrown_results, luna_results):
    """Get set of (benchmark, slurm_id) where BOTH tools computed bounds.

    This is used for standardized bound width comparison, excluding instances
    where either tool timed out before computing bounds.
    """
    abcrown_with_bounds = {
        (r["benchmark"], r["slurm_id"])
        for r in abcrown_results
        if r["bound_width"] is not None
    }
    luna_with_bounds = {
        (r["benchmark"], r["slurm_id"])
        for r in luna_results
        if r["bound_width"] is not None
    }

    common = abcrown_with_bounds & luna_with_bounds

    print(f"ABCrown instances with bounds: {len(abcrown_with_bounds)}")
    print(f"Luna instances with bounds: {len(luna_with_bounds)}")
    print(f"Common instances (both tools have bounds): {len(common)}")

    return common


def get_common_finished_instances(abcrown_results, luna_results):
    """Get set of (benchmark, slurm_id) where BOTH tools solved (computed bounds).

    This is used for standardized runtime comparison, excluding instances
    where either tool timed out before computing bounds.
    """
    abcrown_solved = {
        (r["benchmark"], r["slurm_id"])
        for r in abcrown_results
        if r["bound_width"] is not None
    }
    luna_solved = {
        (r["benchmark"], r["slurm_id"])
        for r in luna_results
        if r["bound_width"] is not None
    }

    common = abcrown_solved & luna_solved

    print(f"ABCrown instances solved: {len(abcrown_solved)}")
    print(f"Luna instances solved: {len(luna_solved)}")
    print(f"Common instances (both tools solved): {len(common)}")

    return common


def main():
    parser = argparse.ArgumentParser(
        description="Compile verification results from Luna and ABCrown tools into CSVs."
    )
    parser.add_argument(
        "luna_results",
        type=Path,
        help="Path to Luna results directory (contains benchmark subdirs with slurm-* folders)"
    )
    parser.add_argument(
        "abcrown_results",
        type=Path,
        help="Path to ABCrown results directory (contains benchmark subdirs with slurm-* folders)"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="Output directory for CSVs (default: ./output)"
    )
    args = parser.parse_args()

    # Set output directory
    output_dir = args.output if args.output else Path("./output")
    output_dir.mkdir(exist_ok=True)

    # Collect results for both tools
    print("=" * 60)
    print("Collecting ABCrown results...")
    print("=" * 60)
    abcrown_results = collect_results_for_tool("abcrown", args.abcrown_results)
    print(f"Found {len(abcrown_results)} total ABCrown instances")

    print("\n" + "=" * 60)
    print("Collecting Luna results...")
    print("=" * 60)
    luna_results = collect_results_for_tool("luna", args.luna_results)
    print(f"Found {len(luna_results)} total Luna instances")

    # Filter to common instances (both tools have results)
    print("\n" + "=" * 60)
    print("Filtering to common instances...")
    print("=" * 60)
    abcrown_filtered, luna_filtered = filter_common_instances(abcrown_results, luna_results)

    # Get instances where BOTH tools computed bounds (for fair avg comparison)
    print("\n" + "=" * 60)
    print("Finding instances where both tools computed bounds...")
    print("=" * 60)
    common_bounds = get_common_bounds_instances(abcrown_filtered, luna_filtered)

    # Get instances where BOTH tools solved (for fair runtime comparison)
    print("\n" + "=" * 60)
    print("Finding instances where both tools solved...")
    print("=" * 60)
    common_finished = get_common_finished_instances(abcrown_filtered, luna_filtered)

    # Write results for each tool
    for tool_name, results in [("abcrown", abcrown_filtered), ("luna", luna_filtered)]:
        print(f"\n{'='*60}")
        print(f"Writing {tool_name} results...")
        print(f"{'='*60}")

        if not results:
            print(f"No results found for {tool_name}")
            continue

        # Write per-instance CSV
        instance_csv = output_dir / f"{tool_name}_instances.csv"
        write_instance_csv(results, tool_name, instance_csv)

        # Compute and write aggregates (using common instances for fair comparison)
        aggregates = compute_aggregates(results, common_bounds, common_finished)
        aggregate_csv = output_dir / f"{tool_name}_aggregated.csv"
        write_aggregate_csv(aggregates, tool_name, aggregate_csv)

    print(f"\nDone! CSVs written to {output_dir}")


if __name__ == "__main__":
    main()
