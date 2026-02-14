#!/usr/bin/env python3
"""
Script to parse AB-CROWN and Luna instance files and create per-benchmark
CSV files with combined results including bound widths and runtimes.
"""

import csv
import os
import sys
from collections import defaultdict

# Increase CSV field size limit for large bounds arrays
csv.field_size_limit(sys.maxsize)


def parse_instances(filepath):
    """Parse an instances CSV file and return a dict keyed by (benchmark, onnx_file, vnnlib_file)."""
    instances = {}
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row['benchmark'], row['onnx_file'], row['vnnlib_file'])
            instances[key] = {
                'bound_width': row['bound_width'],
                'wall_time': row['wall_time'],
                'status': row['status'],
                'timed_out': row['timed_out']
            }
    return instances


def main():
    # Paths (relative to the script's directory)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    results_dir = os.path.join(script_dir, 'exact_results')

    abcrown_file = os.path.join(output_dir, 'abcrown_instances.csv')
    luna_file = os.path.join(output_dir, 'luna_instances.csv')

    # Create output directory
    os.makedirs(results_dir, exist_ok=True)

    # Parse both instance files
    print("Parsing AB-CROWN instances...")
    abcrown_instances = parse_instances(abcrown_file)
    print(f"  Found {len(abcrown_instances)} instances")

    print("Parsing Luna instances...")
    luna_instances = parse_instances(luna_file)
    print(f"  Found {len(luna_instances)} instances")

    # Get all unique benchmarks
    benchmarks = set()
    for key in abcrown_instances.keys():
        benchmarks.add(key[0])
    for key in luna_instances.keys():
        benchmarks.add(key[0])

    print(f"\nFound {len(benchmarks)} benchmarks: {sorted(benchmarks)}")

    # Process each benchmark
    for benchmark in sorted(benchmarks):
        # Collect all instances for this benchmark
        benchmark_data = []

        # Get all unique (onnx, vnnlib) pairs for this benchmark
        instance_keys = set()
        for key in abcrown_instances.keys():
            if key[0] == benchmark:
                instance_keys.add((key[1], key[2]))
        for key in luna_instances.keys():
            if key[0] == benchmark:
                instance_keys.add((key[1], key[2]))

        for onnx_file, vnnlib_file in sorted(instance_keys):
            key = (benchmark, onnx_file, vnnlib_file)

            abcrown_data = abcrown_instances.get(key, {})
            luna_data = luna_instances.get(key, {})

            row = {
                'onnx_file': onnx_file,
                'vnnlib_file': vnnlib_file,
                'abcrown_bound_width': abcrown_data.get('bound_width', ''),
                'luna_bound_width': luna_data.get('bound_width', ''),
                'abcrown_runtime': abcrown_data.get('wall_time', ''),
                'luna_runtime': luna_data.get('wall_time', '')
            }
            benchmark_data.append(row)

        # Write benchmark CSV
        output_file = os.path.join(results_dir, f'{benchmark}_results.csv')
        with open(output_file, 'w', newline='') as f:
            fieldnames = ['onnx_file', 'vnnlib_file', 'abcrown_bound_width',
                         'luna_bound_width', 'abcrown_runtime', 'luna_runtime']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(benchmark_data)

        print(f"Created {output_file} with {len(benchmark_data)} instances")

    print(f"\nDone! Results written to {results_dir}/")


if __name__ == '__main__':
    main()
