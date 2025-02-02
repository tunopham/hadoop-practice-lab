#!/usr/bin/env python3

import sys
import csv

def load_means_from_local(local_path):
    """Helper function to read the cached file on HDFS via a local symlink."""
    means = {}
    try:
        with open(local_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                feature, mean_str = line.split("\t", 1)
                means[feature] = float(mean_str)
    except Exception as e:
        print(f"Error loading means file: {e}", file=sys.stderr)
        sys.exit(1)  # Exit if file reading fails
    return means

def main():
    """Reads features from stdin, normalizes them using precomputed means, and outputs the results."""
    if len(sys.argv) < 2:
        print("Usage: normalize_mapper.py <means_file>", file=sys.stderr)
        sys.exit(1)

    local_means_file = sys.argv[1]
    means = load_means_from_local(local_means_file)

    reader = csv.reader(sys.stdin)
    for index, row in enumerate(reader):
        try:
            label = row[0]
            features = list(map(float, row[1:]))
            normalized_features = [
                feature - means.get(f"feature{i+1}", 0) for i, feature in enumerate(features)
            ]

            print(f"{index}\t{label},{','.join(map(str, normalized_features))}")
        except (ValueError, IndexError, KeyError) as e:
            print(f"Skipped line: {row} - Error: {e}", file=sys.stderr)
            continue
if __name__ == "__main__":
    main()