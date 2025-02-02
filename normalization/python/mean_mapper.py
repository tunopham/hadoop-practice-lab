#!/usr/bin/env python3

import sys
import csv

def main():
    """Reads CSV input from stdin, extracts features, and prints index-feature pairs."""
    for line in sys.stdin:
        try:
            row = next(csv.reader([line.strip()]))
            features = row[1:]  # Skip the first column (label)

            for index, feature in enumerate(features):
                print(f"feature{index+1}\t{feature}")

        except Exception as e:
            print(f"Ignored line: {line.strip()} - Error: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
