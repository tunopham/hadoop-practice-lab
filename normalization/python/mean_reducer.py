#!/usr/bin/env python3

import sys

def main():
    """
    Reducer Phase 1: Computes the mean for each index.
    Input: <featureX, value>
    Output: <featureX, mean>
    """
    current_feature = None
    current_sum = 0.0
    current_count = 0

    for line in sys.stdin:
        try:
            # Read each <featureX, value> pair
            feature, value = line.strip().split("\t")
            value = float(value)

            # If the feature changes, compute and emit the mean for the previous feature
            if current_feature is not None and feature != current_feature:
                print(f"{current_feature}\t{current_sum / current_count}")
                current_sum = 0.0
                current_count = 0

            # Update variables for the current feature
            current_feature = feature
            current_sum += value
            current_count += 1

        except ValueError as e:
            print(f"Ignored line: {line.strip()} - Error: {e}", file=sys.stderr)

    # Emit the mean for the last feature
    if current_feature is not None:
        print(f"{current_feature}\t{current_sum / current_count}")

if __name__ == "__main__":
    main()