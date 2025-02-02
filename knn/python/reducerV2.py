#!/usr/bin/env python3

import sys

def main():
    """Reads label-distance pairs, sorts them by distance, and prints sorted results."""
    distances = []

    for line in sys.stdin:
        line = line.strip()
        if "\t" not in line:
            continue
        
        try:
            # Read the label and distance
            label, distance = line.split("\t", 1)
            distances.append((label, float(distance)))
        except ValueError:
            continue

    # Sort pairs by ascending distance
    distances.sort(key=lambda x: x[1])

    # Emit each sorted pair
    for label, distance in distances:
        print(f"{label}\t{distance}")

if __name__ == "__main__":
    main()
