#!/usr/bin/env python3

import sys

def main():
    """Reads key-value pairs from stdin, swaps them, and prints label-distance pairs."""
    for line in sys.stdin:
        line = line.strip()
        if "\t" not in line:
            continue
        
        try:
            # Parse the input key-value pair
            distance, label = line.split("\t", 1)
            print(f"{label}\t{float(distance)}")
        except ValueError:
            continue

if __name__ == "__main__":
    main()
