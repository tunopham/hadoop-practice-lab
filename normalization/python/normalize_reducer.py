#!/usr/bin/env python3
import sys

def main():
    """
    Reducer for phase 2.
    Emits normalized lines in the original order.
    """
    for line in sys.stdin:
        try:
            # Split the index and the normalized line
            index, normalized_line = line.strip().split("\t", 1)
            # Emit only the normalized line (without the index)
            print(normalized_line)
        except ValueError:
            continue

if __name__ == "__main__":
    main()