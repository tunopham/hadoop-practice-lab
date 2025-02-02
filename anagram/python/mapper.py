#!/usr/bin/env python3

import sys

def main():
    """Reads lines from stdin, generates a sorted anagram key, and prints key-value pairs."""
    for line in sys.stdin:
        stripped_line = line.strip()
        sorted_key = ''.join(sorted(stripped_line.replace(" ", "").lower()))
        print(f"{sorted_key}\t{stripped_line}")

if __name__ == "__main__":
    main()
