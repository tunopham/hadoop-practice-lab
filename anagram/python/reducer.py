#!/usr/bin/env python3

import sys
from collections import defaultdict

def main():
    """Aggregates words by their sorted anagram key and prints grouped anagrams."""
    anagrams = defaultdict(list)

    for line in sys.stdin:
        stripped_line = line.strip()
        if "\t" not in stripped_line:
            continue
        sorted_key, original_line = stripped_line.split("\t", 1)
        anagrams[sorted_key].append(original_line)

    for key, values in anagrams.items():
        print(f"{key}\t{', '.join(values)}")

if __name__ == "__main__":
    main()
