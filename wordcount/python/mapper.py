#!/usr/bin/env python3

import sys

def main():
    """Reads lines from stdin, splits them into words, and prints word count pairs."""
    for line in sys.stdin:
        for word in line.split():
            print(f"{word}\t1")

if __name__ == "__main__":
    main()
