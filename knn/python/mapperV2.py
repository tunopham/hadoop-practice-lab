#!/usr/bin/env python3

import sys
import csv
import math

# Define the test example
TEST_EXAMPLE = [2.0, 3.0]

def read_example(line):
    """
    Parse a line of CSV input into a label and features.
    """
    reader = csv.reader([line])
    for row in reader:
        if len(row) < 2:  # Ensure there's at least one label and one feature
            return None, None
        label = row[0]
        try:
            features = list(map(float, row[1:]))
        except ValueError:
            return None, None  # Handle potential conversion errors
        return label, features
    return None, None

def get_distance(test_example, training_example):
    """
    Calculate the Euclidean distance between the test example and the training example.
    """
    return math.sqrt(sum((t - tr) ** 2 for t, tr in zip(test_example, training_example)))

def main():
    for line in sys.stdin:
        line = line.strip()
        label, features = read_example(line)  # Parse the current training example
        if label is None or features is None:
            continue
        distance = get_distance(TEST_EXAMPLE, features)  # Compute distance
        print(f"{label}\t{float(distance)}")  # Emit distance and label with precision

if __name__ == "__main__":
    main()
