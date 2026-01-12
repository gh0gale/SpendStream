import csv

def read_csv(file_path):
    """
    Reads a CSV file and returns rows as dictionaries
    """
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)
