"""
Main cli or app entry point
"""

from mylib.ETL import extract, load, query_transform
import os


if __name__ == "__main__":
    current_directory = os.getcwd()
    print(current_directory)
    extract()
    load()
    query_transform()
