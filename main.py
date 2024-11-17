"""
Main cli or app entry point
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query_transform
import os


if __name__ == "__main__":
    current_directory = os.getcwd()
    print(current_directory)
    extract()
    load()
    query_transform()