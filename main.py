"""handles cli commands"""

import sys
import argparse
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import (
    update_record,
    delete_record,
    create_record,
    general_query,
    read_data,
)


def handle_arguments(args):
    """add action based on inital calls"""
    # parses command line arguments to determine which action
    parser = argparse.ArgumentParser(description="ETL-Query script")
    # sets up specific arguments
    parser.add_argument(
        "action",
        choices=[
            "extract",
            "transform_load",
            "update_record",
            "delete_record",
            "create_record",
            "general_query",
            "read_data",
        ],
    )
    # parses first argument action to dtermine which action requested
    args = parser.parse_args(args[:1])
    print(args.action)
    if args.action == "update_record":
        parser.add_argument("id", type=int)
        parser.add_argument("Major")
        parser.add_argument("Major_category")
        parser.add_argument("Grad_total", type=int)
        parser.add_argument("Grad_employed", type=int)

    if args.action == "create_record":
        parser.add_argument("Major")
        parser.add_argument("Major_category")
        parser.add_argument("Grad_total", type=int)
        parser.add_argument("Grad_employed", type=int)

    if args.action == "general_query":
        parser.add_argument("query")

    if args.action == "delete_record":
        parser.add_argument("id", type=int)

    # parse again with ever
    return parser.parse_args(sys.argv[1:])


def main():
    """handles all the cli commands"""
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        print("Extracting data...")
        extract()
    elif args.action == "transform_load":
        print("Transforming data...")
        load()
    elif args.action == "update_record":
        update_record(
            args.id,
            args.Major,
            args.Major_category,
            args.Grad_total,
            args.Grad_employed,
        )
    elif args.action == "delete_record":
        delete_record(args.id)
    elif args.action == "create_record":
        create_record(
            args.Major, args.Major_category, args.Grad_total, args.Grad_employed
        )
    elif args.action == "general_query":
        general_query(args.query)
    elif args.action == "read_data":
        data = read_data()
        print(data)
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
