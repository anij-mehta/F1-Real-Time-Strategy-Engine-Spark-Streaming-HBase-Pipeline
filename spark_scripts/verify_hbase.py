"""Scan f1_leaderboard and print a sample of stored Gold records."""

import argparse
import os
import sys

import happybase


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("HBASE_HOST", "hbase"))
    parser.add_argument("--port", type=int, default=int(os.getenv("HBASE_PORT", "9090")))
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit <= 0:
        raise ValueError("--limit must be greater than zero")

    connection = happybase.Connection(args.host, port=args.port, timeout=5000)
    try:
        table = connection.table("f1_leaderboard")
        rows = list(table.scan(limit=args.limit))
        if not rows:
            print("No Gold records found in f1_leaderboard.", file=sys.stderr)
            return 1
        for row_key, columns in rows:
            decoded_columns = {
                key.decode("utf-8"): value.decode("utf-8")
                for key, value in columns.items()
            }
            print(f"{row_key.decode('utf-8')}\t{decoded_columns}")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
