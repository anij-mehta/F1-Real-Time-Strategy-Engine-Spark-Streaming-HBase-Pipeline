#!/usr/bin/env python3
"""Idempotently initialize required HDFS paths and the HBase table schema."""
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import happybase
from thrift.Thrift import TException

HDFS_WEB_URL = os.environ["HDFS_WEB_URL"].rstrip("/")
HBASE_HOST = os.environ["HBASE_HOST"]
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))
TIMEOUT_SECONDS = int(os.environ.get("READY_TIMEOUT_SECONDS", "180"))
TABLE = "f1_leaderboard"
TABLE_BYTES = TABLE.encode("ascii")
REQUIRED_FAMILIES = {b"cf_race", b"cf_strat"}

def webhdfs(path, operation, method="GET"):
    request = Request(f"{HDFS_WEB_URL}/webhdfs/v1{path}?op={operation}", method=method)
    with urlopen(request, timeout=10) as response:
        return response.status, response.read()

def wait_until_ready():
    deadline, last_error = time.monotonic() + TIMEOUT_SECONDS, "not attempted"
    while time.monotonic() < deadline:
        connection = None
        try:
            status, _ = webhdfs("/", "GETHOMEDIRECTORY")
            if status != 200:
                raise RuntimeError(f"WebHDFS returned HTTP {status}")
            connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=5000)
            connection.open()
            tables = set(connection.tables())
            # A listening Thrift port is not enough: HBase can accept the
            # connection while its master is still bringing table metadata
            # online.  A descriptor read proves the existing table is usable.
            if TABLE_BYTES in tables:
                connection.table(TABLE).families()
            print("HDFS WebHDFS and HBase Thrift table metadata are ready.")
            return
        except (OSError, URLError, HTTPError, RuntimeError, TException) as exc:
            last_error = str(exc); time.sleep(2)
        finally:
            if connection is not None: connection.close()
    raise RuntimeError(f"Timed out after {TIMEOUT_SECONDS}s waiting for HDFS at {HDFS_WEB_URL} and HBase Thrift at {HBASE_HOST}:{HBASE_PORT}: {last_error}")

def ensure_hdfs_directories():
    for path in ("/f1/bronze", "/f1/silver"):
        status, _ = webhdfs(path, "MKDIRS", method="PUT")
        if status != 200:
            raise RuntimeError(f"Could not create HDFS directory {path}: HTTP {status}")
        print(f"Ensured HDFS directory: {path}")

def ensure_hbase_table():
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=60000)
    try:
        connection.open()
        if TABLE_BYTES not in set(connection.tables()):
            connection.create_table(TABLE, {"cf_race": {}, "cf_strat": {}})
            print("Created HBase table f1_leaderboard with cf_race and cf_strat.")
            return
        missing = REQUIRED_FAMILIES - set(connection.table(TABLE).families())
        if missing:
            names = ", ".join(name.decode("ascii") for name in sorted(missing))
            raise RuntimeError(f"Existing f1_leaderboard is missing required column families: {names}. Refusing to alter schema automatically.")
        print("Verified existing HBase table f1_leaderboard with cf_race and cf_strat.")
    finally:
        connection.close()

def main():
    try:
        wait_until_ready(); ensure_hdfs_directories(); ensure_hbase_table()
    except Exception as exc:
        print(f"Initialization failed: {exc}", file=sys.stderr)
        return 1
    print("Initialization completed successfully.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
