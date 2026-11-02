#!/usr/bin/env python3
import os, socket, sys, time
from urllib.error import URLError
from urllib.request import urlopen
import happybase
from thrift.Thrift import TException

timeout = int(os.environ.get("READY_TIMEOUT_SECONDS", "180"))
hdfs_url = os.environ["HDFS_WEB_URL"].rstrip("/") + "/webhdfs/v1/?op=GETHOMEDIRECTORY"
deadline, last_error = time.monotonic() + timeout, "not attempted"
while time.monotonic() < deadline:
    connection = None
    try:
        with urlopen(hdfs_url, timeout=5) as response:
            if response.status != 200: raise RuntimeError(f"WebHDFS returned HTTP {response.status}")
        connection = happybase.Connection(os.environ["HBASE_HOST"], port=int(os.environ.get("HBASE_PORT", "9090")), timeout=5000)
        connection.open(); connection.tables()
        print("HDFS WebHDFS and HBase Thrift are ready.")
        stream_host = os.environ.get("F1_STREAM_HOST")
        if stream_host:
            socket.create_connection((stream_host, int(os.environ.get("F1_STREAM_PORT", "9999"))), timeout=5).close()
            print(f"Telemetry stream at {stream_host} is accepting connections.")
        sys.exit(0)
    except (OSError, URLError, RuntimeError, TException) as exc:
        last_error = str(exc); time.sleep(2)
    finally:
        if connection is not None: connection.close()
print(f"Timed out after {timeout}s waiting for HDFS WebHDFS, HBase Thrift and the telemetry stream: {last_error}", file=sys.stderr)
sys.exit(1)
