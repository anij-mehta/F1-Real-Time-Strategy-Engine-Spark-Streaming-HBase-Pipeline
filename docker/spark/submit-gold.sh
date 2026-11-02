#!/usr/bin/env bash
set -euo pipefail
python3 /opt/f1/bin/wait-for-services.py
exec /opt/spark/bin/spark-submit \
  --py-files /opt/spark/work-dir/hbase_sink.py \
  /opt/spark/work-dir/f1_strategy_gold.py
