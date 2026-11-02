# Repository Audit

Scope: `README.md`, `f1_streamer.py`, `docker-compose.yml`, `hadoop.env`, and every file in `spark_scripts/`. This is a static review; the services and jobs were not started.

## 1. What each reviewed file actually does

### `README.md`

- Describes the intended F1 telemetry, Spark, MLlib, HBase pipeline and its basic manual startup flow (`README.md:1-28`, `README.md:32-68`).
- Instructs a user to start Compose, run the host-side Python socket producer, and submit `f1_strategy_gold.py` inside the Spark master (`README.md:34-56`).
- Documents querying an HBase table, but does not document table creation, HBase Thrift, HDFS archive locations, checkpoints, a dashboard, or failure recovery (`README.md:61-70`).

### `f1_streamer.py`

- Opens a TCP server on all host interfaces at port 9999 and accepts one client before loading historical 2024 Bahrain race laps from FastF1 (`f1_streamer.py:7-19`). It is a replay source, not a live telemetry integration.
- Sorts all laps globally by recorded time, creates one JSON record per lap, and writes newline-delimited messages every 0.2 seconds (`f1_streamer.py:21-50`).
- Calculates `GapToAhead` as the absolute time difference from the preceding *globally sorted lap*. It does not identify the driver ahead or calculate an on-track gap (`f1_streamer.py:27-31`).
- Emits driver, lap, tyre, position, and pit-out-derived `IsPitStop` fields (`f1_streamer.py:34-44`), then closes both sockets when the replay finishes or on an exception (`f1_streamer.py:52-56`).

### `docker-compose.yml`

- Starts a NameNode and DataNode using Hadoop 3.2.1 images; both consume `hadoop.env` (`docker-compose.yml:5-25`).
- Starts a standalone `harisekhon/hbase:latest` container, exposes only its web UI port, and does not configure it to use the named HDFS services (`docker-compose.yml:30-38`).
- Starts one Spark master and one worker, both as `root`, with only `spark_scripts/` mounted at `/opt/spark/work-dir` (`docker-compose.yml:43-70`).
- Exposes Spark master UI and cluster port, but does not expose or run the socket producer, create an HBase table, configure a Thrift service, install Python dependencies, or start a Streamlit dashboard (`docker-compose.yml:1-74`).

### `hadoop.env`

- Configures the default filesystem as `hdfs://namenode:8020` (`hadoop.env:1`).
- Enables WebHDFS and disables HDFS permissions (`hadoop.env:5-6`).
- Allows the `hue` proxy user from any host and group, while setting Hadoop's static HTTP user to `root` (`hadoop.env:2-4`).

### `spark_scripts/spark_receiver.py`

- Defines a fuller version of the producer payload schema and reads a Spark socket source from the fixed address `192.168.1.4:9999` (`spark_scripts/spark_receiver.py:14-32`).
- Parses JSON and displays all rows in the Spark console sink; it neither filters nor stores data (`spark_scripts/spark_receiver.py:34-48`).
- Is an independent receiver/debug job. The README does not instruct users to submit it.

### `spark_scripts/f1_strategy_gold.py`

- Creates a Spark Structured Streaming application aimed at `spark://spark-master:7077` (`spark_scripts/f1_strategy_gold.py:9-15`).
- Reads the same fixed socket endpoint, parses a reduced schema, filters pit-stop rows, and assigns a processing-time `timestamp` (`spark_scripts/f1_strategy_gold.py:17-37`).
- Uses a 10-second watermark and groups by driver in 5-second **tumbling** windows, calculating average gap and tyre life (`spark_scripts/f1_strategy_gold.py:39-42`).
- For each nonempty micro-batch, fits one linear regression across all driver/window aggregates, using average tyre life as the sole feature and average gap as the label (`spark_scripts/f1_strategy_gold.py:45-57`).
- Opens an HBase Thrift connection via HappyBase, writes the calculated fields and a `PIT`/`STAY` result to `f1_leaderboard`, and starts a `foreachBatch` sink (`spark_scripts/f1_strategy_gold.py:59-88`).

## 2. Differences from the requested pipeline specification

| Specification item | Current implementation | Concrete mismatch |
| --- | --- | --- |
| Bronze socket stream | Socket source exists in `f1_strategy_gold.py:26-37`; producer exists in `f1_streamer.py:7-50`. | The producer is a finite historical replay, and the consumer is hard-coded to a private LAN address. Bronze is not archived. |
| Silver schema validation | `from_json` with nullable fields is used (`f1_strategy_gold.py:17-35`). | There is no malformed-record handling, required-field validation, null/range validation, dead-letter path, or validation metrics. `from_json` can yield null fields and the code does not explicitly reject or account for them. |
| Silver filter in/out laps | Only `IsPitStop == False` is filtered (`f1_strategy_gold.py:33-37`). | No in-lap/out-lap definition is implemented. `IsPitStop` means a non-null `PitOutTime` in the producer (`f1_streamer.py:43`), so it does not reliably identify both kinds of non-racing laps. |
| 5-second sliding window + 10-second watermark | A 10-second watermark is present (`f1_strategy_gold.py:40`). | `window(timestamp, "5 seconds")` at `f1_strategy_gold.py:41` is tumbling because it omits a slide duration. A 5-second sliding window requires an explicit slide interval. The timestamp is processing time, not an event-time value carried in telemetry. |
| Per-driver average gap and tyre life | Aggregation groups by `Driver` and computes both averages (`f1_strategy_gold.py:41-42`). | This portion is implemented, subject to the window/event-time and input-quality limitations above. |
| Gold MLlib LinearRegression decay rate | MLlib `LinearRegression` is fitted (`f1_strategy_gold.py:51-57`). | One regression is fitted per batch across all drivers/windows, then the same coefficient is stored for every row. It is not a per-driver time-series decay model, does not use a time/lap feature, and can only model cross-sectional `AvgTyreLife -> AvgGap` association. |
| 0.2-second margin to `PIT`/`STAY` | `decay_rate > 0.2` produces `PIT` (`f1_strategy_gold.py:68-69`). | The threshold exists, but is applied to the shared batch coefficient rather than an individual driver's computed decay. The code does not establish that this coefficient is in seconds per lap/window or otherwise a valid 0.2-second margin. |
| Pit-window classifier | None. | No classifier model, features, training/inference path, output column, or persistence exists. |
| HBase upsert to `f1_leaderboard` with specified column families | `table.put` writes `cf_race:avg_gap`, `cf_race:tyre_life`, `cf_strat:decay`, and `cf_strat:decision` (`f1_strategy_gold.py:61-76`). | The requested columns are present in code, but table and family creation are absent. The row key includes driver and only `HHMMSS` window start (`f1_strategy_gold.py:65-66`), omitting date and timezone; it can collide across days/reruns. |
| Parquet archive of Bronze and Silver on HDFS | None. | No `writeStream` to Parquet, no HDFS paths, no checkpoint locations, and no archive retention/partitioning logic exist. |
| Streamlit dashboard polling HBase every 2-3 seconds | None. | No Streamlit source, Compose service, port, HBase client configuration, or polling implementation exists. |

## 3. Bugs and fragile spots

- **Likely unreachable stream endpoint.** The producer binds on the host (`f1_streamer.py:8-13`), whereas both Spark jobs try `192.168.1.4:9999` from a container (`spark_scripts/f1_strategy_gold.py:27-31`, `spark_scripts/spark_receiver.py:28-32`). There is no Compose service or documented host-network route making that address valid. The fixed private address will fail on many machines.
- **No reconnection or multi-client capability.** The producer accepts exactly one connection (`f1_streamer.py:13-16`), never accepts another after disconnect, and exits after replay (`f1_streamer.py:52-56`). A Spark restart therefore requires restarting the producer.
- **`send()` can partially write a message.** `conn.send(...)` is used instead of `sendall(...)` (`f1_streamer.py:46-47`), so a partial TCP write can split/truncate a JSON line without retry logic.
- **Data semantics are incorrect for race gaps.** `GapToAhead` compares adjacent records in globally time-sorted laps (`f1_streamer.py:21`, `f1_streamer.py:27-31`), which can compare unrelated drivers/laps and is not necessarily a gap to the car ahead.
- **Pit filtering is semantically narrow.** `IsPitStop` only tests `PitOutTime` (`f1_streamer.py:43`); it does not model pit entry/in-lap status and may treat laps with missing or unusual FastF1 values incorrectly.
- **A failure can leak the HBase connection.** `connection.close()` is inside the `try` after the write loop (`spark_scripts/f1_strategy_gold.py:61-79`). An error after connection creation but before line 79 skips close. There is also no retry/backoff or idempotency handling.
- **HBase is not runnable as coded from this Compose definition.** HappyBase requires an HBase Thrift endpoint at port 9090 (`spark_scripts/f1_strategy_gold.py:59-62`), but Compose neither enables nor exposes/configures a Thrift server (`docker-compose.yml:30-38`). The table/families are never created, so `table.put` will also fail unless manually prepared.
- **Spark's Python runtime may lack HappyBase.** The Spark image is mounted with scripts only (`docker-compose.yml:54-56`, `docker-compose.yml:68-70`); there is no Dockerfile, `requirements.txt`, `pip install`, or `--py-files` dependency distribution for `happybase`.
- **No streaming checkpoint.** The sink at `spark_scripts/f1_strategy_gold.py:83-86` sets no `checkpointLocation`. Restart recovery and exactly-once progress tracking are unavailable; Spark may use a temporary checkpoint or reject/behave unsafely depending on configuration.
- **The socket source is non-replayable.** Even with checkpoints, Spark's socket source cannot recover data sent during outages. The repo has no durable broker/log between producer and Spark.
- **Regression can fail or be meaningless for a small/degenerate batch.** `lr.fit(...)` (`spark_scripts/f1_strategy_gold.py:54-57`) needs sufficient non-null, nonconstant training data. A batch with a single aggregate or constant `AvgTyreLife` can fail or produce an uninformative coefficient; the broad exception merely logs and drops the batch (`spark_scripts/f1_strategy_gold.py:80-81`).
- **Driver independence is lost in Gold.** `batch_df` is fit as one dataset, yet its one `decay_rate` is written for every driver (`spark_scripts/f1_strategy_gold.py:51-57`, `spark_scripts/f1_strategy_gold.py:64-76`). This contradicts a driver-specific strategy recommendation.
- **Driver collection to the Spark driver does not scale.** `batch_df.collect()` at `spark_scripts/f1_strategy_gold.py:64` materializes every aggregate in driver memory and serializes all HBase writes there. There is no bounded batch size or partition-aware writer.
- **Row keys can overwrite results.** `HHMMSS` has no date and only second precision (`spark_scripts/f1_strategy_gold.py:65-66`). Running another replay or processing overlapping/repeated windows at the same clock time overwrites prior rows for a driver.
- **Documentation contradicts code.** README claims a 5-second sliding window with 30-second duration (`README.md:11`), but code defines a 5-second tumbling window and no 30-second duration (`spark_scripts/f1_strategy_gold.py:39-42`). README also describes live telemetry (`README.md:20`), while the producer replays a fixed 2024 session (`f1_streamer.py:18-21`).
- **Infrastructure security and reproducibility are weak.** HDFS permissions are disabled and WebHDFS enabled (`hadoop.env:5-6`); proxy-user access is wildcarded (`hadoop.env:3-4`); Spark runs as root (`docker-compose.yml:46`, `docker-compose.yml:61`); administrative/service ports are published on all host interfaces (`docker-compose.yml:10-11`, `docker-compose.yml:35-36`, `docker-compose.yml:51-53`); and HBase uses the floating `latest` image tag (`docker-compose.yml:30-31`).

## 4. What runs today versus what is missing

### Present in source

- A manual FastF1 historical-lap replay server that emits newline-delimited JSON after one client connects.
- A debug Spark receiver that prints parsed socket data to the console.
- A Spark Gold job with pit-stop filtering, per-driver/window averages, one batch-wide linear regression, and attempted HappyBase writes using the requested four HBase columns.
- Docker definitions for one-node HDFS, HBase, and a Spark master/worker cluster.

### Not present or not reliably runnable without external/manual setup

- A portable connection between host producer and Spark containers; the fixed `192.168.1.4` address is environment-specific.
- Confirmed HBase Thrift service, HBase table creation, `cf_race`/`cf_strat` family creation, and compatible HBase/HDFS integration.
- Dependency provisioning for FastF1/Pandas on the producer host and HappyBase in Spark containers.
- Bronze and Silver Parquet archives on HDFS, along with checkpointing and recovery semantics.
- The specified Silver validation, in-lap/out-lap filtering, and genuine 5-second sliding event-time window.
- Per-driver decay modeling, a pit-window classifier, model training/versioning, or quality checks.
- A Streamlit dashboard or any component polling HBase every 2-3 seconds.
- Automated startup orchestration, health checks, tests, monitoring, and an end-to-end runbook for the missing services.
