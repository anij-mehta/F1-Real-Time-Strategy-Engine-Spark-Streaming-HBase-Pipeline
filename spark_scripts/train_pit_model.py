"""Train a race-grouped pit-window classifier from historical FastF1 laps."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

from config import (
    APP_NAME,
    PIT_CATEGORICAL_FEATURE_COLUMNS,
    PIT_MODEL_PATH,
    PIT_NUMERIC_FEATURE_COLUMNS,
)


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-path", type=Path, default=Path("/opt/f1/docs/training_data.jsonl"),
        help="JSON-lines training laps written by training_data.py",
    )
    parser.add_argument(
        "--holdout-race",
        default="2024_Australia",
        help="Race identifier YEAR_GP reserved exclusively for evaluation (default: 2024_Australia)",
    )
    parser.add_argument("--model-path", default=PIT_MODEL_PATH, help="HDFS destination for the best PipelineModel")
    parser.add_argument(
        "--report-path", type=Path, default=Path("docs/model_report.md"), help="local Markdown metrics report"
    )
    return parser.parse_args()


def add_class_weights(train_df):
    counts = {row["label"]: row["count"] for row in train_df.groupBy("label").count().collect()}
    positives, negatives = counts.get(1.0, 0), counts.get(0.0, 0)
    if positives == 0 or negatives == 0:
        raise ValueError("Training races must contain both pit-soon and non-pit examples")
    positive_weight = negatives / positives
    return train_df.withColumn("weight", when(col("label") == lit(1.0), lit(positive_weight)).otherwise(lit(1.0)))


def build_pipeline(classifier) -> Pipeline:
    indexers = [
        StringIndexer(inputCol=column, outputCol=f"{column}_index", handleInvalid="keep")
        for column in PIT_CATEGORICAL_FEATURE_COLUMNS
    ]
    assembler = VectorAssembler(
        inputCols=[*PIT_NUMERIC_FEATURE_COLUMNS, *(f"{column}_index" for column in PIT_CATEGORICAL_FEATURE_COLUMNS)],
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=[*indexers, assembler, classifier])


def evaluate(predictions) -> dict[str, float | None]:
    counts = {(row["label"], row["prediction"]): row["count"] for row in predictions.groupBy("label", "prediction").count().collect()}
    true_positive = counts.get((1.0, 1.0), 0)
    false_positive = counts.get((0.0, 1.0), 0)
    false_negative = counts.get((1.0, 0.0), 0)
    true_negative = counts.get((0.0, 0.0), 0)
    total = true_positive + false_positive + false_negative + true_negative
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    metrics: dict[str, float | None] = {
        "accuracy": (true_positive + true_negative) / total if total else 0.0,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc": None,
    }
    try:
        metrics["auc"] = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction").evaluate(predictions)
    except Exception as exc:
        LOGGER.warning("AUC unavailable for this held-out race: %s", exc)
    return metrics


def write_report(path: Path, holdout_race: str, train_races: list[str], results: list[dict[str, Any]], best_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for result in results:
        auc = "n/a" if result["auc"] is None else f"{result['auc']:.4f}"
        rows.append(
            f"| {result['name']} | {result['accuracy']:.4f} | {result['precision']:.4f} | "
            f"{result['recall']:.4f} | {result['f1']:.4f} | {auc} |"
        )
    rows_text = "\n".join(rows)
    path.write_text(
        "# Pit Window Model Report\n\n"
        f"- Held-out race: `{holdout_race}`\n"
        f"- Training races: {', '.join(f'`{race}`' for race in train_races)}\n"
        "- Split strategy: race-grouped; the held-out race was never included in training.\n"
        "- Class imbalance: positive examples are weighted by the training negative/positive ratio.\n"
        f"- Selected model: `{best_name}` (highest held-out F1; AUC breaks ties).\n\n"
        "| Model | Accuracy | Precision | Recall | F1 | AUC |\n"
        "| --- | ---: | ---: | ---: | ---: | ---: |\n"
        f"{rows_text}\n",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not args.data_path.exists():
        raise FileNotFoundError(f"{args.data_path} not found; run the export-training-data step first")
    records = [json.loads(line) for line in args.data_path.read_text(encoding="utf-8").splitlines() if line]
    if not records:
        raise RuntimeError("No labelled training records were loaded")

    spark = SparkSession.builder.appName(f"{APP_NAME}-PitModelTraining").master(os.getenv("TRAIN_SPARK_MASTER", "local[2]")).getOrCreate()
    dataframe = spark.createDataFrame(records)
    races = sorted(row["Race"] for row in dataframe.select("Race").distinct().collect())
    if args.holdout_race not in races:
        raise ValueError(f"Held-out race {args.holdout_race!r} was not loaded; available races: {races}")
    train_df = add_class_weights(dataframe.filter(col("Race") != lit(args.holdout_race)))
    test_df = dataframe.filter(col("Race") == lit(args.holdout_race))
    if train_df.rdd.isEmpty() or test_df.rdd.isEmpty():
        raise ValueError("Race-grouped split produced an empty training or held-out dataset")
    train_races = [race for race in races if race != args.holdout_race]
    if len(train_races) < 2:
        raise ValueError(f"At least two training races are required; found {train_races}")

    candidates = [
        ("LogisticRegression", LogisticRegression(labelCol="label", weightCol="weight", maxIter=100)),
        ("DecisionTreeClassifier", DecisionTreeClassifier(labelCol="label", weightCol="weight", maxDepth=6)),
    ]
    results: list[dict[str, Any]] = []
    models: dict[str, Any] = {}
    for name, classifier in candidates:
        model = build_pipeline(classifier).fit(train_df)
        metrics = evaluate(model.transform(test_df))
        results.append({"name": name, **metrics})
        models[name] = model
        LOGGER.info("%s held-out metrics: %s", name, metrics)

    best = max(results, key=lambda result: (result["f1"], result["auc"] if result["auc"] is not None else -1.0))
    models[best["name"]].write().overwrite().save(args.model_path)
    write_report(args.report_path, args.holdout_race, train_races, results, best["name"])
    LOGGER.info("Saved %s to %s and wrote %s", best["name"], args.model_path, args.report_path)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
