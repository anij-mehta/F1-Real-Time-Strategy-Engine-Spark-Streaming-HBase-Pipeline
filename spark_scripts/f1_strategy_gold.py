from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, current_timestamp
from pyspark.sql.types import *
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import happybase
import logging

# 1. Setup Spark with MLlib
spark = SparkSession.builder \
    .appName("F1-Strategy-Gold") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Schema Definition
schema = StructType([
    StructField("Driver", StringType(), True),
    StructField("LapNumber", IntegerType(), True),
    StructField("GapToAhead", DoubleType(), True),
    StructField("TyreLife", DoubleType(), True),
    StructField("IsPitStop", BooleanType(), True)
])

# 3. [BRONZE] - Ingestion
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "192.168.1.4") \
    .option("port", 9999) \
    .load()

# Filter for active racing laps only
bronze_df = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("IsPitStop") == False) \
    .withColumn("timestamp", current_timestamp())

# 4. [SILVER] - Aggregation (5-second window)
silver_df = bronze_df.withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "5 seconds"), "Driver") \
    .agg(avg("GapToAhead").alias("AvgGap"), avg("TyreLife").alias("AvgTyreLife"))

# 5. [GOLD] - MLlib & HBase Upsert
def process_gold_layer(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # A. Regression Model: Predict decay
    try:
        assembler = VectorAssembler(inputCols=["AvgTyreLife"], outputCol="features")
        ml_prep = assembler.transform(batch_df)
        
        # Simple Linear Regression to find the 'decay rate'
        lr = LinearRegression(featuresCol="features", labelCol="AvgGap")
        model = lr.fit(ml_prep)
        decay_rate = model.coefficients[0]
        
        # B. HBase Integration
        # Connect to Thrift server (default port 9090)
        connection = happybase.Connection('hbase', port=9090)
        table = connection.table('f1_leaderboard')
        
        for row in batch_df.collect():
            # Create a unique row key: Driver + Window Start
            row_key = f"{row['Driver']}_{row['window'].start.strftime('%H%M%S')}"
            
            # Identify 'Pit' trigger: If decay is > 0.2s margin
            trigger = "PIT" if decay_rate > 0.2 else "STAY"
            
            table.put(row_key, {
                b'cf_race:avg_gap': str(row['AvgGap']).encode(),
                b'cf_race:tyre_life': str(row['AvgTyreLife']).encode(),
                b'cf_strat:decay': str(decay_rate).encode(),
                b'cf_strat:decision': trigger.encode()
            })
        
        print(f"✅ Batch {batch_id} processed. Global Decay Rate: {decay_rate:.4f}")
        connection.close()
    except Exception as e:
        print(f"❌ Error in Gold Layer: {e}")

# 6. Start the Stream
query = silver_df.writeStream \
    .foreachBatch(process_gold_layer) \
    .start()

query.awaitTermination()