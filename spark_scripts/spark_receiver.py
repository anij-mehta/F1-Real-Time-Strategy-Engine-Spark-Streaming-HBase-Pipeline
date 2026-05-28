from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("F1LiveStream") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Set log level to WARN to reduce console clutter
spark.sparkContext.setLogLevel("WARN")

# 2. Define Updated Schema for Strategy Analytics
schema = StructType([
    StructField("Driver", StringType(), True),
    StructField("LapNumber", IntegerType(), True),
    StructField("LapTime", StringType(), True),
    StructField("GapToAhead", DoubleType(), True),
    StructField("Compound", StringType(), True),
    StructField("TyreLife", DoubleType(), True),
    StructField("FreshTyre", StringType(), True),
    StructField("Position", IntegerType(), True),
    StructField("IsPitStop", BooleanType(), True)
])

# 3. Connect to Stream 
df = spark.readStream \
    .format("socket") \
    .option("host", "192.168.1.4") \
    .option("port", 9999) \
    .load()

# 4. Process JSON
json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Output to Console
print("🚀 Strategy Engine Active. Monitoring for Undercuts & Degradation...")

query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()