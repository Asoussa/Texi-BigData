import numpy as np
import pandas as pd
import onnxruntime as rt

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, hour, month, dayofweek, radians, sin, cos, 
    atan2, sqrt, degrees, abs as spark_abs, when, expm1, pandas_udf, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


STORAGE_ACCOUNT_NAME="nycdataset1"
STORAGE_ACCOUNT_KEY="4COOZKKxYaA7KU5FexS9167y3TuEpTfBxctOzMKisLNPeZTZWuNyJDqTZpXXjeDwW8MHTubweNaJ+AStr7rH6Q=="
CONTAINER_NAME="processed-data"

output_path = (
    f"wasbs://{CONTAINER_NAME}@"
    f"{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/output_data/"
)
checkpoint_path = f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/checkpoints/predictions_checkpoint/"


spark = SparkSession.builder \
    .appName("KafkaToAzureBlob") \
    .config("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

spark.sparkContext.setLogLevel("WARN")


def pyspark_haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (sin(dlat / 2.0) ** 2) + cos(lat1) * cos(lat2) * (sin(dlon / 2.0) ** 2)
    return R * 2.0 * atan2(sqrt(a), sqrt(1.0 - a))

def pyspark_bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    dlon = lon2 - lon1
    x = sin(dlon) * cos(lat2)
    y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)
    bearing = degrees(atan2(x, y))
    return (bearing + 360.0) % 360.0


event_schema = StructType([
    StructField("id", StringType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True)
])


kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "NYC") \
    .option("startingOffsets", "latest") \
    .load()


parsed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")) \
    .select("data.*")


df = parsed_df.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))


df = df.withColumn("distance_KM", pyspark_haversine(
    col("pickup_latitude"), col("pickup_longitude"),
    col("dropoff_latitude"), col("dropoff_longitude")
))

df = (
    df.withColumn("hours", hour(col("pickup_datetime")))
      .withColumn("month", month(col("pickup_datetime")))
      .withColumn("day_of_week", (dayofweek(col("pickup_datetime")) + 1) % 7 + 1)
      .withColumn("is_weekend", when(col("day_of_week") >= 6, 1).otherwise(0))
      .withColumn("is_rush_hour", when(col("hours").between(8, 17), 1).otherwise(0))
      .withColumn("hour_day_interaction", col("hours") * col("day_of_week"))
)

# Spatial Features
df = df.withColumn("bearing", pyspark_bearing(
    col("pickup_latitude"), col("pickup_longitude"),
    col("dropoff_latitude"), col("dropoff_longitude")
))
df = df.withColumn("manhattan_distance", spark_abs(col("pickup_latitude") - col("dropoff_latitude")) + 
                                       spark_abs(col("pickup_longitude") - col("dropoff_longitude")))


jfk_lat, jfk_lon = 40.6413, -73.7781
lga_lat, lga_lon = 40.7769, -73.8740
manhattan_lat, manhattan_long = 40.7580, -73.9855

df = df.withColumn("pickup_dist_jfk", pyspark_haversine(col("pickup_latitude"), col("pickup_longitude"), lit(jfk_lat), lit(jfk_lon))) \
       .withColumn("dropoff_dist_jfk", pyspark_haversine(col("dropoff_latitude"), col("dropoff_longitude"), lit(jfk_lat), lit(jfk_lon))) \
       .withColumn("pickup_dist_lga", pyspark_haversine(col("pickup_latitude"), col("pickup_longitude"), lit(lga_lat), lit(lga_lon))) \
       .withColumn("dropoff_dist_lga", pyspark_haversine(col("dropoff_latitude"), col("dropoff_longitude"), lit(lga_lat), lit(lga_lon)))

df = df.withColumn("is_jfk", when((col("pickup_dist_jfk") <= 2) | (col("dropoff_dist_jfk") <= 2), 1).otherwise(0)) \
       .withColumn("is_lga", when((col("pickup_dist_lga") <= 2) | (col("dropoff_dist_lga") <= 2), 1).otherwise(0)) \
       .withColumn("is_airport", when((col("is_jfk") == 1) | (col("is_lga") == 1), 1).otherwise(0))

df = df.withColumn("pickup_distance_from_center", pyspark_haversine(col("pickup_latitude"), col("pickup_longitude"), lit(manhattan_lat), lit(manhattan_long))) \
       .withColumn("dropoff_distance_from_center", pyspark_haversine(col("dropoff_latitude"), col("dropoff_longitude"), lit(manhattan_lat), lit(manhattan_long)))

df = df.withColumn("Predicted", lit(1))


feature_cols = [
    'vendor_id', 'passenger_count', 'pickup_longitude',
    'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',
    'distance_KM', 'manhattan_distance', 'bearing',
    'hours', 'month', 'day_of_week',
    'is_weekend', 'is_rush_hour', 'hour_day_interaction', 'is_jfk',
    'is_lga', 'is_airport', 'pickup_distance_from_center',
    'dropoff_distance_from_center'
]

onnx_model_path = "/tmp/saved_lgb_model.onnx"

_session = None

def get_onnx_session():
    global _session
    if _session is None:
        _session = rt.InferenceSession(onnx_model_path, providers=["CPUExecutionProvider"])
    return _session

@pandas_udf(DoubleType())
def predict_onnx_udf(*cols: pd.Series) -> pd.Series:
    X = np.column_stack(cols).astype(np.float32)
    session = get_onnx_session()
    input_name = session.get_inputs()[0].name
    
    preds = session.run(None, {input_name: X})[0]

    return pd.Series(preds.squeeze()).astype(float)

df = df.withColumn("log_prediction", predict_onnx_udf(*feature_cols)) \
                   .withColumn("predicted_trip_duration_sec", expm1(col("log_prediction")))

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()



query.awaitTermination()