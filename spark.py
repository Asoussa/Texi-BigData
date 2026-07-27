from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, hour, dayofweek, month,
    when, radians, sin, cos, atan2, sqrt, lit, round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# 1. إنشاء جلسة Spark
spark = SparkSession.builder \
    .appName("NYC_Taxi_Cleaning_FeatureEngineering") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. تعريف الـ Schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("pickup_longitude", FloatType(), True),
    StructField("pickup_latitude", FloatType(), True),
    StructField("dropoff_longitude", FloatType(), True),
    StructField("dropoff_latitude", FloatType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("trip_duration", IntegerType(), True)
])

# 3. القراءة من Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "NYC") \
    .option("startingOffsets", "latest") \
    .load()

# 4. فك تشفير JSON
parsed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. تحويل التواريخ من String إلى Timestamp (لإلغاء قيم NULL)
timestamp_df = parsed_df \
    .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))

# 6. إضافة حقول التاريخ والوقت (Date/Time Features)
time_df = timestamp_df \
    .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
    .withColumn("pickup_dayofweek", dayofweek(col("pickup_datetime"))) \
    .withColumn("pickup_month", month(col("pickup_datetime"))) \
    .withColumn("is_weekend", when(col("pickup_dayofweek").isin([1, 7]), 1).otherwise(0))

# 7. حساب مسافة الرحلة بالـ KM (Haversine Formula)
R = 6371.0 # نصف قطر الأرض بالـ KM
lat1 = radians(col("pickup_latitude"))
lon1 = radians(col("pickup_longitude"))
lat2 = radians(col("dropoff_latitude"))
lon2 = radians(col("dropoff_longitude"))

dlat = lat2 - lat1
dlon = lon2 - lon1

a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
c = 2 * atan2(sqrt(a), sqrt(1 - a))
distance = R * c

df_with_distance = time_df.withColumn("trip_distance_km", round(distance, 2))

# 8. حساب السرعة (Speed km/h)
final_df = df_with_distance.withColumn(
    "speed_kmh",
    when(col("trip_duration") > 0, round((col("trip_distance_km") / (col("trip_duration") / 3600)), 2))
    .otherwise(0)
)

# 9. طباعة المخرجات على الشاشة (Console Output)
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()