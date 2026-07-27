from pyspark.sql import SparkSession
from pyspark.sql.functions import (
<<<<<<< Updated upstream
    from_json, col, to_timestamp, hour, dayofweek, month, 
    when, radians, sin, cos, atan2, sqrt, lit, round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from kafka import KafkaConsumer
=======
    from_json, col, to_timestamp, hour, dayofweek, month, udf,
    when, radians, sin, cos, atan2, sqrt, lit, round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import requests as r
import json
>>>>>>> Stashed changes
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
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "NYC") \
    .option("startingOffsets", "latest") \
    .load()

# 4. فك تشفير JSON
parsed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# =========================================================
#  أولاً: تنظيف البيانات (DATA CLEANING)
# =========================================================

cleaned_df = parsed_df \
    .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "M/d/yyyy H:m")) \
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"), "M/d/yyyy H:m")) \
    .withColumn("store_and_fwd_flag_numeric", when(col("store_and_fwd_flag") == "Y", 1).otherwise(0))

filtered_df = cleaned_df.filter(
    col("id").isNotNull() &
    (col("passenger_count") > 0) & 
    (col("passenger_count") <= 6) &
    (col("trip_duration") > 0) & (col("trip_duration") <= 86400) &
    (col("pickup_latitude").between(40.5, 41.0)) &
    (col("dropoff_latitude").between(40.5, 41.0)) &
    (col("pickup_longitude").between(-74.3, -73.7)) &
    (col("dropoff_longitude").between(-74.3, -73.7))
)

# =========================================================
#  ثانياً: هندسة الخصائص وإضافة المدينة والمنطقة (CITY & BOROUGH)
# =========================================================

# 1. إضافة عمود المدينة (City) وعمود المنطقة (Borough) بناءً على الإحداثيات
city_df = filtered_df \
    .withColumn("city", lit("New York")) \
    .withColumn("pickup_borough", 
        when((col("pickup_latitude").between(40.70, 40.88)) & (col("pickup_longitude").between(-74.02, -73.93)), "Manhattan")
        .when((col("pickup_latitude").between(40.57, 40.73)) & (col("pickup_longitude").between(-74.05, -73.85)), "Brooklyn")
        .when((col("pickup_latitude").between(40.54, 40.80)) & (col("pickup_longitude").between(-73.96, -73.70)), "Queens")
        .when((col("pickup_latitude").between(40.78, 40.91)) & (col("pickup_longitude").between(-73.93, -73.78)), "Bronx")
        .otherwise("NYC Outer/Airport")
    )

# 2. استخراج ميزات الوقت
time_features_df = city_df \
    .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
    .withColumn("pickup_dayofweek", dayofweek(col("pickup_datetime"))) \
    .withColumn("pickup_month", month(col("pickup_datetime"))) \
<<<<<<< Updated upstream
    .withColumn("is_weekend", when(col("pickup_dayofweek").isin(1, 7), 1).otherwise(0))
=======
    .withColumn("is_weekend", when(col("pickup_dayofweek").isin([1, 7]), 1).otherwise(0))
    
>>>>>>> Stashed changes

# 3. حساب المسافة الجغرافية (Haversine Distance)
lat1 = radians(col("pickup_latitude"))
lon1 = radians(col("pickup_longitude"))
lat2 = radians(col("dropoff_latitude"))
lon2 = radians(col("dropoff_longitude"))

dlat = lat2 - lat1
dlon = lon2 - lon1

a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
c = 2 * atan2(sqrt(a), sqrt(1 - a))
r = 6371.0
distance_df = time_features_df.withColumn("trip_distance_km", round(lit(r) * c, 2))

# 4. حساب السرعة
final_df = distance_df.withColumn(
    "speed_kmh", 
    round((col("trip_distance_km") / (col("trip_duration") / 3600)), 2)
)
#####GEt the neighborhood ##############

<<<<<<< Updated upstream
# =========================================================
#  ثالثاً: طباعة النتائج
# =========================================================
=======

def get_nyc_neighborhood(lat, lon):
    if lat is None or lon is None:
        return "Unknown1"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
    }
    
    # Pass actual float values (not ints, not Spark columns)
    url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=1"
    
    try:
        response = r.get(url, headers=headers, timeout=3)
        if response.status_code == 200:
            data = response.json()
            address = data["address"]
            # Safely fall back if 'neighbourhood' key is missing
            return address["neighbourhood"] if "neighbourhood" in address else "Unknown"
    except Exception:
        pass
    return "Unknown2"

geocode_udf = udf(get_nyc_neighborhood, StringType())

final_df = final_df \
    .withColumn("pickup_neighborhood", geocode_udf(col("pickup_latitude"), col("pickup_longitude"))) \
    .withColumn("dropoff_neighborhood", geocode_udf(col("dropoff_latitude"), col("dropoff_longitude")))
# 9. طباعة المخرجات على الشاشة (Console Output)
>>>>>>> Stashed changes
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()