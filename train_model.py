from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, hour, month, dayofweek, radians, sin, cos, 
    atan2, sqrt, degrees, abs as spark_abs, when, log1p, expm1
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxiTripDurationTraining") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Helper function for Haversine Distance in PySpark SQL
def pyspark_haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (sin(dlat / 2) ** 2) + cos(lat1) * cos(lat2) * (sin(dlon / 2) ** 2)
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

# Helper function for Bearing in PySpark SQL
def pyspark_bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    dlon = lon2 - lon1
    x = sin(dlon) * cos(lat2)
    y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)
    bearing = degrees(atan2(x, y))
    return (bearing + 360) % 360

# 2. Load Raw CSV Data
print("Loading CSV data...")
raw_ds = spark.read.csv("train.csv", header=True, inferSchema=True)

# Parse pickup_datetime
ds = raw_ds.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))

# 3. Filtering & Cleaning
ds = ds.filter(col("passenger_count").between(1, 6)) \
       .filter(col("trip_duration").between(60, 10800)) \
       .filter(col("pickup_latitude").between(40.4, 41.0)) \
       .filter(col("pickup_longitude").between(-74.3, -73.6)) \
       .filter(col("dropoff_latitude").between(40.4, 41.0)) \
       .filter(col("dropoff_longitude").between(-74.3, -73.6))

# 4. Feature Engineering
# Distance & Speed Filter
ds = ds.withColumn("distance_KM", pyspark_haversine(col("pickup_latitude"), col("pickup_longitude"), col("dropoff_latitude"), col("dropoff_longitude")))
ds = ds.withColumn("speed_km_h", col("distance_KM") / (col("trip_duration") / 3600.0))
ds = ds.filter(col("speed_km_h") <= 100)

# Time Features
ds = ds.withColumn("hours", hour(col("pickup_datetime"))) \
       .withColumn("month", month(col("pickup_datetime"))) \
       .withColumn("day_of_week", dayofweek(col("pickup_datetime"))) \
       .withColumn("is_weekend", when(col("day_of_week") >= 6, 1).otherwise(0)) \
       .withColumn("is_rush_hour", when(col("hours").between(8, 17), 1).otherwise(0)) \
       .withColumn("hour_day_interaction", col("hours") * col("day_of_week"))

# Spatial Features
ds = ds.withColumn("bearing", pyspark_bearing(col("pickup_latitude"), col("pickup_longitude"), col("dropoff_latitude"), col("dropoff_longitude"))) \
       .withColumn("manhattan_distance", spark_abs(col("pickup_latitude") - col("dropoff_latitude")) + spark_abs(col("pickup_longitude") - col("dropoff_longitude")))

# Airport Distances
jfk_lat, jfk_lon = 40.6413, -73.7781
lga_lat, lga_lon = 40.7769, -73.8740
manhattan_lat, manhattan_long = 40.7580, -73.9855

ds = ds.withColumn("pickup_dist_jfk", pyspark_haversine(col("pickup_latitude"), col("pickup_longitude"), jfk_lat, jfk_lon)) \
       .withColumn("dropoff_dist_jfk", pyspark_haversine(col("dropoff_latitude"), col("dropoff_longitude"), jfk_lat, jfk_lon)) \
       .withColumn("pickup_dist_lga", pyspark_haversine(col("pickup_latitude"), col("pickup_longitude"), lga_lat, lga_lon)) \
       .withColumn("dropoff_dist_lga", pyspark_haversine(col("dropoff_latitude"), col("dropoff_longitude"), lga_lat, lga_lon))

ds = ds.withColumn("is_jfk", when((col("pickup_dist_jfk") <= 2) | (col("dropoff_dist_jfk") <= 2), 1).otherwise(0)) \
       .withColumn("is_lga", when((col("pickup_dist_lga") <= 2) | (col("dropoff_dist_lga") <= 2), 1).otherwise(0)) \
       .withColumn("is_airport", when((col("is_jfk") == 1) | (col("is_lga") == 1), 1).otherwise(0))

ds = ds.withColumn("pickup_distance_from_center", pyspark_haversine(col("pickup_latitude"), col("pickup_longitude"), manhattan_lat, manhattan_long)) \
       .withColumn("dropoff_distance_from_center", pyspark_haversine(col("dropoff_latitude"), col("dropoff_longitude"), manhattan_lat, manhattan_long))

# Target Variable Transformation: log1p(trip_duration)
ds = ds.withColumn("log_duration", log1p(col("trip_duration")))

# 5. ML Pipeline Setup
feature_cols = [
    'vendor_id', 'passenger_count', 'pickup_longitude',
    'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',
    'distance_KM', 'manhattan_distance', 'bearing',
    'hours', 'month', 'day_of_week',
    'is_weekend', 'is_rush_hour', 'hour_day_interaction', 'is_jfk',
    'is_lga', 'is_airport', 'pickup_distance_from_center',
    'dropoff_distance_from_center'  
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Gradient Boosted Trees (Equivalent to LightGBM in native Spark)
gbt = GBTRegressor(featuresCol="features", labelCol="log_duration", maxDepth=8, maxBins=64)

pipeline = Pipeline(stages=[assembler, gbt])

# 6. Train-Test Split & Fitting
train_df, test_df = ds.randomSplit([0.8, 0.2], seed=42)

print("Training GBT Model pipeline...")
model_pipeline = pipeline.fit(train_df)

# 7. Evaluation (RMSLE calculation)
predictions = model_pipeline.transform(test_df)

evaluator = RegressionEvaluator(labelCol="log_duration", predictionCol="prediction", metricName="rmse")
rmsle = evaluator.evaluate(predictions)
print(f"--- Model Evaluation: RMSLE = {rmsle:.4f} ---")

# Save trained pipeline to disk
model_path = "./saved_ml_pipeline"
model_pipeline.write().overwrite().save(model_path)
print(f"Pipeline successfully saved to '{model_path}'!")

spark.stop()