import csv
import json
import time 
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'NYC'
CSV_FILE_PATH = r"D:\Mydownload\archive (6)\NYC.csv"


#time.sleep(5)  


with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        vendor_id = int(row["vendor_id"])
        payload = {
            "id": row["id"],
            "vendor_id": int(row["vendor_id"]),
            "pickup_datetime": row["pickup_datetime"],
            "dropoff_datetime": row["dropoff_datetime"],
            "passenger_count": int(row["passenger_count"]),
            "pickup_longitude": float(row["pickup_longitude"]),
            "pickup_latitude": float(row["pickup_latitude"]),
            "dropoff_longitude": float(row["dropoff_longitude"]),
            "dropoff_latitude": float(row["dropoff_latitude"]),
            "store_and_fwd_flag": row["store_and_fwd_flag"],
            "trip_duration": int(row["trip_duration"])
        }
        
        producer.send(
            topic=TOPIC_NAME, 
            key=str(vendor_id).encode('utf-8'), 
            value=payload
        )
        #time.sleep(0.05)
        print(f"Sent: {payload['id']} | Pickup: {payload['pickup_datetime']}")
        

        time.sleep(2)

producer.flush()
producer.close()