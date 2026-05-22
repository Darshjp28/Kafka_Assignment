"""
producer.py  —  Reads hour.csv row by row and publishes to raw-data topic @ 1/sec
"""
import json, time
import pandas as pd
from kafka import KafkaProducer

FEATURES = ["season","yr","mnth","hr","holiday","weekday",
            "workingday","weathersit","temp","atemp","hum","windspeed"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
)

df = pd.read_csv("hour.csv")
print(f"[Producer] {len(df)} rows loaded. Streaming at 1 row/sec...")

for i, row in df.iterrows():
    record = {
        "row_id":     int(i),
        "timestamp":  str(row["dteday"]) + f" {int(row['hr']):02d}:00",
        "actual_cnt": int(row["cnt"]),
        "features":   {col: float(row[col]) for col in FEATURES},
    }
    producer.send("raw-data", key=str(i), value=record)
    print(f"[Producer] row={i:>5}  ts={record['timestamp']}  actual_cnt={record['actual_cnt']}")
    time.sleep(1)

producer.flush()
print("[Producer] Done.")
