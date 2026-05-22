"""
consumer.py  —  Reads from predictions topic and prints formatted results
"""
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "predictions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="predictions-display-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("[Consumer] Listening on predictions topic...\n")
print(f"{'Row':>5}  {'Timestamp':<18}  {'Actual':>8}  {'Predicted':>10}  {'Error':>8}")
print("-" * 60)

total, total_err = 0, 0.0
for msg in consumer:
    r = msg.value
    print(f"{r['row_id']:>5}  {r['timestamp']:<18}  "
          f"{r['actual_cnt']:>8}  {r['predicted_cnt']:>10.1f}  {r['abs_error']:>8.1f}")
    total     += 1
    total_err += r["abs_error"]
    if total % 10 == 0:
        print(f"  → Running MAE after {total} predictions: {total_err/total:.2f} bikes\n")
