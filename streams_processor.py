"""
streams_processor.py  —  Faust Streams processor
Consumes raw-data topic, runs Random Forest model, publishes to predictions topic.

Run with:  faust -A streams_processor worker -l info
"""
import json, joblib
import faust
import numpy as np

FEATURES = ["season","yr","mnth","hr","holiday","weekday",
            "workingday","weathersit","temp","atemp","hum","windspeed"]

# Faust App setup
app = faust.App(
    "bike-sharing-processor",
    broker="kafka://localhost:9092",
    value_serializer="json",
    consumer_auto_offset_reset="earliest",
    topic_replication_factor=1,
)

raw_topic         = app.topic("raw-data",    value_type=bytes)
predictions_topic = app.topic("predictions", value_type=bytes)

# Load model once at startup
model  = joblib.load("model/bike_model.joblib")
scaler = joblib.load("model/scaler.joblib")
print("[Processor] Model and scaler loaded.")

# Faust Agent — this IS the Streams topology
@app.agent(raw_topic, sink=[predictions_topic])
async def process_record(stream):
    async for record in stream:
        try:
            X     = np.array([[record["features"][c] for c in FEATURES]])
            pred  = float(model.predict(scaler.transform(X))[0])
            pred  = max(0.0, round(pred, 1))
            error = round(abs(pred - record["actual_cnt"]), 1)

            output = {
                "row_id":        record["row_id"],
                "timestamp":     record["timestamp"],
                "actual_cnt":    record["actual_cnt"],
                "predicted_cnt": pred,
                "abs_error":     error,
            }
            print(f"[Processor] row={output['row_id']:>5} "
                  f"actual={output['actual_cnt']:>4} "
                  f"predicted={output['predicted_cnt']:>7.1f} "
                  f"error={output['abs_error']:>6.1f}")

            yield json.dumps(output).encode("utf-8")

        except Exception as e:
            print(f"[Processor] Error: {e}")
