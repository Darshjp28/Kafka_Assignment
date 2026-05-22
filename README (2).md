# ENGR 5785G — Assignment 1: Real-Time Streaming with Apache Kafka

## Dataset
**Bike Sharing (UCI)** — Predict hourly bike rental count  
Source: https://archive.ics.uci.edu/dataset/275

## Streams Library
**Python + Faust** (faust-streaming)

## Project Structure
```
Kafka_Assignment/
├── producer.py              # Reads dataset, publishes to raw-data topic @ 1 row/sec
├── streams_processor.py     # Faust Streams processor — runs ML model, publishes predictions
├── consumer.py              # Reads predictions topic, prints formatted results
├── requirements.txt         # Python dependencies
├── model/
│   ├── bike_model.joblib    # Trained Random Forest model
│   └── scaler.joblib        # StandardScaler
└── ENGR5785G_Assignment1.ipynb  # Full pipeline notebook (Google Colab)
```

## Setup & How to Run

### Step 1 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 2 — Start Kafka
```bash
docker compose up -d
```

### Step 3 — Train the model (already done — model file included)
```bash
python train_model.py
```

### Step 4 — Run all 3 components (open 3 terminals)

**Terminal 1 — Faust Streams Processor:**
```bash
faust -A streams_processor worker -l info
```

**Terminal 2 — Output Consumer:**
```bash
python consumer.py
```

**Terminal 3 — Producer:**
```bash
python producer.py
```

Predictions will appear in Terminal 2 in real time.

## ML Model Performance

| Metric | Value |
|--------|-------|
| Algorithm | Random Forest Regressor (100 trees) |
| Train / Test Split | 80% / 20% |
| MAE | ~28 bikes |
| R² | ~0.95 |
| Accuracy (within ±50 bikes) | ~89% |
| Streams Library | Faust (Python) |

## Pipeline Architecture

```
hour.csv (Bike Sharing Dataset)
      │
      ▼
┌─────────────┐   raw-data topic   ┌──────────────────────┐   predictions topic   ┌─────────────┐
│ producer.py │ ─────────────────▶ │ streams_processor.py │ ────────────────────▶ │ consumer.py │
│             │   JSON @ 1/sec     │   (Faust Streams)    │   JSON + prediction   │  (terminal) │
└─────────────┘                    └──────────────────────┘                        └─────────────┘
                                           ▲
                                           │ loads on startup
                                   model/bike_model.joblib
```

## Video Demo
https://drive.google.com/file/d/1Je_aQXGcruvglD4f2FR8CXuUUa_5_b3N/view?usp=share_link
