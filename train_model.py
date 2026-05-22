"""
train_model.py  —  Run this once to train and save the model
Usage: python train_model.py
"""
import numpy as np, joblib, os
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score

os.makedirs("model", exist_ok=True)

FEATURES = ["season","yr","mnth","hr","holiday","weekday",
            "workingday","weathersit","temp","atemp","hum","windspeed"]
TARGET = "cnt"

df = pd.read_csv("hour.csv")
print(f"Loaded {len(df)} rows")

X = df[FEATURES].values
y = df[TARGET].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s  = scaler.transform(X_test)

print("Training Random Forest...")
model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train_s, y_train)

y_pred   = model.predict(X_test_s)
mae      = mean_absolute_error(y_test, y_pred)
r2       = r2_score(y_test, y_pred)
accuracy = np.mean(np.abs(y_pred - y_test) < 50) * 100

print(f"\nMAE      : {mae:.2f} bikes")
print(f"R²       : {r2:.4f}")
print(f"Accuracy : {accuracy:.1f}% (within ±50 bikes)")

joblib.dump(model,  "model/bike_model.joblib")
joblib.dump(scaler, "model/scaler.joblib")
print("\n✓ Model saved to model/bike_model.joblib")
print("✓ Scaler saved to model/scaler.joblib")
