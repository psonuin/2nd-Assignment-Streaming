import pandas as pd
import numpy as np
import boto3
import json
from datetime import datetime, timedelta
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Set seed for reproducibility
np.random.seed(42)

# Initialize the S3 client
s3 = boto3.client('s3')

# Your S3 bucket name and folder
bucket_name = 'data-stream-practice'
folder_name = 'iot-sensor-data/'

def generate_sensor_data(force_failure=False, base_time=None):
    # Generate a single timestamp
    if base_time is None:
        base_time = datetime.now()
    timestamp = base_time + timedelta(seconds=np.random.randint(1, 10))  # Add a small random increment to simulate real-time data

    if force_failure:
        # Generate sensor data that ensures a failure status
        temperature = np.random.normal(loc=90, scale=5)  # High temperature readings (°C)
        vibration = np.random.normal(loc=30, scale=5)    # High vibration levels (mm/s)
        pressure = np.random.normal(loc=4, scale=0.5)    # High pressure readings (bar)
    else:
        # Generate normal sensor data
        temperature = np.random.normal(loc=75, scale=10)  # Temperature readings (°C)
        vibration = np.random.normal(loc=20, scale=5)     # Vibration levels (mm/s)
        pressure = np.random.normal(loc=3, scale=0.5)     # Pressure readings (bar)

    # Generate Failure Status
    failure_status = int(
        (temperature > 85) & 
        (vibration > 25) & 
        (pressure > 3.5)
    )

    # Create a dictionary with sensor data
    sensor_data = {
        "Timestamp": timestamp.isoformat(),
        "Temperature": temperature,
        "Vibration": vibration,
        "Pressure": pressure,
        "Failure Status": failure_status
    }
    return sensor_data

# Lists to hold generated data
failure_data = []
non_failure_data = []

# Generate sensor data entries
base_time = datetime.now()  # Set a base time for all timestamps
for i in range(100):
    # Decide whether to force a failure or not
    force_failure = len(failure_data) < 25  # Force failure until we have 25 failures

    # Generate sensor data
    data = generate_sensor_data(force_failure=force_failure, base_time=base_time)
    
    # Separate failure and non-failure data
    if data["Failure Status"] == 1:
        failure_data.append(data)
    else:
        non_failure_data.append(data)

# Balance the dataset
if len(failure_data) < 50:
    # Oversample failure data
    failure_data = failure_data * (50 // len(failure_data)) + failure_data[:50 % len(failure_data)]
else:
    # Ensure exactly 50 failure records
    failure_data = failure_data[:50]

if len(non_failure_data) > 50:
    # Undersample non-failure data
    non_failure_data = non_failure_data[:50]

# Combine the balanced data
balanced_data = failure_data + non_failure_data

# Sort the data by timestamp to maintain ascending order
balanced_data = sorted(balanced_data, key=lambda x: x["Timestamp"])

# Stream balanced data into S3
for i, data in enumerate(balanced_data):
    # Generate a unique file name for each record
    file_name = f"{folder_name}sensor_data_{i+1}.json"

    try:
        # Upload the JSON string as a file to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(data))
        logger.info(f"Uploaded {file_name} to {bucket_name}")
    except Exception as e:
        logger.error(f"Error uploading data: {e}")

    # Simulate real-time streaming by waiting for a short interval
    time.sleep(5)  # Adjust the sleep time as needed

print("Streaming completed.")
