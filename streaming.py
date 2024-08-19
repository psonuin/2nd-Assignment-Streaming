import pandas as pd
import numpy as np
import boto3
import json
from datetime import datetime
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

def generate_sensor_data():
    # Generate a single timestamp (e.g., current time)
    timestamp = datetime.now()

    # Generate sensor data
    temperature = np.random.normal(loc=75, scale=10)  # Temperature readings (°C)
    vibration = np.random.normal(loc=20, scale=5)     # Vibration levels (mm/s)
    pressure = np.random.normal(loc=3, scale=0.5)      # Pressure readings (bar)

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

# Stream data into S3
for i in range(25):  # Generating 100 sensor data entries for demonstration
    # Generate sensor data
    data = generate_sensor_data()
    
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
