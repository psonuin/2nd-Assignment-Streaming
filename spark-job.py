from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)

logging.info("Starting Spark Job")
# Initialize Spark session
spark = SparkSession.builder \
    .appName("EMR_Spark_ML_Job") \
    .getOrCreate()
logging.info("Spark Session Created")
# Define S3 paths
input_s3_path = "s3://data-stream-practice/iot-sensor-data/"  # Path to your 100 JSON files
output_s3_path = "s3://data-stream-practice/iot-sensor-data-predictions/"  # Path for saving predictions

# Read all JSON files from the S3 directory into a DataFrame
df = spark.read.json(input_s3_path)
logging.info("df reading Completed")
# Display the schema to understand the structure of the DataFrame
df.printSchema()

# Add row numbers to each record to facilitate splitting
window_spec = Window.orderBy('Timestamp')  # Order by Timestamp or another column
df_with_index = df.withColumn("row_index", row_number().over(window_spec))

# Define split point
split_point = int(df_with_index.count() * 0.8)

# Split data into training and test sets
training_data = df_with_index.filter(df_with_index.row_index <= split_point).drop("row_index")
test_data = df_with_index.filter(df_with_index.row_index > split_point).drop("row_index")

# Convert features to a vector
feature_columns = ['Temperature', 'Vibration', 'Pressure']
assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')

# Initialize the RandomForestClassifier
rf = RandomForestClassifier(labelCol='Failure Status', featuresCol='features')

# Create a pipeline
pipeline = Pipeline(stages=[assembler, rf])

# Train the model
model = pipeline.fit(training_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(labelCol='Failure Status', predictionCol='prediction', metricName='accuracy')
accuracy = evaluator.evaluate(predictions)
print(f"Test Accuracy = {accuracy}")

# Select relevant columns for output
predicted_df = predictions.select('Timestamp', 'Temperature', 'Vibration', 'Pressure', 'Failure Status', 'prediction')

# Save predictions to S3
predicted_df.write.mode('overwrite').json(output_s3_path)

# Stop the Spark session
spark.stop()
