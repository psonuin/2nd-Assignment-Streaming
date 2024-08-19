# STREAMING DATA ANALYTICS PIPELINE

## Project Overview
This project builds a streaming data analytics pipeline for simulating Iot sensor data and streaming each data record in JSON format. The pipeline uses AWS services S3, EMR, and CloudWatch. The project includes: <br/>
1. Simulating and Streaming  Iot sensor data into S3 Bucket.<br/>
2. Creating an EMR cluster and submitting the spark-job.<br/>
3. Processing the data by building an ML model using spark-job.<br/>
4. Monitoring the EMR cluster using CloudWatch with custom alarms.<br/>
### Technologies Used
AWS CLI<br/>
AWS S3<br/>
AWS EMR<br/>
AWS CloudWatch<br/>
Python<br/>

### Use Case
The primary use case is to process sensor data records and predict the failure of the Device by using ML Algorithm RandomForest Classifier.

### Prerequisites
Install AWS CLI.
Python environment (VS Code) for server-log data generation, lambda function and etl-job development.

### Implementation (step by step) 
#### DATA INGESTION AND POLICIES:
1. Create IAM user and access keys. Use those keys to configure aws in the CLI using command
   ```
   aws configure
   ```
2. Create roles for EMR and EC2 using TrustPolicyForEMR.json and TrustPolicyForEC2.json files with the commands
```
aws iam create-role --role-name EMR_EC2_DefaultRole --assume-role-policy-document file://TrustPolicyForEC2.json
aws iam create-role --role-name EMR_DefaultRole --assume-role-policy-document file://TrustPolicyForEMR.json
```
3. Attach role policies.
```
aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforSSM
aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name EMR_DefaultRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole
aws iam attach-role-policy --role-name EMR_DefaultRole --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```
4. Create EC2 Instance Profile add roles to it which will be used to create EMR Cluster.
```
aws iam create-instance-profile --instance-profile-name EMR_EC2_DefaultRole
aws iam add-role-to-instance-profile --instance-profile-name EMR_EC2_DefaultRole --role-name EMR_EC2_DefaultRole
```
5. Create an S3 bucket with a proper name.
```
aws s3 mb s3://my-bucket-name
```
6. Write a Streaming.py function to synthetically give data and stream it to S3 bucket into the iot-sensor-data. Run the file at the terminal.
```
python Streaming.py
```
#### EMR CLUSTER CREATION AND DATA PROCESSING:
7. Create an EMR Cluster.
```
aws emr create-cluster --name "IoT Data Cluster" --release-label emr-6.7.0 \
--applications Name=Hadoop Name=Spark --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
--service-role EMR_DefaultRole \
--instance-groups InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=2 --log-uri s3://my-bucket-name/emr-logs/ \
--enable-debugging
```

8. Write a spark-job to process the input data and open a spark session in the file and write code to predict the failure status using RF machine learning model. Upload the script into S3 Bucket using cp command.
9. Now submit the spark-job to the EMR cluster using the command.
```
aws emr add-steps --cluster-id j-XXXXXXXXXXXX \
--steps Type=Spark,Name="Spark Job",ActionOnFailure=CONTINUE,Args='[--deploy-mode,cluster,--master,yarn,s3://my-bucket-name/scripts/spark-job.py]'
```

#### MONITORING AND ALARMS:








