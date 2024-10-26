import time
from flask import jsonify
import boto3
import botocore
import uuid
from face_recognition import face_match
import subprocess
import requests

REGION = "us-east-1"
S3_RESOURCE_TYPE = "s3"
SQS_RESOURCE_TYPE = "sqs"

IMAGE_AMI = "ami-0d61bbe8408bfa411"
INSTANCE_MODEL = "t2.micro"
INPUT_BUCKET_NAME = "1230402773-in-bucket"
OUTPUT_BUCKET_NAME = "1230402773-out-bucket"
REQ_QUEUE = "https://sqs.us-east-1.amazonaws.com/710946350960/1230402773-req-queue"
RESP_QUEUE = "https://sqs.us-east-1.amazonaws.com/710946350960/1230402773-resp-queue"
MAX_RUNNING_INSTANCES = 20

s3_client = None
bucket_input = INPUT_BUCKET_NAME
bucket_output = OUTPUT_BUCKET_NAME
sqs_client = None
queue_req_url = None
queue_resp_url = None


# Initialize S3 resource
def initialize_s3():
    global s3_client
    s3_client = boto3.client(
        S3_RESOURCE_TYPE,
        region_name=REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

# Initialize SQS resource
def initialize_sqs():
    global sqs_client
    sqs_client = boto3.client(
        SQS_RESOURCE_TYPE,
        region_name=REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

# Create S3 bucket
def create_input_bucket():
    global bucket_input
    bucket_input = s3_client.create_bucket(Bucket=bucket_input)

def create_output_bucket():
    global bucket_output
    bucket_output = s3_client.create_bucket(Bucket=bucket_output)


# Check if bucket exists
def verify_bucket_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
        return True
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == '404':
            print(f"Bucket '{bucket_name}' does not exist.")
            return False
        else:
            print(f"Error checking bucket: {error}")
            return False

# Create SQS request queue
def create_request_queue():
    global queue_req_url
    response = sqs_client.create_queue(
        QueueName='1230402773-req-queue', 
        Attributes={}
    )
    queue_req_url = response.get("QueueUrl")


# Create SQS response queue
def create_response_queue():
    global queue_resp_url
    response = sqs_client.create_queue(
        QueueName='1230402773-resp-queue',  
        Attributes={}
    )
    queue_resp_url = response.get("QueueUrl")

# Check if queue exists
def verify_queue_exists(queue_name):
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        print(f"Queue '{queue_name}' exists. URL: {response['QueueUrl']}")
        return response['QueueUrl']
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            print(f"Queue '{queue_name}' does not exist.")
            return None
        else:
            print(f"Error verifying queue: {error}")
            return None


# Retrieve S3 object
def retrieve_s3_object(bucket_name, object_key, download_path):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        with open(f'{download_path}/{object_key}', 'wb') as file_obj:
            file_obj.write(response['Body'].read())
        print(f"Object '{object_key}' downloaded from bucket '{bucket_name}' to '{download_path}/{object_key}'")
    except Exception as error:
        print(f"Error retrieving object: {error}")

# Store text in S3 bucket
def save_text_to_s3(bucket_name, object_key, text_data):
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=text_data)
        print(f"Text data uploaded to '{bucket_name}/{object_key}' successfully.")
    except Exception as error:
        print(f"Error saving text to S3: {error}")

initialize_s3()

if not verify_bucket_exists(bucket_input):
    create_input_bucket()
if not verify_bucket_exists(bucket_output):
    create_output_bucket()


# Prediction function
def perform_image_prediction():
    image_name = None
    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_req_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            VisibilityTimeout=60
        )
        
        if 'Messages' in response and len(response['Messages']) > 0:
            request_id = response['Messages'][0]['Body'].split(":")[0]
            image_name = response['Messages'][0]['Body'].split(":")[1]
            print(f"Received image name: {image_name}")
            
            # Download image from S3
            download_path = "./images"
            retrieve_s3_object(bucket_input, image_name, download_path)
            time.sleep(5)
            
            # Predict using face_match function
            # Use the image path where you downloaded the image.
            result = face_match(f'{download_path}/{image_name}', 'data.pt')
            
            if result is None:
                return jsonify({"message": "No matching data found in the CSV"}), 200

            # Store the result in the output bucket
            save_text_to_s3(bucket_output, image_name[:-4], result[0])

            # Send result to response SQS queue (remove FIFO-specific parameters)
            sqs_client.send_message(
                QueueUrl=queue_resp_url,
                MessageBody=f"{request_id}:{image_name[:-4]}:{result[0]}"
            )
            sqs_client.delete_message(
                QueueUrl=queue_req_url,
                ReceiptHandle=response['Messages'][0]['ReceiptHandle']
            )
        time.sleep(5)

initialize_sqs()


queue_req_url = verify_queue_exists('1230402773-req-queue')
if not queue_req_url:
    create_request_queue()

queue_resp_url = verify_queue_exists('1230402773-resp-queue')
if not queue_resp_url:
    create_response_queue()

if __name__ == '__main__':
    perform_image_prediction()