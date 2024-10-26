import time
from flask import Flask, jsonify, request, Response
import boto3
import botocore
import uuid
import base64
import math
import threading
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

REGION_CODE = "us-east-1"
S3_SERVICE = "s3"
SQS_SERVICE = "sqs"

IMAGE_AMI = "ami-06f7eadf16ba71dbc"
INSTANCE_MODEL = "t2.micro"
INPUT_BUCKET = "1230402773-in-bucket"
OUTPUT_BUCKET = "1230402773-out-bucket"
REQ_QUEUE = "https://sqs.us-east-1.amazonaws.com/710946350960/1230402773-req-queue"
RESP_QUEUE = "https://sqs.us-east-1.amazonaws.com/710946350960/1230402773-resp-queue"
MAX_RUNNING_INSTANCES = 20

status_100 = None
status_1000 = None
output_results = {}


s3_client = None
bucket_in = INPUT_BUCKET
bucket_out = OUTPUT_BUCKET
sqs_client = None
queue_req_link = None
queue_resp_link = None
ec2_client = None

instance_msg_map = {}

def count_queue_messages(queue_url):
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])


def scaling_control():
    active_instance_count = 0
    live_instances = []
    backoff_rate = 0

    while True:
        queue_msg_count = count_queue_messages(queue_req_link)
        print(f"Queue contains {queue_msg_count} messages")

        needed_instances = min(20, math.ceil((20 * queue_msg_count) / 50))

        if active_instance_count < needed_instances:
            new_instances = 2 ** backoff_rate
            
            if new_instances > (needed_instances - active_instance_count):
                new_instances = needed_instances - active_instance_count
            
            launched_instances = initiate_instances(new_instances)
            live_instances.extend(launched_instances)
            active_instance_count += new_instances
            print(f"Started {new_instances} instances, currently running: {active_instance_count}")

            backoff_rate += 1
        elif active_instance_count > needed_instances:
            print("DECREASING INSTANCES", "RUNNING COUNT", "REQUIRED", "BACKOFF INDEX")
            print(f"\t{active_instance_count}\t{needed_instances}\t{backoff_rate}")
            terminate_list = []

            terminate_instances = min(2 ** backoff_rate, active_instance_count - needed_instances)
            terminate_list = live_instances[-terminate_instances:]

            if terminate_list:
                remove_instances(terminate_list)
                live_instances = [inst for inst in live_instances if inst not in terminate_list]
                active_instance_count -= len(terminate_list)
                print(f"Removed {len(terminate_list)} instances, currently running: {active_instance_count}")

            backoff_rate = max(0, backoff_rate - 1)

        time.sleep(30)


def create_ec2_client():
    global ec2_client
    ec2_client = boto3.client(
        "ec2", 
        region_name=REGION_CODE,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS
    )

create_ec2_client()

def assign_instance_name(instance_id, name_tag):
    ec2_client.create_tags(
        Resources=[instance_id],
        Tags=[
            {
                'Key': 'Name',
                'Value': name_tag
            }
        ]
    )


def initiate_instances(num_instances):
    script = """#!/bin/bash
    cd /home/ubuntu/
    source env/bin/activate
    nohup python3 appserver.py &
    """
    print("INITIATING INSTANCE")
    try:
        instances = ec2_client.run_instances(
            ImageId=IMAGE_AMI,
            InstanceType=INSTANCE_MODEL,
            MinCount=num_instances,
            MaxCount=num_instances,
            UserData=base64.b64encode(script.encode()).decode(),
            # KeyName = "web-tier-key",
            KeyName = "project2-part2-saketh",
            # SecurityGroupIds=['sg-0f506c2c4ba85936b'],
            SecurityGroupIds=['sg-091e892fbf8d7c65b'],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags' : [{
                'Key': 'Name',
                'Value': 'ChildInstance'
                    }]
                }]
        )
        print(f"Initiated {num_instances} instances: {instances['Instances']}")
        instance_ids = [instance.get("InstanceId") for instance in instances.get("Instances", [])]

        if instance_ids is None:
            print("No instances initiated.")
            return []
        
        for instance_id in instance_ids:
            print("INSTANCE ID-", instance_id)
            assign_instance_name(instance_id=instance_id, name_tag= f"app-tier-instance-{instance_id}")
        
        return instance_ids
    except Exception as error:
        print(f"Error initiating instances: {error}")
        return None

def remove_instances(instance_ids):
    if instance_ids:
        ec2_client.terminate_instances(InstanceIds=instance_ids)
        print(f"Removed instances: {instance_ids}")

def create_s3_client():
    global s3_client
    s3_client = boto3.client(
        S3_SERVICE,
        region_name=REGION_CODE,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS
    )

def verify_s3_bucket(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' is present.")
        return True
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == '404':
            print(f"Bucket '{bucket_name}' is absent.")
            return False
        else:
            print(f"Error during bucket verification: {error}")
            return False

def setup_s3_bucket(bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    except Exception as error:
        print(f"Error creating bucket '{bucket_name}': {error}")

def create_sqs_client():
    global sqs_client
    sqs_client = boto3.client(
        SQS_SERVICE,
        region_name=REGION_CODE,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS
    )

def verify_sqs_queue(queue_name):
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        print(f"Queue '{queue_name}' is present. URL: {response['QueueUrl']}")
        return response['QueueUrl']
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            print(f"Queue '{queue_name}' is absent.")
            return None
        else:
            print(f"Error verifying queue: {error}")
            return None

def create_sqs_queue(queue_name):
    try:
        response = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={}
        )
        print(f"Queue '{queue_name}' created.")
        return response['QueueUrl']
    except Exception as error:
        print(f"Error creating queue '{queue_name}': {error}")
        return None

create_s3_client()
create_sqs_client()

queue_req_link = verify_sqs_queue('1230402773-req-queue') or create_sqs_queue('1230402773-req-queue')
queue_resp_link = verify_sqs_queue('1230402773-resp-queue') or create_sqs_queue('1230402773-resp-queue')

if not verify_s3_bucket(bucket_in):
    setup_s3_bucket(bucket_in)

if not verify_s3_bucket(bucket_out):
    setup_s3_bucket(bucket_out)

scaling_thread = threading.Thread(target=scaling_control, daemon=True)
scaling_thread.start()

@app.route('/test')
def home():
    return jsonify({"message": "Hello, World"})

@app.route('/', methods=['POST'])
def upload_image():
    if 'inputFile' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    input_file = request.files['inputFile']

    if input_file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    file_name = input_file.filename
    print(file_name)

    request_uuid = str(uuid.uuid4())

    # Send message to request queue
    try:
        sqs_client.send_message(
            QueueUrl=queue_req_link,
            MessageBody=f"{request_uuid}:{file_name}"
        )
        print("Message successfully sent to request queue.")
    except Exception as error:
        print("Error sending message to SQS:", error)
        return jsonify({"error": "Error sending message to request queue"}), 500
    
     # Upload image to input S3 bucket
    try:
        s3_client.put_object(Bucket=bucket_in, Key=file_name, Body=input_file)
        print(f"File {file_name} uploaded to '{bucket_in}'")
    except Exception as error:
        print(f"Error uploading file: {error}")
        return jsonify({"error": "Error uploading file"}), 500

        # Poll the response queue for the matching request UUID
    try:
        
        while True:
            sqs_response = sqs_client.receive_message(
                QueueUrl=queue_resp_link,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=5,
                VisibilityTimeout=30
            )

            # If a message is found in the response queue
            if 'Messages' in sqs_response and len(sqs_response['Messages']) > 0:
                sqs_message = sqs_response['Messages'][0]
                req_response_id = sqs_message["Body"].split(":")[0]

                # Check if this is the response for the current request UUID
                if req_response_id == request_uuid:
                    message_details = sqs_message["Body"].split(":")[1:3]
                    final_output = ":".join(message_details)

                    # Delete the message from the response queue after reading it
                    sqs_client.delete_message(
                        QueueUrl=queue_resp_link,
                        ReceiptHandle=sqs_message['ReceiptHandle']
                    )
                    
                    print(f"Retrieved response for request {request_uuid}: {final_output}")

                    # Return the output as plain text
                    return Response(final_output, status=200, mimetype='text/plain')

            else:
                # Continue polling until a matching message is found
                time.sleep(1)

    except Exception as error:
        print(f"Error retrieving response from response queue: {error}")
        return jsonify({"error": "Error retrieving response"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000,threaded=True)