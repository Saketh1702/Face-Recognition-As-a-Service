import boto3

# AWS SQS client
sqs = boto3.client('sqs', region_name='us-east-1')

# Queue URL
queue_url = 'https://sqs.us-east-1.amazonaws.com/710946350960/1230402773-req-queue'
# queue_url = 'https://sqs.us-east-1.amazonaws.com/710946350960/1230402773-resp-queue'

# Keep receiving and deleting messages until the queue is empty
while True:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # Receive up to 10 messages at a time
        WaitTimeSeconds= 2
    )
    
    messages = response.get('Messages', [])
    if not messages:
        print("Queue is empty.")
        break

    for message in messages:
        # Deleting the message after processing
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        print(f"Deleted message: {message['MessageId']}")
