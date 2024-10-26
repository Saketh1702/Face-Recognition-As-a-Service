# Face Recognition As a Service
# Key Features:
AWS EC2 Instances: Handles the computational load for face recognition tasks.
S3 Buckets: Used to store both the input images and the processed results.
Custom Auto-Scaling Algorithm: Automatically scales the number of EC2 instances based on the number of requests in the SQS queue, ensuring cost-effective resource utilization.
SQS Queues: Manages request and response queues for image processing jobs.
Efficient Image Handling: The web tier uploads images to SQS queues, and the app tier processes them, sending back results through the response queue.

# Technologies Used:
AWS EC2
AWS S3
AWS SQS
Python
Custom Auto-Scaling Logic

This repository demonstrates how to build a highly available and scalable face recognition service using cloud-based architecture.
