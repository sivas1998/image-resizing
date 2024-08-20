import boto3
import os
import sys
import io
import uuid
from urllib.parse import unquote_plus
from PIL import Image
import PIL.Image
import time
            
# Initialize AWS clients
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Configuration
SOURCE_BUCKET = 'source-img-20'
DESTINATION_BUCKET = 'destination-image-lambda'
SNS_TOPIC_ARN = 'arn:aws:sns:ap-southeast-1:565687073766:Test'
RESIZE_DIMENSIONS = (100, 100)
THRESHOLD_COUNT = 5
THRESHOLD_DURATION = 600  # 10 minutes in seconds

# Global variable to keep track of resized objects
resized_objects = []

def lambda_handler(event, context):
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Download the image from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        image_content = response['Body'].read()
        
        # Open the image using PIL
        image = Image.open(io.BytesIO(image_content))
        
        # Resize the image
        resized_image = image.resize(RESIZE_DIMENSIONS)
        
        # Save the resized image to a buffer
        buffer = io.BytesIO()
        resized_image.save(buffer, format='PNG')
        buffer.seek(0)
        
        # Upload the resized image to the destination bucket
        resized_key = f'resized_{key}'
        s3.put_object(Bucket=DESTINATION_BUCKET, Key=resized_key, Body=buffer)
        
        # Send success notification
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='Image Resized Successfully',
            Message=f'Image {key} has been resized and uploaded to {DESTINATION_BUCKET}/{resized_key}'
        )
        
        # Update resized objects list
        current_time = time.time()
        resized_objects.append(current_time)
        
        # Remove objects older than the threshold duration
        resized_objects[:] = [t for t in resized_objects if current_time - t <= THRESHOLD_DURATION]
        
        # Check if threshold is exceeded
        if len(resized_objects) > THRESHOLD_COUNT:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject='Resizing Threshold Exceeded',
                Message=f'More than {THRESHOLD_COUNT} objects have been resized in the last {THRESHOLD_DURATION/60} minutes.'
            )
        
        return {
            'statusCode': 200,
            'body': 'Image resized and uploaded successfully'
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error processing image: {str(e)}'
        }
