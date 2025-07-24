import boto3
import csv
import io
from datetime import datetime, timedelta

# Initialize the boto3 client for CloudWatch Logs and S3
cloudwatch_logs_client = boto3.client('logs')
s3_client = boto3.client('s3')

# Configuration
log_group_name = '/aws-glue/jobs/logs-v2'  # Replace with your CloudWatch Log Group
s3_bucket_name = 'cloudwatchlogde'  # Replace with your S3 bucket name
s3_key_prefix = 'cloudwatch-logs/'  # S3 key prefix (folder structure in S3)
start_time = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)  # Start time in milliseconds
end_time = int(datetime.now().timestamp() * 1000)  # End time in milliseconds

def export_logs_to_s3():
    # Create an in-memory buffer for CSV data
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    
    # Write CSV header
    csv_writer.writerow(['timestamp', 'message', 'logStream'])
    
    next_token = None
    while True:
        # Prepare the parameters for the request
        params = {
            'logGroupName': log_group_name,
            'startTime': start_time,
            'endTime': end_time
        }
        
        # Include the nextToken parameter only if it's not None
        if next_token:
            params['nextToken'] = next_token
        
        # Fetch logs from CloudWatch Logs
        response = cloudwatch_logs_client.filter_log_events(**params)
        
        # Write each log event to the CSV buffer
        for event in response['events']:
            timestamp = event['timestamp']
            message = event['message']
            log_stream = event['logStreamName']
            csv_writer.writerow([timestamp, message, log_stream])
        
        next_token = response.get('nextToken')
        if not next_token:
            break
    
    # Save the CSV data to S3
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{s3_key_prefix}{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_cloudwatch_logs.csv",
        Body=csv_buffer.getvalue()
    )

    csv_buffer.close()
    print(f"Logs exported to s3://{s3_bucket_name}/{s3_key_prefix}{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_cloudwatch_logs.csv")

# Run the function
export_logs_to_s3()
