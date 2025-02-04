import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError

# Load environment variables from .env file
load_dotenv()

# AWS configurations
region = os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-2')  # Default to 'ap-southeast-2' if not set
bucket_name = "bai-xu-sports-analytics-data-lake"  # Change to a unique S3 bucket name
glue_database_name = "xu-glue_nba_data_lake"
glue_table_name = "nba_players"
athena_output_location = f"s3://{bucket_name}/athena-results/"

# Sportsdata.io configurations (loaded from .env)
api_key = os.getenv("SPORTS_DATA_API_KEY")  # Get API key from .env
nba_endpoint = os.getenv("NBA_ENDPOINT")  # Get NBA endpoint from .env

# Print the loaded API key for debugging purposes
print(f"Loaded API Key: {api_key}")

# Create AWS clients
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)

def create_s3_bucket(bucket_name, region):
    """Create an S3 bucket in a specified region"""
    try:
        # Create bucket
        s3_client = boto3.client('s3', region_name=region)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        print(f"Bucket {bucket_name} created successfully.")
    except ClientError as e:
        print(f"Error while creating bucket: {e}")

def create_glue_database():
    """Create a Glue database for the data lake."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for NBA sports analytics.",
            }
        )
        print(f"Glue database '{glue_database_name}' created successfully.")
    except Exception as e:
        print(f"Error creating Glue database: {e}")

def fetch_nba_players(api_key, nba_endpoint):
    """Fetch NBA players data from SportsData.io API"""
    headers = {
        'Ocp-Apim-Subscription-Key': api_key
    }
    print(f"Request Headers: {headers}")  # Print headers for debugging purposes
    try:
        response = requests.get(nba_endpoint, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"Error while fetching NBA players data: {err}")
        return None

def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""
    print("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])

def upload_data_to_s3(data):
    """Upload NBA data to the S3 bucket."""
    try:
        # Convert data to line-delimited JSON
        line_delimited_data = convert_to_line_delimited_json(data)

        # Define S3 object key
        file_key = "raw-data/nba_player_data.jsonl"

        # Upload JSON data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
        )
        print(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")

def create_glue_table():
    """Create a Glue table for the data."""
    try:
        glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": "nba_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Points", "Type": "int"}
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"Glue table 'nba_players' created successfully.")
    except Exception as e:
        print(f"Error creating Glue table: {e}")

def configure_athena():
    """Set up Athena output location."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
        )
        print("Athena output location configured successfully.")
    except Exception as e:
        print(f"Error configuring Athena: {e}")

def query_athena(database, table, output_location):
    """Query data from Athena"""
    query = f'SELECT COUNT(*) FROM "{database}"."{table}";'
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': output_location
            }
        )
        query_execution_id = response['QueryExecutionId']
        print(f"Query Execution ID: {query_execution_id}")

        # Wait for the query to complete
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            query_state = query_status['QueryExecution']['Status']['State']
            if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        if query_state == 'SUCCEEDED':
            result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            return result['ResultSet']['Rows']
        else:
            print(f"Query failed with state: {query_state}")
            return None
    except ClientError as e:
        print(f"Error while querying Athena: {e}")
        return None

# Main workflow
def main():
    print("Setting up data lake for NBA sports analytics...")
    create_s3_bucket(bucket_name, region)
    time.sleep(5)  # Ensure bucket creation propagates
    create_glue_database()
    nba_data = fetch_nba_players(api_key, nba_endpoint)
    if nba_data:  # Only proceed if data was fetched successfully
        upload_data_to_s3(nba_data)
    create_glue_table()
    configure_athena()
    print("Data lake setup complete.")

    # Query Athena
    athena_results = query_athena(glue_database_name, glue_table_name, athena_output_location)
    if athena_results:
        print("Athena query results:")
        for row in athena_results:
            print(row)
    else:
        print("Failed to query Athena.")

if __name__ == "__main__":
    main()