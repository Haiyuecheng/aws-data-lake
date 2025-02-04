import os
import time

import requests

import boto3
import json
from dotenv import load_dotenv

load_dotenv()


class SetupResource:
    def __init__(self, ):
        # load environment variables
        self.bucket_name = "bai-xu-sports-analytics-data-lake"
        self.glue_database_name = "xu-glue_nba_data_lake"
        self.glue_table_name="nba_players"
        self.athena_output_location = f"s3://{self.bucket_name}/athena-results/"
        self.region = os.getenv('AWS_DEFAULT_REGION')
        self.nba_url = 'https://api.sportsdata.io/v3/nba/scores/json/Players'
        self.sport_data_api_key = os.getenv("SPORTS_DATA_API_KEY")

        # create clients
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.glue_client = boto3.client('glue', region_name=self.region)
        self.athena_client = boto3.client('athena', region_name=self.region)

    def does_bucket_exist(self) -> bool:
        """Check if the bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except Exception as e:
            print(f'Bucket does not exist: {e}')
            return False

    def create_bucket(self) -> None:
        """Create an S3 bucket"""
        try:
            # check if bucket exists -> if it does do nothing
            if self.does_bucket_exist():
                print(f'Bucket: {self.bucket_name} already exists.')
                return None

            # create bucket
            self.s3_client.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.region
                }
            )
            print("S3 bucket created successfully.")
        except Exception as e:
            print(f'Error while creating bucket: {e}')

    def does_glue_database_exist(self) -> bool:
        """Check if Glue database exists"""
        try:
            self.glue_client.get_database(Name=self.glue_database_name)
            return True
        except Exception as e:
            print(f'Glue database does not exist: {e}')
            return False

    def create_glue_database(self)-> None:
        """Create a Glue database"""
        try:
            # check if the glue database exists -> if it does do nothing
            if self.does_glue_database_exist():
                print(f'Glue database: {self.glue_database_name} already exists.')
                return None

            self.glue_client.create_database(
                DatabaseInput={
                    'Name': self.glue_database_name,
                    'Description': 'Glue database for NBA player data analytics',
                }
            )
            print(f"Glue database ({self.glue_database_name}) created successfully.")
        except Exception as e:
            print(f'Error while creating Glue database: {e}')

    def create_glue_table(self):
        """Create a Glue table"""
        try:
            self.glue_client.create_table(
                DatabaseName=self.glue_database_name,
                TableInput={
                    'Name': self.glue_table_name,
                    'Description': 'NBA players data',
                    'StorageDescriptor': {
                        "Columns": [
                            {"Name": "PlayerID", "Type": "int"},
                            {"Name": "FirstName", "Type": "string"},
                            {"Name": "LastName", "Type": "string"},
                            {"Name": "Team", "Type": "string"},
                            {"Name": "Position", "Type": "string"},
                            {"Name": "Points", "Type": "int"}
                        ],
                        "Location": f"s3://{self.bucket_name}/raw-data/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                        },
                    },
                    "TableType": "EXTERNAL_TABLE",
                },
            )
            print(f"Glue table {self.glue_table_name} created successfully.")
        except Exception as e:
            print(f"Error creating Glue table: {e}")

    @staticmethod
    def convert_to_line_delimited_json(data):
        """Convert JSON data to line delimited JSON"""
        return '\n'.join([json.dumps(record) for record in data])

    def fetch_nba_data(self):
        """Fetch NBA data from SportsData API"""
        try:
            print('Fetching NBA players data...')
            response = requests.get(
                self.nba_url,
                headers={
                     'Ocp-Apim-Subscription-Key': self.sport_data_api_key
                }
            )
            response.raise_for_status()
            print('NBA players data fetched successfully.')
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f'Error while fetching loading nba players data: {e}')
            return None

    def upload_to_s3(self, data) -> bool:
        """Save JSON file to S3 bucket"""
        try:
            # check if bucket exists -> if it does do nothing
            if not self.does_bucket_exist():
                return False

            if not data:
                return False

            # define a file key for s3
            key = "raw-data/nba_player_data.json"

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=self.convert_to_line_delimited_json(data),
                ContentType='application/json'
            )

            print(f'File: {key} uploaded successfully to S3!!!')
            return True
        except Exception as e:
            print(f'Error while saving to S3: {e}')
            return False

    def configure_athena(self):
        """Set up Athena output location."""
        try:
            self.athena_client.start_query_execution(
                QueryString='CREATE DATABASE IF NOT EXISTS nba_analytics',
                QueryExecutionContext={"Database": self.glue_database_name},
                ResultConfiguration={"OutputLocation": self.athena_output_location},
            )
            print("Athena output location configured successfully.")
        except Exception as e:
            print(f"Error configuring Athena: {e}")
        pass

    def setup_resource(self):
        """Setup resources for the data lake."""
        print('Setting up resources for the data lake...')
        self.create_bucket()
        print('-' * 40, '\n')

        # adding a delay to ensure previous code has run successfully (with exit 0)
        time.sleep(5)
        self.create_glue_database()
        print('-' * 40, '\n')

        nba_data = self.fetch_nba_data()
        if nba_data:
            self.upload_to_s3(nba_data)
        print('-' * 40, '\n')

        self.create_glue_table()
        print('-' * 40, '\n')

        self.configure_athena()
        print('Data lake setup completed successfully.')

if __name__ == "__main__":
    setup = SetupResource()
    setup.setup_resource()