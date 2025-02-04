import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

class DeleteResource:
    def __init__(self):
        # load environment variables
        self.bucket_name = "bai-xu-sports-analytics-data-lake"
        self.glue_database_name = "xu-glue_nba_data_lake"
        self.region = os.getenv('AWS_DEFAULT_REGION')

        # create clients
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.glue_client = boto3.client('glue', region_name=self.region)

    def does_bucket_exist(self) -> bool:
        """Check if the bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except Exception as e:
            print(f'Bucket does not exist: {e}')
            return False

    def delete_bucket(self):
        """Delete an S3 bucket and its contents"""
        # check if bucket exists -> if it does do nothing
        if not self.does_bucket_exist():
            print(f"Bucket {self.bucket_name} does not exist.")
            return
        
        try:
            print(f'Deleting bucket: {self.bucket_name} its contents')

            objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

            if 'Contents' in objects:
                for obj in objects['Contents']:
                    self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
                    print(f"Object {obj['Key']} deleted successfully")
            else:
                print(f"No objects found in bucket {self.bucket_name}.")

            # delete bucket
            self.s3_client.delete_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} deleted successfully.")
        except ClientError as e:
            print(f"Error deleting bucket {self.bucket_name}: {e}" )

    def does_glue_database_exist(self) -> bool:
        """Check if Glue database exists"""
        try:
            self.glue_client.get_database(Name=self.glue_database_name)
            return True
        except Exception as e:
            print(f'Glue database does not exist: {e}')
            return False

    def delete_glue_database(self):
        """Delete a Glue database and its tables"""
        if not self.does_glue_database_exist():
            print(f"Glue database {self.glue_database_name} does not exist.")
            return

        try:
            print(f'Deleting Glue database: {self.glue_database_name} and its tables')
            tables = self.glue_client.get_tables(DatabaseName=self.glue_database_name)['TableList']
            for table in tables:
                table_name = table['Name']
                self.glue_client.delete_table(DatabaseName=self.glue_database_name, Name=table_name)
                print(f"Table {table_name} deleted successfully.")

            self.glue_client.delete_database(Name=self.glue_database_name)
            print(f"Database {self.glue_database_name} deleted successfully.")
        except ClientError as e:
            print(f"Error deleting database {self.glue_database_name}: {e}")

    def delete_athena_query_results(self):
        """Delete Athena query results"""
        try:
            print(f"Deleting Athena query results in bucket: {self.bucket_name}")
            objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix="athena-results/")
            if "Contents" in objects:
                for obj in objects["Contents"]:
                    self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])
                    print(f"Deleted Athena query result: {obj['Key']}")
        except ClientError as e:
            print(f"Error deleting Athena query results in bucket {self.bucket_name}: {e}")

    def delete_resource(self):
        print('Deleting resources created earlier...')
        print('-' * 40, '\n')
        self.delete_glue_database()
        print('-' * 40, '\n')
        self.delete_athena_query_results()
        print('-' * 40, '\n')
        self.delete_bucket()


if __name__ == "__main__":
    remove = DeleteResource()
    remove.delete_resource()