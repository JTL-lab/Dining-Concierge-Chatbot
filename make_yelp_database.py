"""
Script to perform DynamoDB operations.
"""

import boto3
import csv
from datetime import datetime

# Define credentials and region: replace with real credentials from AWS!
aws_access_key_id = ''
aws_secret_access_key = ''
region_name = 'us-east-2'

def make_dynamo_db_table(dynamodb, table_name):
    """Creates a DynamoDB table with given client and specified name"""
    # Create the table: use timestamp as key as directed with businessID
    try:
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'insertedAtTimestamp',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'businessID',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'insertedAtTimestamp',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'businessID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Table created!")
        dynamodb.get_waiter('table_exists').wait(TableName=table_name)

    except dynamodb.exceptions.ResourceInUseException:
        print("Table already exists. Skipping table creation.")


def csv_to_dict_list(csv_file):
    """Function to convert the CSV file to a list of dictionaries"""
    dict_list = []
    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            dict_list.append(row)

    return dict_list


# Insert data into the DynamoDB table
def insert_data_into_dynamodb(aws_access_key_id, aws_secret_access_key, region_name, table_name, data):
    """Adds data row by row to database"""
    dynamodb = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    # Write all row data to instantiated DynamoDB table
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for item in data:
            now = datetime.now()
            item['insertedAtTimestamp'] = int(now.strftime("%Y%m%d%H%M%S"))
            batch.put_item(Item=item)


if __name__ == '__main__':
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=region_name)

    # Define the CSV file path
    csv_file = 'restaurant_data.csv'

    # Define DynamoDB table name
    table_name = 'yelp-restaurants'

    # Convert CSV data to a list of dictionaries
    data_to_insert = csv_to_dict_list(csv_file)

    # Create table
    make_dynamo_db_table(dynamodb, table_name)

    # Insert the data into the DynamoDB table
    insert_data_into_dynamodb(aws_access_key_id, aws_secret_access_key, region_name, table_name, data_to_insert)

    print("Data insertion completed.")
