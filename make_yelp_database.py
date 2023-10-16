"""
Script to perform DynamoDB operations, including making a DynamoDB table and populating it with
data from a CSV file. This was used to create the yelp-restaurants table and populate it with data in
restaurant_data.csv, which was obtained using yelp_restaurant_scraper.py.
"""

import boto3
import csv
import argparse
import sys
from datetime import datetime

# Define credentials and region: replace with real credentials from AWS!
aws_access_key_id = ''
aws_secret_access_key = ''
region_name = 'us-east-1'

def parse_args(args=None):
    Description = ("Script to create DynamoDB table restaurants and populate it with restaurant data CSV containing "
                   "restaurant data scraped from Yelp.")
    Epilog = "Example usage: python3 make_yelp_database.py <CSV_PATH> <TABLE_NAME> <PARTITION_KEY>"
    parser = argparse.ArgumentParser(description=Description, epilog=Epilog)
    parser.add_argument('CSV_PATH', metavar='-c', type=str, help='Path to CSV file containing data to '
                                                                 'be written to the DynamoDB table.')
    parser.add_argument('TABLE_NAME', metavar='-n', type=str, help='Name for DynamoDB table.')
    parser.add_argument('PARTITION_KEY', metavar = '-k', type=str, help='Name of partition key variable.')
    return parser.parse_args(args)


def make_dynamo_db_table(dynamodb, table_name, partition_key):
    """Creates a DynamoDB table with given client and specified name"""
    # Create the table: use timestamp as key as directed with businessID
    try:
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': partition_key,
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': partition_key,
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


def insert_data_into_dynamodb(aws_access_key_id, aws_secret_access_key, region_name, table_name, data):
    """Adds data row by row to database"""
    dynamodb = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    # Write all row data to the instantiated DynamoDB table
    table = dynamodb.Table(table_name)
    for item in data:
        now = datetime.now()
        # Include time stamp of when item was created
        item['insertedAtTimestamp'] = int(now.strftime("%Y%m%d%H%M%S"))
        table.put_item(Item=item)


def make_populate_dynamo_table(csv_file, table_name, partition_key):
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=region_name)

    # Convert CSV data to a list of dictionaries
    data_to_insert = csv_to_dict_list(csv_file)

    # Create table
    make_dynamo_db_table(dynamodb, table_name, partition_key)

    # Insert the data into the DynamoDB table
    insert_data_into_dynamodb(aws_access_key_id, aws_secret_access_key, region_name, table_name, data_to_insert)

    print("Data insertion completed.")


def main(args=None):
    args = parse_args(args)
    make_populate_dynamo_table(args.CSV_PATH, args.TABLE_NAME, args.PARTITION_KEY)


if __name__ == '__main__':
    sys.exit(main())