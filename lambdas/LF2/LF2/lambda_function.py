import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import random

REGION = 'us-east-1'
INDEX = 'restaurants'

# Resource URLs
ES_HOST = 'search-restaurants-s5vhrrhqfgjrvnpttcolyhvosq.us-east-1.es.amazonaws.com'
SQS_QUEUE_HOST = 'https://sqs.us-east-1.amazonaws.com/957008505873/sqs_a1_dining'

# Clients
sqs = boto3.client('sqs')
sns = boto3.client('sns')


def get_restaurants_table():
    cred = boto3.Session().get_credentials()
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    return table 
    

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    auth = AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
    return auth


def get_random_restaurant_id(cuisine):
    """Fetches restaurants for a given cuisine type and returns a random one"""
    query = {"query": {"match": {"category": cuisine}}, "size": 10}

    es_client = OpenSearch(hosts=[{
        'host': ES_HOST,
        'port': 443
    }],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)

    response = es_client.search(index="restaurants", body=query)

    results = response['hits']['hits']
    print(results)
    if not results:
        return None
    else:
        restaurant_options = [hit['_source']['businessID'] for hit in results]
        
        # Ensure at least 3 restaurants are available for recommendation
        if len(restaurant_options) < 3:
            print("Less than 3 restaurants found for the given cuisine. Returning all available options.")
            return restaurant_options
        else:
            # Select 3 random non-repeating business IDs
            restaurant_recommendations = random.sample(restaurant_options, 3)
            return restaurant_recommendations


def get_restaurant_info(restaurant_id):
    """Looks up the restaurant data in DynamoDB using restaurant ID"""
    table = get_restaurants_table()
    response = table.get_item(
        Key={
            'businessID': restaurant_id
        }
    )
    return response.get('Item')


def format_message(restaurant_info, restaurant_ids, num_people, date, time):
    """Formats text to user with required restaurant details"""
    cuisine_type = ''
    restaurant_details = []
    for restaurant_id in restaurant_ids:
        restaurant_data = restaurant_info[restaurant_id]
        res_str = "{n}. {name}, located at {address}. ".format(n=str(len(restaurant_details)+1), name=restaurant_data['name'], address=restaurant_data['address'])
        if cuisine_type == '':
            cuisine_type = restaurant_data['category']
        restaurant_details.append(res_str)

    message_str = "Hello! Here are my {cuisine} restaurant suggestions for {n} people, for {d} at {t}: ".format(cuisine=cuisine_type, n=num_people, d=date, t=time)
    for recommendation in restaurant_details:
        message_str += recommendation
    message_str += "Enjoy your meal!"
    return message_str


def send_sms(phone_number, message):
    """Sends text (note: might need to replace with SES)"""
    sns.publish(
        PhoneNumber=phone_number,
        Message=message
    )
    
    
def send_email(email_address, message_body):
    """Sends an email using AWS SES"""
    ses = boto3.client('ses')
    
    response = ses.send_email(
        Source='conciergedining@gmail.com',
        Destination={
            'ToAddresses': [email_address],
        },
        Message={
            'Subject': {
                'Data': 'Dining Concierge: Your Restaurant Recommendations',
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': message_body,
                    'Charset': 'UTF-8'
                }
            }
        }
    )
    return response


def get_date_time(sqs_datetime):
    date_time_tokens = sqs_datetime.split(' ')
    date = date_time_tokens[0]
    time = date_time_tokens[1]
    return date, time


def lambda_handler(event, context):

    # Pull message from the SQS queue.
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_HOST,
        MaxNumberOfMessages=1
    )
    # UNCOMMENT FOR ACTUAL IMPLEMENTATION (this is just for testing)
    #messages = response.get('Messages')
    #if not messages:
        #return "No messages in the SQS queue."
    #print(f"messages: {messages}")
    
    # Get first message
    #message = messages[0]
    # Test case only: get rid of this later
    message = event

    sqs_message_attributes = message['Records'][0]['messageAttributes']
    email_address = sqs_message_attributes['Email']['StringValue']
    phone_number = sqs_message_attributes['PhoneNumber']['StringValue']
    cuisine = sqs_message_attributes['Cuisine']['StringValue']
    num_people = sqs_message_attributes['NumberOfPeople']['StringValue']
    time = sqs_message_attributes['DiningTime']['StringValue']
    date = sqs_message_attributes['Date']['StringValue']
    
    # Get one random restaurant recommendation
    #restaurant_ids = get_random_restaurant_id(message_body['Cuisine'])
    restaurant_ids = get_random_restaurant_id(cuisine)
    print(restaurant_ids)
    if not restaurant_ids:
        print("No restaurants found for the given cuisine.")

    # Retrieve rest of restaurant details from DynamoDB table using ID as key
    restaurant_list = {}
    for restaurant_id in restaurant_ids:
        restaurant_info = get_restaurant_info(restaurant_id)
        restaurant_list[restaurant_id] = restaurant_info
    
    if restaurant_list:
        print(restaurant_list)

        # Format text to the user
        message = format_message(restaurant_list, restaurant_ids, num_people, date, time)
        if email_address:
            send_email(email_address, message)
        # Send SMS message
        #elif phone_number:
            # send_sms(message_body['PhoneNumber'], sms_message)