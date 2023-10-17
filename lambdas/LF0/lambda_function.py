from datetime import datetime
import json
import logging
from botocore.exceptions import ClientError
import boto3
from lex_utility import elicit_intent_lf0, close


GREETING_MSG = "Hello there! What can I help you with today?"
THANK_YOU_MSG = "You're welcome, come again!"
TABLE_STATE_ACTIVE = 'ACTIVE'

client = boto3.client('lexv2-runtime')
dynamodb = boto3.resource('dynamodb')
table_last_search = dynamodb.Table('last-search')
TABLE_NAME_LAST_SEARCH = 'last-search'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def handle_greeting_intent(intent_request):
    print('@handle_greeting_intent: intent_request', intent_request)
    session_attributes = get_session_attributes(intent_request)
    messages = [{
        "contentType": "PlainText",
        "content": GREETING_MSG
    }]
    resp = elicit_intent_lf0(session_attributes, messages)
    return resp


def handle_thank_you_intent(intent_request):
    print('@handle_thank_you_intent: intent_request', intent_request)
    session_attributes = get_session_attributes(intent_request)
    intent = intent_request['sessionState']['intent']
    intent['state'] = 'Fulfilled'
    messages = [{
        "contentType": "PlainText",
        "content": THANK_YOU_MSG
    }]
    resp = close(session_attributes, intent, messages)
    return resp


def dispatch(intent_request):
    intent_name = intent_request['sessionState']['intent']['name']
    if intent_name == 'GreetingIntent':
        return handle_greeting_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return handle_thank_you_intent(intent_request)


def get_session_attributes(intent_request):
    session_attributes = intent_request.get('sessionState', {}).get('sessionAttributes', {})
    print('@get_session_attributes: session_attributes', session_attributes)
    return session_attributes


def call_lex(msg_from_user):
    # Initiate conversation with Lex
    response_from_lex = client.recognize_text(
            botId='QD66RYOWG0', # MODIFY HERE
            botAliasId='IGKQTXZN9N', # MODIFY HERE
            localeId='en_US',
            # sessionId=session_id,
            sessionId='test_session',
            text=msg_from_user)
    print(f'@call_lex: response_from_lex: {response_from_lex}')
    return response_from_lex


def lambda_handler(event, context):
    try:
        print('@lambda_handler: event', event)
        msg_from_user = ''
        messages = event['messages']
        print(f"Message from frontend: {messages}")

        for message in messages:
            print('@lambda_handler: message', message)
            msg_from_user += message['unstructured']['text']
            print(f'MSG_FROM_USER: {msg_from_user}')

        response_from_lex = call_lex(msg_from_user)
        if response_from_lex:
            resp_dispatch = dispatch(response_from_lex)
            # intent_name = response_from_lex['sessionState']['intent']['name']
            # print(f'intent_name: {intent_name}')

            """
            lex_msg_content = ''
            delimiter = ' '
            index = 1
            for message in response_from_lex:
                print('@response_from_lex: message', message)
                lex_msg_content += message['content'] + delimiter
                index += 1
                print(f'lex_msg_content {index}: {lex_msg_content}')

            index = 0
            print(f'ALL of lex_msg_content: {lex_msg_content}')
            current_timestamp = datetime.now()
            print(f'current_timestamp: {current_timestamp}')
            """
            resp = {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json'
                },
                "messages": [
                    {
                        "type": "unstructured",
                        "unstructured": {
                            "text": resp_dispatch
                        }
                    }
                ]
            }
            return resp
    except ClientError as ex:
        resp = {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "text": json.dumps({
                            'message': "I'm still under development. Please come back later. This was a ClientError.",
                            'error': str(ex)
                        })
                    }
                }
            ]
        }
        return resp

"""
Synchronous with API Gateway

TODO:
- Store and check if this is the same user by hashed email session_id mapping in dynamodb (last-search)
- Implement and Call the Lambda Fx - Recommendation Engine
- LF0 handles: Greeting intent; Thank you intent; 

Reference: 
- https://docs.aws.amazon.com/lexv2/latest/APIReference/API_runtime_RecognizeText.html
- https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html

Notes:
- modify resp to send back the next question Lex would ask from the user
- format resp in a way that is understood by the frontend
- HINT: refer to function insertMessage() in chat.js that you uploaded to the S3 bucket
- Create a separate Lambda function to handle the recommendation based on the last search.

"""
