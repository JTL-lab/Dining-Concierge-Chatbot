import os
import logging
import json
import time
from datetime import datetime
import boto3
from lex_utility import build_validation_result, elicit_slot, elicit_intent, delegate, close
from utility import utility_validate_date, utility_validate_dining_time, utility_is_valid_email, utility_is_valid_usa_phone_number, utility_is_valid_party_size, lowercase_string


GREETING_MSG = "Hello there! What can I help you with today?"
THANK_YOU_MSG = "Thank you, come again!"
FULFILLED_MSG = "Awesome, the suggestions will be sent to your email shortly."
REGION_NAME = "us-east-1"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
MSG_CONTENT_CUISINE = "Sorry, that Cuisine is not offered. Please enter a supported cuisine [chinese, italian, french, spanish, japanese, korean, thai, mexican, american]."
MSG_CONTENT_LOCATION = "Sorry, No food found in that Location. Please enter a valid Location: [new york, ny, nyc, manhattan]."
MSG_CONTENT_NUM_OF_PPL = "Invalid Party Size. Please enter a valid number."
MSG_CONTENT_DINING_DATE = "Invalid Dining Date. Please enter a valid date such as [today, tomorrow, monday, MM/DD/YYYY]."
MSG_CONTENT_DINING_TIME = "Invalid Dining Time. Please enter a valid time."
MSG_CONTENT_PHONE = "Invalid Phone Number. Please provide a valid Phone Number."
MSG_CONTENT_EMAIL = "Invalid Email. Please provide a valid Email."
OS_ENV = "America/New_York"

client = boto3.client('lambda')
sqs = boto3.client('sqs')
SQS_QUEUE_URI = 'https://sqs.us-east-1.amazonaws.com/957008505873/sqs_a1_dining'
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
yelp_cuisines = ['chinese', 'japanese', 'korean', 'thai', 'mexican', 'indian', 'american', 'italian', 'french', 'middle eastern', 'seafood', 'fast food']
yelp_locations = ['new york', 'ny', 'nyc', 'manhattan']
slot_order = ['Location', 'Cuisine', 'DiningDate', 'DiningTime', 'NumberOfPeople', 'PhoneNumber', 'Email']


def check_user_history(email_address):

    query_expression = {
    "TableName": table_name,
    "KeyConditionExpression": "#email = :email",
    "ExpressionAttributeNames": {
        "#email": "email"
    },
    "ExpressionAttributeValues": {
        ":email": {"S": email_address}
    },
    "ScanIndexForward": False,  
    "Limit": 1
    }

    # Query the history table
    response = dynamodb.query(**query_expression)

    if response['Count'] > 0:
        # Get the most recent item
        most_recent_item = response['Items'][0]
        print("Most recent item for email {}: {}".format(email_to_query, most_recent_item))
	cuisine = most_recent_item['Cuisine'] 
	location = most_recent_item['Location']
	
	return cuisine, location 


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


def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']


def get_slot(intent_request, slot_name):
    slots = get_slots(intent_request)
    slot =  slots.get(slot_name, None)
    print('@get_slot: slot', slot)
    return slot


def get_slot_value(slot):
    slot_values = slot['value']
    if slot_values:
        slot_value = slot_values['resolvedValues'][0] if slot_values['resolvedValues'] else slot_values['originalValue']
        print('@get_slot_value: slot_value', slot_value)
        return slot_value
    else:
        return None


def get_next_empty_slot(intent_request):
    slots = get_slots(intent_request)
    # Must match the order in Lex V2!
    # The current_slot is the first slot that's null
    for slot_name in slot_order:
        if slots[slot_name] is None:
            print('@get_current_slot: slot_name', slot_name)
            return slot_name
    return None


def push_to_sqs(message_attributes):
    print('@push_to_sqs: message', message_attributes)
    location = message_attributes['Location']
    cuisine = message_attributes['Cuisine']
    number_of_people = message_attributes['NumberOfPeople']
    dining_time = message_attributes['DiningTime']
    email = message_attributes['Email']
    phone_number = message_attributes['PhoneNumber']
    current_date = message_attributes['Date']

    sqs_msg_attributes = {
        'Location': {
                'DataType': 'String',
                'StringValue': location
            },
        'Cuisine': {
            'DataType': 'String',
            'StringValue': cuisine
        },
        'DiningTime': {
            'DataType': 'String',
            'StringValue': dining_time
        },
        'NumberOfPeople': {
            'DataType': 'String',
            'StringValue': number_of_people
        },
        'PhoneNumber': {
            'DataType': 'String',
            'StringValue': phone_number
        },
        'Email': {
            'DataType': 'String',
            'StringValue': email
        },
        'Date': {
            'DataType': 'String',
            'StringValue': current_date
        }
    }
    print(f'sqs_msg_attributes: {sqs_msg_attributes}')
    message_body = 'Slots from LexV2 to SQS'
    resp = sqs.send_message(
        QueueUrl=SQS_QUEUE_URI,
        DelaySeconds=10,
        MessageAttributes=sqs_msg_attributes,
        MessageBody=message_body
    )
    print('@push_to_sqs: response', resp)


# Not used (yet?): https://docs.aws.amazon.com/lexv2/latest/dg/lambda-router-function.html
def router(event):
    intent_name = event['sessionState']['intent']['name']
    fn_name = os.environ.get(intent_name)
    print(f"Intent: {intent_name} -> Lambda: {fn_name}")
    if fn_name:
        # invoke lambda and return result
        invoke_response = client.invoke(FunctionName=fn_name, Payload = json.dumps(event))
        print(invoke_response)
        payload = json.load(invoke_response['Payload'])
        return payload
    raise Exception('No environment variable for intent: ' + intent_name)


def get_current_slot_from_session(session_attributes):
    return session_attributes.get('last_slot_filled')


def set_current_slot_in_session(session_attributes, current_slot):
    print(f'set_current_slot_in_session:current_slot {current_slot}')
    session_attributes['last_slot_filled'] = current_slot


def get_combined_datetime_from_session(session_attributes):
    return session_attributes.get('combined_datetime')


def set_combined_datetime_in_session(session_attributes, combined_datetime):
    print(f'set_combined_datetime_in_session: {combined_datetime}')
    session_attributes['combined_datetime'] = combined_datetime


def get_session_attributes(intent_request):
    session_attributes = intent_request.get('sessionState', {}).get('sessionAttributes', {})
    print('@get_session_attributes: session_attributes', session_attributes)
    return session_attributes


def get_msg_attributes(intent_request, session_attributes):
    location_slot = get_slot(intent_request, 'Location')
    location_value = lowercase_string(get_slot_value(location_slot))
    cuisine_slot = get_slot(intent_request, 'Cuisine')
    cuisine_value = lowercase_string(get_slot_value(cuisine_slot))
    dining_time_value = get_combined_datetime_from_session(session_attributes)
    number_of_people_slot = get_slot(intent_request, 'NumberOfPeople')
    number_of_people_value = get_slot_value(number_of_people_slot)
    phone_number_slot = get_slot(intent_request, 'PhoneNumber')
    phone_number_value = get_slot_value(phone_number_slot)
    email_slot = get_slot(intent_request, 'Email')
    email_value = get_slot_value(email_slot)

    message_attributes = {
        'Location': location_value,
        'Cuisine': cuisine_value,
        'DiningTime': dining_time_value,
        'NumberOfPeople': number_of_people_value,
        'PhoneNumber': phone_number_value,
        'Email': email_value,
        'Date': datetime.now().strftime(DATE_FORMAT)
    }
    print('@get_msg_attributes: message_attributes', message_attributes)
    return message_attributes


def handle_greeting_intent(intent_request):
    print('@handle_greeting_intent: intent_request', intent_request)
    intent_name = 'GreetingIntent'
    session_attributes = get_session_attributes(intent_request)
    messages = [{
        "contentType": "PlainText",
        "content": GREETING_MSG
    }]
    resp = elicit_intent(intent_name, session_attributes, messages)
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


def handle_fallback_intent():
    resp = {
        'statusCode': 500,
        'headers': {
            'Content-Type': 'application/json'
        },
        "messages": [
            {
                "type": "string",
                "unstructured": {
                    "text": "I'm still under development. Please come back later.Boop."
                }
            }
        ]
    }
    return resp


def validate_slot(slot_name, slot_value, slots, session_attributes):
    print(f'validate_slot: {slot_name} {slot_value}')

    # Location Validation
    if slot_name == 'Location' and slot_value not in yelp_locations:
        return build_validation_result(False, 'Location', MSG_CONTENT_LOCATION)

    # Cuisine Validation
    if slot_name == 'Cuisine' and slot_value not in yelp_cuisines:
        return build_validation_result(False, 'Cuisine', MSG_CONTENT_CUISINE)

    # DiningDate Validation
    if slot_name == 'DiningDate' and utility_validate_date(slot_value) is None:
        return build_validation_result(False, 'DiningDate', MSG_CONTENT_DINING_DATE)

    # DiningTime Validation
    if slot_name == 'DiningTime':
        dining_date_from_prev_slot = lowercase_string(get_slot_value(slots[slot_order[2]]))
        combined_datetime = utility_validate_dining_time(dining_date_from_prev_slot, slot_value)
        print(f'combined_datetime: {combined_datetime}')
        if combined_datetime is None:
            print('Error: Failed validation for DiningTime')
            return build_validation_result(False, 'DiningTime', MSG_CONTENT_DINING_TIME)
        set_combined_datetime_in_session(session_attributes, combined_datetime)

    # NumberOfPeople Validation
    if slot_name == 'NumberOfPeople' and not utility_is_valid_party_size(slot_value):
        return build_validation_result(False, 'NumberOfPeople', MSG_CONTENT_NUM_OF_PPL)

    # PhoneNumber Validation
    if slot_name == 'PhoneNumber' and not utility_is_valid_usa_phone_number(slot_value):
        return build_validation_result(False, 'PhoneNumber', MSG_CONTENT_PHONE)

    # Email Validation
    if slot_name == 'Email' and not utility_is_valid_email(slot_value):
        return build_validation_result(False, 'Email', MSG_CONTENT_EMAIL)

    # No Validation Errors found!
    print('No Validation Errors found!')
    return build_validation_result(True, None, None)


def validate_dining_slot(intent_request, invocation_source, session_attributes):
    print('@validate_dining_slot: intent_request', intent_request)
    intent = intent_request['sessionState']['intent']
    intent_name = intent.get('name', [])
    slots = get_slots(intent_request)

    if invocation_source == 'DialogCodeHook':
        print(f"Inside DialogCodeHook invocation_source: {invocation_source}")

        current_slot_name = get_current_slot_from_session(session_attributes)
        print(f'current_slot_name {current_slot_name}')
        if not current_slot_name:
            current_slot_name = slot_order[0]

        print(slots[current_slot_name])
        if slots[current_slot_name]:
            slot_value_string = lowercase_string(get_slot_value(slots[current_slot_name]))
            print(f'slot_value_string: {slot_value_string}')

            slot_validation_result = validate_slot(current_slot_name, slot_value_string, slots, session_attributes)
            print(f'slot_validation_result: {slot_validation_result}')

            if slot_validation_result['isValid'] is False:
                return elicit_slot(intent_name,
                                slots,
                                slot_validation_result['violatedSlot'],
                                session_attributes,
                                slot_validation_result['message'])
            resp_delegate = delegate(session_attributes, slots)
            if current_slot_name in slot_order:
                # if current_slot_name == 'Email':
                    # Query the DynamoDB table to see if history exists for user
                    # cuisine, location = check_user_history(slot_value_string)
                    # slots['Location']['value']['originalValue'] = location
                    # slots['Location']['value']['resolvedValues'] = location
                    # slots['Location']['value']['interpretedValue'] = location
                    # slots['Cuisine']['value']['originalValue'] = cuisine
                    # slots['Cuisine']['value']['resolvedValues'] = cuisine
                    # slots['Cuisine']['value']['interpretedValue'] = cuisine
                    # if cuisine and location: 
                    #     next_slot_index = slot_order.index(current_slot_name) + 3
                # else: 
                #     next_slot_index = slot_order.index(current_slot_name) + 1
                next_slot_index = slot_order.index(current_slot_name) + 1
                if next_slot_index < len(slot_order):
                    next_slot = slot_order[next_slot_index]
                    set_current_slot_in_session(session_attributes, next_slot)
            return resp_delegate
        else:
            print(f'Error: No slot value present for {current_slot_name}')
            input_transcript = intent_request['inputTranscript']
            print(f'inputTranscript: {input_transcript}')
            slot_validation_result = validate_slot(current_slot_name, input_transcript, slots, session_attributes)
            print(f'slot_validation_result: {slot_validation_result}')
            return elicit_slot(intent_name,
                                slots,
                                slot_validation_result['violatedSlot'],
                                session_attributes,
                                slot_validation_result['message'])

def handle_dining_suggestions_intent(intent_request, invocation_source):
    print('@handle_dining_suggestions_intent: intent_request', intent_request)
    print(f"invocation_source: {invocation_source}")
    session_attributes = get_session_attributes(intent_request)

    if invocation_source == 'DialogCodeHook':
        resp_validate_dining_slot = validate_dining_slot(intent_request, invocation_source, session_attributes)
        if resp_validate_dining_slot:
            return resp_validate_dining_slot

    # Fulfillment step: only if no slot validation errors
    elif invocation_source == 'FulfillmentCodeHook':
        fulfilled_intent = intent_request['sessionState']['intent']
        print(f'intent: {fulfilled_intent}')
        message_attributes = get_msg_attributes(intent_request, session_attributes)
        
        push_to_sqs(message_attributes)
        fulfilled_intent['state'] = 'Fulfilled'
        
        # Create history table if it doesn't already exist
        cred = boto3.Session().get_credentials()
        dynamodb = boto3.resource('dynamodb',
                                  aws_access_key_id=cred.access_key,
                                  aws_secret_access_key=cred.secret_key,
                                  region_name=REGION)

        history_table_name = 'user-dining-history'
        partition_key = 'DateTime'
        
        make_dynamo_db_table(dynamodb, history_table_name, partition_key)
        
        # Add item to the user history table 
        now = datetime.now()
        # Include time stamp of when item was created as partition key
        history_item = {
            'DateTime': int(datetime.now().strftime("%Y%m%d%H%M%S")),
            'Location': message_attributes['Location'],
            'Cuisine': message_attributes['Cuisine'],
            'Email': message_attributes['Email']
        }

        # Add the item to the user history table
        table = dynamodb.Table(table_name)
        response = table.put_item(Item=history_item)
        
        messages = [{
            "contentType": "PlainText",
            "content": FULFILLED_MSG
        }]
        session_attributes = {}
        resp = close(session_attributes, fulfilled_intent, messages)
        return resp
    else:
        raise ValueError(f"Unknown invocation source: {invocation_source}")


def dispatch(intent_request):
    print('@dispatch: intent_request', intent_request)
    intent_name = intent_request['sessionState']['intent']['name']
    print(f'intent_name {intent_name}')

    if intent_name == 'GreetingIntent':
        return handle_greeting_intent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        invocation_source = intent_request['invocationSource']
        print(f'invocationSource: {invocation_source}')
        return handle_dining_suggestions_intent(intent_request, invocation_source)
    elif intent_name == 'ThankYouIntent':
        return handle_thank_you_intent(intent_request)
    elif intent_name == 'FallbackIntent':
        return handle_fallback_intent()
    else:
        raise Exception('Error: {intent_name} is unsupported.')


def init_load_session(event):
    current = event['currentIntent']
    print('@init_load_session: current', current)


def lambda_handler(event, context):
    print('@lambda_handler: event', event)
    os.environ['TZ'] = OS_ENV
    time.tzset()
    logger.debug(event)
    resp = dispatch(event)
    return resp


"""
References
- https://aws.amazon.com/blogs/machine-learning/interact-with-an-amazon-lex2v2-bot-with-the-aws-cli-aws-sdk-for-python-and-aws-sdk-dotnet/
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
- https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
- https://docs.aws.amazon.com/lexv2/latest/APIReference/API_runtime_DialogAction.html

Requirements
- Check the Cloudwatch logs to understand data inside event and parse it to handle logic to validate user input and send it to Le
- Lex called LF1 with the user message and previous related state so you can verify the user input. Validate and let Lex know what to do next.
- resp = {"statusCode": 200, "sessionState": event["sessionState"]
- Lex will propose a next state if available but if user input is not valid, you will modify it to tell Lex to ask the same question again (meaning ask the current slot question again)
    
    if "proposedNextState" not in event:
        resp["sessionState"]["dialogAction"] = {"type": "Close"}
    else:
        resp["sessionState"]["dialogAction"] = event["proposedNextState"][
            "dialogAction"
        ]
    dialog_action_type = event['proposedNextState']['dialogAction']['type']
    print(f'dialog_action_type: {dialog_action_type}')
    session_intent_state = event['sessionState']['intent']['state']
    print(f'session_intent_state: {session_intent_state}')
"""