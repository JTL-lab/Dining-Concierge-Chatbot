from datetime import datetime
from utility import serialize_datetime

def build_validation_result(is_valid, violated_slot, message_content):
    resp = {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {
            'contentType': 'PlainText',
            'content': message_content,
        }
    }
    print('@build_validation_result: response ', resp)
    return resp


def elicit_slot(intent_name, slots, slot_name,
                session_attributes, message):
    resp = {
        'messages': [message],
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_name
            },
            'intent': {
                'name': intent_name,
                'slots': slots
            }
        }
    }
    print('@elicit_slot: response ', resp)
    return resp


def elicit_intent(intent_name, session_attributes, messages):
    resp = {
        'messages': messages,
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ElicitIntent'
            },
            'intent': {
                'name': intent_name,
            }
        }
    }
    print('@elicit_intent: response ', resp)
    return resp


def delegate(session_attributes, slots, intent_name='DiningSuggestionsIntent'):
    # Serialize the combined_datetime field
    if 'combined_datetime' in session_attributes:
        print(session_attributes['combined_datetime'])
        combined_datetime_val = session_attributes['combined_datetime']

        if isinstance(combined_datetime_val, datetime):
            combined_datetime_str = serialize_datetime(combined_datetime_val)
            session_attributes['combined_datetime'] = combined_datetime_str
            time_part = datetime.strptime(combined_datetime_str, "%Y-%m-%dT%H:%M:%S").strftime("%I:%M%p")
        else:
            time_part = datetime.strptime(combined_datetime_val, "%Y-%m-%d %I:%M %p").strftime("%I:%M%p")

        if 'DiningTime' in slots and 'value' in slots['DiningTime']:
            slots['DiningTime']['value']['resolvedValues'] = [time_part]

    resp = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Delegate',
            },
            'intent': {
                'name': intent_name,
                'slots': slots,
                'state': 'InProgress'
            }
        }
    }
    print('@delegate: response ', resp)
    return resp


def close(session_attributes, intent, messages):
    resp = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'intent': intent,
            'dialogAction': {
                'type': 'Close'
            }
        },
        'messages': messages
    }
    print('@close: response ', resp)
    return resp

"""
Reference:
- https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-lex-runtime-v2/modules/dialogaction.html
"""