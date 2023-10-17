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


def elicit_slot(intent_name, slots, slot_to_elicit,
                session_attributes, message):
    resp = {
        'messages': [message],
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit
            },
            'intent': {
                'name': intent_name,
                'slots': slots
            }
        }
    }
    print('@elicit_slot: response ', resp)
    return resp


def elicit_intent_lf0(session_attributes, messages):
    resp = {
        'messages': messages,
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ElicitIntent'
            },
            'intent': {
                'name': 'GreetingIntent',
            }
        }
    }
    print('@elicit_intent: response ', resp)
    return resp


def delegate_lf0(session_attributes, slots):
    resp = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Delegate',
            },
            'intent': {
                'name': 'DiningSuggestionsIntent',
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
