import re
import hashlib
import calendar
from datetime import datetime, timedelta, time
import phonenumbers
from word2number import w2n

DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
EMAIL_PATTERN = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
AM_PM_PATTERN = r'^\d{1,2}(:[0-5][0-9])?(\s*[ap]m)?$'


def utility_validate_date(date_string):
    print(f'@utility_validate_date: {date_string}')
    today = datetime.today().date()
    day_to_number = {day.lower(): idx + 1 for idx, day in enumerate(calendar.day_name)}

    if date_string == "today":
        return today
    elif date_string == "tomorrow":
        return today + timedelta(days=1)

    try:
        date_obj = datetime.strptime(date_string, "%m/%d/%Y")
        parsed_date = date_obj.date()

        if parsed_date < today:
            return None
        return parsed_date
    except ValueError:
        try:
            date_obj = datetime.strptime(date_string, "%Y-%m-%d")
            parsed_date = date_obj.date()

            if parsed_date < today:
                return None
            return parsed_date
        except ValueError:
            input_day = date_string.lower()
            input_day_number = day_to_number.get(input_day)

            if input_day_number is not None:
                days_until_next_day = (input_day_number - today.weekday() + 7) % 7
                future_date = today + timedelta(days=days_until_next_day)
                if future_date < today:
                    return None
                return future_date
    return None


def utility_validate_lex_ok_time(time_string):
    # From the slot originalValue, bc otherwise lex will ask again for the dining time when calling delegate
    if not re.match(AM_PM_PATTERN, time_string):
        return None
    else:
        return time_string


def utility_validate_dining_time(date_string, time_string):
    print(f'@utility_validate_dining_time: time_string {time_string}, date_string {date_string}')
    try:
        date_obj = utility_validate_date(date_string)
    except ValueError:
        return None

    cleaned_time_string = re.sub(r'[\s\.]', '', time_string).lower()

    if cleaned_time_string.endswith("pm"):
        cleaned_time_string = cleaned_time_string.replace("pm", "PM")
    elif cleaned_time_string.endswith("am"):
        cleaned_time_string = cleaned_time_string.replace("am", "AM")

    if ':' in cleaned_time_string: # check for 5:30pm or 14:30
        try:
            if "PM" in cleaned_time_string or "AM" in cleaned_time_string:
                time_obj = datetime.strptime(cleaned_time_string, "%I:%M%p").time()
            else:
                time_obj = datetime.strptime(cleaned_time_string, "%H:%M").time()
        except ValueError:
            return None
    else:
        try:
            time_obj = datetime.strptime(cleaned_time_string, "%I%p").time()
        except ValueError:
            # Military time: 14:00 = 2:00 PM
            try:
                hour_minute = cleaned_time_string.split(":")
                hour_value = int(hour_minute[0])
                minute_value = int(hour_minute[1])
                if 0 <= hour_value <= 23 and 0 <= minute_value <= 59:
                    time_obj = time(hour=hour_value, minute=minute_value)
                else:
                    return None
            except ValueError:
                return None

    combined_datetime = datetime.combine(date_obj, time_obj)
    print(f'combined_datetime: {combined_datetime}')
    if combined_datetime <= datetime.now():
        return None
    twelve_hour_datetime = combined_datetime.strftime("%Y-%m-%d %I:%M %p")
    return twelve_hour_datetime


def utility_is_valid_email(email):
    print(f'@utility_is_valid_email: {email}')
    resp = re.match(EMAIL_PATTERN, email) is not None
    print('response_EMAIL_PATTERN: ', resp)
    return resp


def utility_is_valid_usa_phone_number(phone):
    try:
        print(f'@utility_is_valid_usa_phone_number: {phone}')
        usa_number = phonenumbers.parse(phone, "US")
        print('usa_number: ', usa_number)
        return phonenumbers.is_valid_number(usa_number)
    except phonenumbers.NumberParseException:
        return False


def utility_is_valid_party_size_old(number_of_people):
    max_party_size = 1000
    try:
        num_of_ppl = int(number_of_people)
        if 0 < num_of_ppl < max_party_size:
            return True
    except ValueError:
        pass
    return False


def utility_is_valid_party_size(number_of_people):
    max_party_size = 1000

    try:
        num_of_ppl = int(number_of_people)
        if 0 < num_of_ppl < max_party_size:
            return True
    except ValueError:
        # Try to convert written numbers to integers
        try:
            num_of_ppl = w2n.word_to_num(number_of_people)
            if 0 < num_of_ppl < max_party_size:
                return True
        except ValueError:
            pass

    return False


def lowercase_string(date_string):
    if isinstance(date_string, str):
        return date_string.lower()
    else:
        return date_string


def hash_email_to_session_id(email):
    return hashlib.sha256(email.encode()).hexdigest()


def serialize_datetime(date_input):
    if isinstance(date_input, datetime):
        return date_input.isoformat()
    return None
