"""
Script to scrape restaurant data using Yelp API.
Data is later stored in DynamoDB and processed with ElasticSearch to allow the chatbot to make dining suggestions.
"""

import requests
import time
import csv

def scrape_restaurant_data(api_key, api_url, search_categories):
    """Scrapes required data from Yelp restaurant businesses matching dining categories"""
    restaurant_data = []

    # Write data to a CSV file: will be used to populate DynamoDB table downstream
    with open('restaurant_data.csv', mode='w', newline='') as file:

        #Write header column
        writer = csv.writer(file)
        writer.writerow(
            ['businessID', 'name', 'address', 'coordinates', 'numberOfReviews', 'rating', 'zipCode', 'category'])

        # Look for restaurants matching each cuising type
        for category in search_categories:
            print("Now searching for restaurants matching {} cuisine type...".format(category))

            # Use offset to keep track of progress towards maximum scraping limit
            offset = 0
            # Yelp API maximum limit per request
            limit = 50

            # Counter for consecutive 400 errors: after 10 invalid requests pivot to looking for next cuisine type
            consecutive_400_errors = 0

            while offset < 10000 and consecutive_400_errors < 10:

                # Search for restaurants in Manhattan
                params = {
                    'location': 'Manhattan',
                    'categories': category,
                    'offset': offset,
                    'limit': limit
                }
                headers = {
                    'Authorization': f'Bearer {api_key}'
                }

                # Check response to handle edge cases with server issues, etc.
                response = requests.get(api_url, params=params, headers=headers)

                # Case 1: Valid response!
                if response.status_code == 200:
                    data = response.json()
                    businesses = data.get('businesses', [])

                    if not businesses:
                        break

                    for business in businesses:

                        # Extract the Yelp categories for the restaurant
                        yelp_categories = [cat['title'].lower() for cat in business.get('categories', [])]

                        # Check if any category the restaurant is listed under matches the search category
                        if any(category in yelp_cat for yelp_cat in yelp_categories):

                            # Scrape the needed data
                            restaurant_id = business['id']
                            name = business['name']
                            address = ', '.join(business['location']['display_address'])
                            coordinates = (f"{business['coordinates']['latitude']}, "
                                           f"{business['coordinates']['longitude']}")
                            num_reviews = business['review_count']
                            rating = business['rating']
                            zip_code = business['location']['zip_code']

                            row_data = [restaurant_id, name, address, coordinates, num_reviews, rating, zip_code,
                                 category]

                            # Add each restaurant only once (even if it's listed under multiple categories)
                            if row_data not in restaurant_data:
                                restaurant_data.append(row_data)
                                writer.writerow(row_data)

                    offset += limit
                    # Reset the consecutive error counter if we haven't had any errors
                    consecutive_400_errors = 0

                # Case 2: Invalid/corrupted request
                elif response.status_code == 400:
                    consecutive_400_errors += 1  # Increment the consecutive error counter
                    print(
                        f"Error 400 for category '{category}' ({consecutive_400_errors} consecutive errors). "
                        f"Skipping...")
                    # Wait for a few seconds before trying again...
                    time.sleep(10)

                # Case 3: Too many requests sent to Yelp within given timeframe
                elif response.status_code == 429:
                    if 'Retry-After' in response.headers:
                        retry_after = int(response.headers['Retry-After'])
                        time.sleep(retry_after)
                    else:
                        print("Waiting before sending more requests...")
                        time.sleep(10)

                # Case 4: something else happens, in which case we skip
                else:
                    print(f"Error: {response.status_code}")

    print('Data scraping and CSV creation completed!')


if __name__ == '__main__':

    # Set Yelp client id and api key credentials: replace with real tokens generated from Yelp for Developers!
    client_id = ''
    api_key = ''

    # Define the Yelp API endpoint
    api_url = 'https://api.yelp.com/v3/businesses/search'

    # List cuisine types to search for
    categories = ['chinese', 'japanese', 'korean', 'thai', 'mexican', 'indian', 'american', 'italian', 'french',
                  'middle eastern', 'seafood', 'fast food']

    scrape_restaurant_data(api_key, api_url, categories)
