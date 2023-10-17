"""
Script to generate the proper JSON formatted data for the ElasticSearch Bulk API format to populate ElasticSearch.
"""

import csv
import json

def csv_to_elastic_bulk_format(csv_filename):
    """Convert CSV to Elasticsearch Bulk API format."""

    # List to store formatted data
    formatted_data = []
    # Counter for _id
    _id_counter = 1

    with open(csv_filename, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            # Extract businessID and category from the current row
            business_id = row.get('businessID')
            category = row.get('category')

            # Add action metadata
            action = {
                "index": {
                    "_index": "restaurants",
                    "_id": _id_counter
                }
            }
            formatted_data.append(json.dumps(action))
            _id_counter += 1

            # Add the businessID and category data from the row
            data = {
                "businessID": business_id,
                "category": category
            }
            formatted_data.append(json.dumps(data))

    return formatted_data


def main():
    # Convert CSV to Elasticsearch Bulk API format
    formatted_data = csv_to_elastic_bulk_format('restaurant_filtered.csv')

    # Save the transformed data to a JSON file
    with open('restaurant_data.json', 'w') as outfile:
        outfile.write('\n'.join(formatted_data) + '\n')

    print("Data saved to restaurant_data.json")


if __name__ == '__main__':
    main()
