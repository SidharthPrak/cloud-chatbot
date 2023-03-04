from collections import defaultdict
import boto3
from requests_aws4auth import AWS4Auth
import requests
import time
from datetime import datetime
from decimal import *
import simplejson as json
import json

# this is the code to hit yelp api to get restaurant data and insert it into dynambodb

def empty_field_check(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

session = boto3.session.Session()
credentials = session.get_credentials()

region = 'us-east-1'

# define api key, define the endpoint, and define the header
API_KEY = 'SLh8kW81W_6Jbrotifj5UEo2iYZcMMgodTkC_NqO6TgeIYWg_iK3P1eI3pvbYmUvVh3Qimn5dd1Q_YEFpbA0TO28IfxA3cGbHzdCIgfA1jVETT31_Uw6lCU8QAP3Y3Yx'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
ENDPOINT_ID = 'https://api.yelp.com/v3/businesses/'  # + {id}
HEADERS = {'Authorization': 'bearer %s' % API_KEY}

# set parameters

#request parameter details obtained from https://docs.developer.yelp.com/reference/v3_business_search
PARAMETERS = {'term': 'food',
              'limit': 50,
              'radius': 15000,
              'offset': 200,
              'location': 'Manhattan'}

cuisines = ['italian', 'chinese', 'mexican', 'greek', 'french', 'korean', 'jewish', 'japanese', 'malaysian',
            'mediterranean', 'persian']

locations_around_manhattan = ['Lower East Side, Manhattan',
                              'Upper East Side, Manhattan',
                              'Upper West Side, Manhattan',
                              'Washington Heights, Manhattan',
                              'Central Harlem, Manhattan',
                              'Chelsea, Manhattan',
                              'Manhattan',
                              'East Harlem, Manhattan',
                              'Gramercy Park, Manhattan',
                              'Greenwich, Manhattan',
                              'Lower Manhattan, Manhattan',
                              'Bay Ridge, Brooklyn',
                              'Williamsburg, Brooklyn',
                              'Sunset Park, Brooklyn',
                              'Dyker Heights, Brooklyn',
                              'Midwood, Brooklyn',
                              'Flatbush, Brooklyn',
                              'Bushwick, Brooklyn',
                              'Martin Luther King Rd, Brooklyn',
                              'Dumbo, Brooklyn',
                              'Downtown Brooklyn',
                              'Greenpoint, Brooklyn',
                              'Park Slope, Brooklyn',
                              'Bedford Park, Bronx',
                              'Belmont, Bronx',
                              'Fordham, Bronx',
                              'Highbridge, Bronx',
                              'Hunts Point, Bronx',
                              'Jerome Park, Bronx',
                              'Kingsbridge, Bronx',
                              'Mott Haven, Bronx',
                              'Parkchester, Bronx',
                              'Riverdale, Bronx',
                              'Woodlawn, Bronx',
                              'Astoria, Queens',
                              'Jackson Heights, Queens',
                              'Long Island City, Queens',
                              'Sunnyside, Queens',
                              'Woodside, Queens',
                              'Bayside, Queens',
                              'Corona, Queens',
                              'Flushing, Queens',
                              'Glen Oaks, Queens',
                              'Jamaica, Queens']

start_time = time.time()
for location in locations_around_manhattan:
    PARAMETERS['location'] = location
    for cuisine in cuisines:
        PARAMETERS['term'] = cuisine
        response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)
        business_data = response.json()['businesses']
        for business in business_data:
            current_timestamp = datetime.now()
            timestamp_string = current_timestamp.strftime("%d/%m/%Y %H:%M:%S")
            table.put_item(
                Item={
                    'Business_ID': empty_field_check(business['id']),
                    'insertedAtTimestamp': empty_field_check(timestamp_string),
                    'Name': empty_field_check(business['name']),
                    'Cuisine': empty_field_check(cuisine),
                    'Rating': empty_field_check(Decimal(business['rating'])),
                    'Number of Reviews': empty_field_check(Decimal(business['review_count'])),
                    'Address': empty_field_check(business['location']['address1']),
                    'Zip Code': empty_field_check(business['location']['zip_code']),
                    'Latitude': empty_field_check(str(business['coordinates']['latitude'])),
                    'Longitude': empty_field_check(str(business['coordinates']['longitude'])),
                    'Open': 'N/A'
                }
            )

    print(f'Finished fetching results for {location} . Time taken: {time.time() - start_time}')
