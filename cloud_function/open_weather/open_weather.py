import os
import sys

import functions_framework
import requests
from cloudevents.http import CloudEvent
from google.cloud.pubsub import PublisherClient
from google.protobuf.json_format import ParseDict

from weather_pb2 import Weather

API_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"

'''
Environment Variables:
API_KEY: API Key for openweathermap.org
PROJECT_ID: Current project ID
TOPIC_ID: Topic ID of the published message
'''

@functions_framework.cloud_event
def collect_weather_data(cloud_event: CloudEvent):
    response = requests.get(API_ENDPOINT, params={
        "lon": cloud_event.data['message']['lon'],
        "lat": cloud_event.data['message']['lat'],
        "appid": os.environ['API_KEY'],
        "units": "metric",
    })
    response.raise_for_status()
    proto = ParseDict(response.json(), Weather())

    publisher_client = PublisherClient()
    topic_path = publisher_client.topic_path(os.environ['PROJECT_ID'], os.environ['TOPIC_ID'])
    publisher_client.publish(topic_path, proto.SerializeToString())