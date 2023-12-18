import json
import os

import functions_framework
import requests
from cloudevents.http import CloudEvent
from google.cloud import bigquery, pubsub
from google.protobuf.json_format import ParseDict

from weather_pb2 import Weather

API_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"

'''
Environment Variables:
API_KEY: API Key for openweathermap.org
PROJECT_ID: Current project ID
TOPIC_ID: Topic ID of the published message
CONFIG_TABLE: Table which stores configuration file
CONFIG_VERSION: Version of the config to use
'''


@functions_framework.cloud_event
def collect_weather_data(cloud_event: CloudEvent):
    for detector in get_conf():
        get_weather(lat=detector["representative_point"][0], lon=detector["representative_point"][1])


def get_weather(lon: float, lat: float):
    response = requests.get(API_ENDPOINT, params={
        "lon": lon,
        "lat": lat,
        "appid": os.environ['API_KEY'],
        "units": "metric",
    })
    response.raise_for_status()
    proto = ParseDict(response.json(), Weather())

    publisher_client = pubsub.PublisherClient()
    topic_path = publisher_client.topic_path(os.environ['PROJECT_ID'], os.environ['TOPIC_ID'])
    publisher_client.publish(topic_path, proto.SerializeToString())


def get_conf():
    client = bigquery.Client(project=os.environ['PROJECT_ID'])
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("version", "INT64", int(os.environ['CONFIG_VERSION']))
        ],
    )

    query_job = client.query(
        f"""
            SELECT *
            FROM `{os.environ['CONFIG_TABLE']}`
            WHERE version = @version
            """, job_config=job_config)

    result = query_job.result(max_results=1)
    return json.loads(next(result).data)
