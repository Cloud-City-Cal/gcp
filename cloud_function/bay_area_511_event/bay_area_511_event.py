import os

import functions_framework
import requests
from cloudevents.http import CloudEvent
from google.cloud.pubsub import PublisherClient
from google.cloud.bigquery import Client
from google.protobuf.json_format import ParseDict

from bay_area_511_pb2 import Event

API_ENDPOINT = "https://api.511.org/traffic/events"

'''
Environment Variables:
API_KEY: API Key for 511.org
PROJECT_ID: Current project ID
DATASET_ID: Dataset ID to check for existing records
TABLE_ID: Table ID to check for existing records
TOPIC_ID: Topic ID of the published message
https://511.org/sites/default/files/2023-10/511%20SF%20Bay%20Open%20Data%20Specification%20-%20Traffic.pdf
'''


@functions_framework.cloud_event
def collect_weather_data(cloud_event: CloudEvent):
    publisher_client = PublisherClient()

    response = requests.get(f"https://api.511.org/traffic/events", params={
        'API_KEY': os.environ['API_KEY'],
        'format': 'json'
    })
    response.raise_for_status()
    response.encoding = 'utf-8-sig'
    data = response.json()

    for event in data["events"]:
        if record_exists(event['id']):
            continue
        proto = ParseDict(clean_up_keys(event), Event(), ignore_unknown_fields=True)
        topic_path = publisher_client.topic_path(os.environ['PROJECT_ID'], os.environ['TOPIC_ID'])
        publisher_client.publish(topic_path, proto.SerializeToString())


def record_exists(id: int) -> bool:
    client = Client()
    query_job = client.query(
        f"""
        SELECT COUNT(*)
        FROM {os.environ['PROJECT_ID']}.{os.environ['DATASET_ID']}.{os.environ['TABLE_ID']}
        WHERE id = '{id}'
        """
    )
    result = query_job.result()
    row_count = next(result)[0]
    return row_count != 0


def clean_up_keys(data: dict):
    for key in list(data.keys()):
        if isinstance(data[key], dict):
            data[key] = clean_up_keys(data[key])

        if key[0] == "+":
            data[key[1:]] = data[key]
            del data[key]

    return data
