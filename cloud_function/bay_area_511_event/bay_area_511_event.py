import os
from typing import List, Set, Tuple

import functions_framework
import requests
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from google.cloud.pubsub import PublisherClient
from google.protobuf.json_format import ParseDict

from bay_area_511_event_pb2 import Event

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
def collect_bay_area_511_event_data(cloud_event: CloudEvent):
    publisher_client = PublisherClient()
    topic_path = publisher_client.topic_path(os.environ['PROJECT_ID'], os.environ['TOPIC_ID'])

    events = get_all_events()
    missing_ids = get_missing_record_ids(set(map(lambda x: (x['id'], x['headline']), events)))

    for event in filter(lambda event: event['id'] in missing_ids, events):
        proto = ParseDict(clean_up_keys(event), Event(), ignore_unknown_fields=True)

        publisher_client.publish(topic_path, proto.SerializeToString())


def get_all_events() -> List:
    events = []
    offset = 0

    while True:
        response = requests.get(f"https://api.511.org/traffic/events", params={
            'API_KEY': os.environ['API_KEY'],
            'format': 'json',
            'offset': offset
        })
        response.raise_for_status()
        response.encoding = 'utf-8-sig'
        data = response.json()
        events.extend(data['events'])

        if len(data['events']) == 20:
            offset += 20
        else:
            return events


def get_missing_record_ids(id_updated_tuple: Set[Tuple[str, str]]) -> Set:
    client = bigquery.Client(project=os.environ['PROJECT_ID'])
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("ids", "STRING", [data[0] for data in id_updated_tuple]),
        ],
    )

    query_job = client.query(
        f"""
        SELECT id, headline
        FROM {os.environ['PROJECT_ID']}.{os.environ['DATASET_ID']}.{os.environ['TABLE_ID']}
        WHERE id in UNNEST(@ids)
        """, job_config=job_config)

    result = query_job.result()
    existing_tuples = set(map(lambda x: (x.id, x.headline), result))
    return {data[0] for data in id_updated_tuple - existing_tuples}


def clean_up_keys(data: dict | list):
    if isinstance(data, dict):
        for key in list(data.keys()):
            data[key] = clean_up_keys(data[key])

            if key[0] == "+":
                data[key[1:]] = data[key]
                del data[key]
    elif isinstance(data, list):
        for item in data:
            clean_up_keys(item)

    return data
