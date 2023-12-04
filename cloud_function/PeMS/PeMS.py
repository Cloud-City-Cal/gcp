import os
from io import StringIO
from typing import List

import requests
import csv
import datetime
import json
from cloudevents.http import CloudEvent
from google.cloud.pubsub import PublisherClient
from google.cloud import bigquery

from pems_pb2 import PeMS

BASE_URL = "https://pems.dot.ca.gov"
TIME_RANGE = 60 * 60  # 60 mins interval

'''
Environment Variables:
USERNAME: Username for pems.dot.ca.gov
PASSWORD: Password for pems.dot.ca.gov
PROJECT_ID: Current project ID
DATASET_ID: Dataset ID to check for existing records
TABLE_ID: Table ID to check for existing records
TOPIC_ID: Topic ID of the published message
'''


def get_pems_data(cloud_event: CloudEvent):
    # Get the station id from big query or request
    with open('station_ids.json') as f:
        stations = json.load(f)

    session = requests.Session()
    # Log in
    session.post(
        BASE_URL,
        data={
            "username": os.environ['USERNAME'],
            "password": os.environ['PASSWORD'],
            "login": "Login",
        },
    )

    # PeMS is timezone unaware, so we treat Pacific Time as UTC.
    timestamp_now = int(datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp())

    publisher_client = PublisherClient()
    topic_path = publisher_client.topic_path(os.environ['PROJECT_ID'], os.environ['TOPIC_ID'])


    # Crawl Data
    for station_id in stations:
        print(f"Fetching data for station_id: {station_id}.")
        params = {
            "report_form": 1,
            "dnode": "VDS",
            "content": "loops",
            "export": "text",
            "station_id": station_id,
            "s_time_id": timestamp_now - TIME_RANGE,
            "e_time_id": timestamp_now,
            "dow_0": "on",
            "dow_1": "on",
            "dow_2": "on",
            "dow_3": "on",
            "dow_4": "on",
            "dow_5": "on",
            "dow_6": "on",
            "dow_7": "on",
            "holidays": "on",
            "q": "speed",
            "q2": "flow",
            "gn": "5min",
            "agg": "on",
            "lane1": "on",
            "lane2": "on",
            "lane3": "on",
            "lane4": "on",
            "lane5": "on",
            "lane6": "on",
            "lane7": "on",
        }
        res = session.post(
            BASE_URL,
            params=params,
        )
        res.raise_for_status()
        reader = csv.DictReader(StringIO(res.text), delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            lanes_len = int(row["# Lane Points"])
            data = PeMS(station_id=station_id, time=row["5 Minutes"], observed=float(row["% Observed"]), lanes=[])
            for i in range(1, lanes_len + 1):
                data.lanes.append(PeMS.Lane(speed=float(row[f"Lane {i} Speed (mph)"]), flow=float(row[f"Lane {i} Flow (Veh/5 Minutes)"])))
            publisher_client.publish(topic_path, data.SerializeToString())


def get_missing_record_ids() -> List:
    client = bigquery.Client(project=os.environ['PROJECT_ID'])

    query_job = client.query(
        f"""
        SELECT station_id, MAX(`time`) as lastest_time
        FROM {os.environ['PROJECT_ID']}.{os.environ['DATASET_ID']}.{os.environ['TABLE_ID']}
        GROUP BY station_id
        """)

    return query_job.result()


if __name__ == '__main__':
    get_pems_data(None)