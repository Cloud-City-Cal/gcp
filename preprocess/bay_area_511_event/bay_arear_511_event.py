import json, os, requests, math
from collections import defaultdict
from google.cloud import bigquery
import functions_framework
from datetime import datetime

"""
Environment Variables:
PROJECT_ID: Current project ID
DATASET_ID: Dataset ID to check for existing records
TABLE_ID: Table ID to check for existing records
TOPIC_ID: Topic ID of the published message
"""

SEVERITY_TO_SCORE = {"MINOR": 1, "MODERATE": 2, "MAJOR": 3, "SEVERE": 4, "UNKNOWN": 1}
EVENT_TYPE_TO_IDX = {
    "CONSTRUCTION": 0,
    "SPECIAL_EVENT": 1,
    "INCIDENT": 2,
    "WEATHER_CONDITION": 3,
    "ROAD_CONDITION": 4,
}


def coordinate_to_postmile(x: float, y: float) -> float:
    payload = {"callType": "getPostmileForPoint", "content": {"x": x, "y": y}}
    response = requests.post(
        "https://postmile.dot.ca.gov/PMQT/proxy.php",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data="request=" + json.dumps(payload),
    )
    response.raise_for_status()
    data = json.loads(response.text)
    postmile_data = data["locations"][0]["results"][0]["postmile"]
    payload = {
        "callType": "getOdometerForPostmile",
        "content": {
            "req": {
                "routeNumber": postmile_data["routeNumber"],
                "routeSuffixCode": postmile_data["routeSuffix"],
                "countyCode": postmile_data["county"],
                "postmilePrefixCode": postmile_data["postmilePrefix"],
                "postmileSuffixCode": postmile_data["postmileSuffix"],
                "postmileValue": postmile_data["postmileValue"],
                "alignmentCode": "",
            }
        },
    }
    response = requests.post(
        "https://postmile.dot.ca.gov/PMQT/proxy.php",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data="request=" + json.dumps(payload),
    )
    response.raise_for_status()
    return float(response.text)


@functions_framework.http
def preprocess(request):
    client = bigquery.Client("cloud-city-cal")
    segment_definition = []
    # TODO read segment_definition from storage

    with open("segment_definition.json", "r") as f:
        segment_definition = json.load(f)

    # TODO read timestamp_s from cloud event
    timestamp_s = "2023-11-18 19:00:00"
    timestamp = datetime.strptime(timestamp_s, "%Y-%m-%d %H:%M:%S")
    query_job = client.query(
        f"""
        SELECT *
        FROM {os.environ['PROJECT_ID']}.{os.environ['DATASET_ID']}.{os.environ['TABLE_ID']}
        WHERE "I-880 N" in UNNEST(roads.name) AND
        DATETIME_ADD(DATETIME({json.dumps(timestamp_s)}), INTERVAL -1 HOUR) < PARSE_DATETIME('%Y-%m-%dT%H:%MZ', created) AND
        PARSE_DATETIME('%Y-%m-%dT%H:%MZ', created) < DATETIME({json.dumps(timestamp_s)})
        """
    )
    result = query_job.result()
    scores = defaultdict(lambda: 0)
    event_types = defaultdict(lambda: [0] * len(EVENT_TYPE_TO_IDX))
    for row in result:
        event_postmile = coordinate_to_postmile(
            row["geography_point"]["coordinates"][0],
            row["geography_point"]["coordinates"][1],
        )
        event_happen_time = datetime.strptime(row["created"], "%Y-%m-%dT%H:%MZ")
        for segment in segment_definition:
            segment_postmile = segment["postmile"]
            if (
                event_postmile <= segment_postmile
                and segment_postmile - event_postmile < 10
            ):
                severity = SEVERITY_TO_SCORE.get(row["severity"], 1)
                score = severity * (
                    math.exp(
                        -float((timestamp - event_happen_time).total_seconds()) / 3600
                    )
                    + math.exp(-float(segment_postmile - event_postmile) / 10)
                )
                if score > scores[segment["id"]]:
                    scores[segment["id"]] = score
                    event_types[segment["id"]][EVENT_TYPE_TO_IDX[row["event_type"]]] = 1
    for segment in segment_definition:
        pass
        # TODO Insert into pubsub bigquery
        # segment["id"],
        # timestamp_s or timestamp,
        # scores[segment["id"]],
        # event_types[segment["id"]]
    return "ok"
