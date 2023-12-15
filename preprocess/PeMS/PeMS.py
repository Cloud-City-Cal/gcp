import json, os
from google.cloud import bigquery
import functions_framework

"""
Environment Variables:
PROJECT_ID: Current project ID
DATASET_ID: Dataset ID to check for existing records
TABLE_ID: Table ID to check for existing records
TOPIC_ID: Topic ID of the published message
"""


@functions_framework.http
def preprocess(request):
    client = bigquery.Client("cloud-city-cal")
    segment_definition = []
    # TODO read segment_definition from storage
    with open("segment_definition.json", "r") as f:
        segment_definition = json.load(f)

    # TODO read timestamp_s from cloud event
    timestamp_s = "2023-11-18 19:00:00"
    for segment in segment_definition:
        query_job = client.query(
            f"""
            SELECT station_id, agg_speed
            FROM {os.environ['PROJECT_ID']}.{os.environ['DATASET_ID']}.{os.environ['TABLE_ID']}
            WHERE station_id in UNNEST({list(segment["station_ids"].keys())}) AND
            time == DATETIME({json.dumps(timestamp_s)})
            """
        )
        segment_id = segment["id"]
        result = query_job.result()

        agg_speed = (segment["end_postmile"] - segment["start_postmile"]) / sum(
            map(lambda x: segment["station_ids"][x[0]] / x[1], result)
        )
        # TODO Insert into pubsub bigquery
        # segment_id, timestamp_s, agg_speed
    return "ok"
