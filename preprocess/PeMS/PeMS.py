import bigquery, json, os

def preprocess(request):
    client = bigquery.Client()
    segment_definition = None
    # read from storage
    with open("segment_definition.json", "r") as f:
        segment_definition = json.load(f)
    
    year = 2023
    month = 12
    day = 10
    hour = 14
    minute = 30
    for segment in segment_definition:
        query_job = client.query(
            f"""
            SELECT agg_speed
            FROM {os.environ['PROJECT_ID']}.{os.environ['DATASET_ID']}.{os.environ['TABLE_ID']}
            WHERE id in {segment["station_id"]})
            AND time == DATETIME({year}, {month}, {day}, {hour}, {minute}, 0)
            """)
        segment_id = segment["id"]
        result = query_job.result()
        agg_speed = sum(result) / len(result)
        # Insert into database
        # segment_id, time, agg_speed
    return "ok"