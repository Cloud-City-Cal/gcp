import requests, json
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

project_id = "cloud-city-cal"
dataset_id = "511_org"
API_KEY = "YOUR_KEY"


def insert_into_bigquery(request):
    # Create a reference to the dataset and table
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_id_event = "event"
    table_ref_event = dataset_ref.table(table_id_event)
    table_event = client.get_table(table_ref_event)
    table_id_area = "area"
    table_ref_area = dataset_ref.table(table_id_area)
    table_area = client.get_table(table_ref_area)
    table_id_event_area = "event_area"
    table_ref_event_area = dataset_ref.table(table_id_event_area)
    table_event_area = client.get_table(table_ref_event_area)
    table_id_road = "road"
    table_ref_road = dataset_ref.table(table_id_road)
    table_road = client.get_table(table_ref_road)

    res = requests.get(f"http://api.511.org/traffic/events?API_KEY={API_KEY}")
    res = json.loads(res.text.encode().decode("utf-8-sig"))
    client = bigquery.Client(project=project_id)
    for event in res["events"]:
        event_id = event["id"]
        query_job = client.query(
            f"""
                SELECT COUNT(*)
                FROM `{project_id}.{dataset_id}.{table_id_event}`
                WHERE id = '{event_id}'
            """
        )
        result = query_job.result()
        row_count = next(result)[0]
        if row_count != 0:
            continue
        keys = list(event.keys())
        for key in keys:
            if key[0] == "+":
                event[key[1:]] = event[key]
                del event[key]
        created = datetime.strptime(event["created"], "%Y-%m-%dT%H:%MZ")
        event["created"] = created
        updated = datetime.strptime(
            event["updated"], "%Y-%m-%dT%H:%MZ"
        )  # 2023-08-01T21:42Z
        event["updated"] = updated
        event_subtypes = str(event["event_subtypes"])  # list of string
        event["event_subtypes"] = event_subtypes
        event["geography"] = str(event["geography"])
        schedule = str(event["schedule"])
        event["schedule"] = schedule
        if "closure_geography" in event:
            event["closure_geography"] = str(event["closure_geography"])
        areas = []
        if "areas" in event:
            areas = event["areas"]  # Optional Areas structure
            del event["areas"]
        roads = []
        if "roads" in event:
            roads = event["roads"]  # Optional Road structure
            del event["roads"]

        # Insert the data into the table
        errors = client.insert_rows(table_event, [event])

        if errors:
            return f"Error inserting event data into BigQuery: {errors}"

        for area in areas:
            query_job = client.query(
                f"""
                    SELECT COUNT(*)
                    FROM `{project_id}.{dataset_id}.{table_id_area}`                  
                                                WHERE id = '{area["id"]}'
                """
            )
            result = query_job.result()
            row_count = next(result)[0]
            if row_count == 0:
                errors = client.insert_rows(table_area, [area])
                if errors:
                    return f"Error inserting area data into BigQuery: {errors}"
            errors = client.insert_rows(
                table_event_area, [{"event_id": event_id, "area_id": area["id"]}]
            )
            if errors:
                return f"Error inserting event_area data into BigQuery: {errors}"
        for road in roads:
            keys = list(road.keys())
            for key in keys:
                if key[0] == "+":
                    road[key[1:]] = road[key]
                    del road[key]
            if "restrictions" in road:
                road["restrictions"] = str(road["restrictions"])
            if "areas" in road:
                road["areas"] = str(road["areas"])
            road["event_id"] = event_id
            errors = client.insert_rows(table_road, [road])
            if errors:
                return f"Error inserting road data into BigQuery: {errors}"
    return "ok"
