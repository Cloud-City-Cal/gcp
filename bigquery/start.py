import os.path
import requests, json

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/bigquery"]

WEATHER_API_KEY = "YOUR_KEY"


def create_dataset(creds, project_id, dataset_id):
    try:
        service = build("bigquery", "v2", credentials=creds)
        service.datasets().insert(
            projectId=project_id,
            body={
                "datasetReference": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                }
            },
        ).execute()
    except HttpError as err:
        print(err)


def create_table(creds, project_id, dataset_id, table_id, table_schema):
    try:
        service = build("bigquery", "v2", credentials=creds)
        service.tables().insert(
            projectId=project_id,
            datasetId=dataset_id,
            body={
                "tableReference": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "schema": {"fields": table_schema},
            },
        ).execute()
    except HttpError as err:
        print(err)


def create_city_geo(creds, project_id, dataset_id, table_id, city):
    try:
        service = build("bigquery", "v2", credentials=creds)
        res = requests.get(
            f"http://api.openweathermap.org/geo/1.0/direct?q={city},California,US&limit=5&appid={WEATHER_API_KEY}"
        )
        res = json.loads(res.text)
        for data in res:
            if "local_names" in data:
                del data["local_names"]
            if data["lon"] > -121.8:
                continue
            service.tabledata().insertAll(
                projectId=project_id,
                datasetId=dataset_id,
                tableId=table_id,
                body={"rows": [{"json": data}]},
            ).execute()

    except HttpError as err:
        print(err)


def main():
    """Shows basic usage of the Docs API.
    Prints the title of a sample document.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    event_schema = [
        {"name": "url", "type": "STRING", "mode": "REQUIRED"},
        {"name": "jurisdiction_url", "type": "STRING", "mode": "REQUIRED"},
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "headline", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "severity", "type": "STRING", "mode": "REQUIRED"},
        {"name": "geography", "type": "STRING", "mode": "REQUIRED"},
        {"name": "created", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "updated", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "schedule", "type": "STRING", "mode": "REQUIRED"},
        {"name": "closure_geography", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timezone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_subtypes", "type": "STRING", "mode": "NULLABLE"},
        {"name": "certainty", "type": "STRING", "mode": "NULLABLE"},
        {"name": "grouped_events", "type": "STRING", "mode": "NULLABLE"},
        {"name": "detour", "type": "STRING", "mode": "NULLABLE"},
        {"name": "attachments", "type": "STRING", "mode": "NULLABLE"},
        {"name": "source_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "source_id", "type": "STRING", "mode": "NULLABLE"},
    ]

    road_schema = [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "from", "type": "STRING", "mode": "NULLABLE"},
        {"name": "to", "type": "STRING", "mode": "NULLABLE"},
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "direction", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lane_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "road_advisory", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lane_status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "article", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lanes_open", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "lanes_closed", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "impacted_systems", "type": "STRING", "mode": "NULLABLE"},
        {"name": "areas", "type": "STRING", "mode": "NULLABLE"},
    ]
    area_schema = [
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {
            "name": "id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
    ]
    event_area_schema = [
        {
            "name": "event_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "area_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
    ]

    weather_schema = [
        {"name": "dt", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "weather", "type": "STRING", "mode": "REQUIRED"},
        {"name": "base", "type": "STRING", "mode": "REQUIRED"},
        {"name": "visibility", "type": "STRING", "mode": "REQUIRED"},
        {"name": "clouds", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "timezone", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "lat", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "lon", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "temp", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "feels_like", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "temp_min", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "temp_max", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "pressure", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "humidity", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "wind_speed", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "wind_deg", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "sunrise", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "sunset", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "rain_1h", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "rain_3h", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "snow_1h", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "snow_3h", "type": "FLOAT", "mode": "NULLABLE"},
    ]

    geo_schema = [
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "lat", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "lon", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "country", "type": "STRING", "mode": "REQUIRED"},
        {"name": "state", "type": "STRING", "mode": "REQUIRED"},
    ]

    project_id = "cloud-city-cal"
    dataset_definition = {
        "511_org": {
            "event": event_schema,
            "road": road_schema,
            "area": area_schema,
            "event_area": event_area_schema,
        },
        "open_weather": {"weather": weather_schema, "geo": geo_schema},
        "PeMS": {},
    }

    city_names = ["San Mateo", "Berkeley", "Alameda", "Oakland", "Fruitvale"]

    for dataset_id in dataset_definition:
        create_dataset(creds, project_id, dataset_id)
        for table_id in dataset_definition[dataset_id]:
            create_table(
                creds,
                project_id,
                dataset_id,
                table_id,
                dataset_definition[dataset_id][table_id],
            )
    for city in city_names:
        create_city_geo(
            creds, project_id, dataset_id="open_weather", table_id="geo", city=city
        )


if __name__ == "__main__":
    main()
