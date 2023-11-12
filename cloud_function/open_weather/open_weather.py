import requests, json
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

project_id = "cloud-city-cal"
dataset_id = "open_weather"
API_KEY = "YOUR_KEY"


def insert_into_bigquery(request):
    # Create a reference to the dataset and table
    api_key = API_KEY
    city_names = ["San Mateo", "Berkeley", "Alameda", "Oakland", "Fruitvale"]
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_id_weather = "weather"
    table_ref_weather = dataset_ref.table(table_id_weather)
    table_weather = client.get_table(table_ref_weather)
    table_id_geo = "geo"
    table_ref_geo = dataset_ref.table(table_id_geo)
    table_geo = client.get_table(table_ref_geo)

    for city_name in city_names:
        query_job = client.query(
            f"""
                SELECT lat, lon
                FROM `{project_id}.{dataset_id}.{table_id_geo}`
                WHERE name = '{city_name}'
            """
        )
        result = query_job.result()
        lat, lon = next(result)

        unit = "metric"
        base_url = "https://api.openweathermap.org/data/2.5/weather"

        params = {
            "lon": lon,
            "lat": lat,
            "appid": api_key,
            "units": unit,
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            weather_data = response.json()
            if "main" not in weather_data:
                continue
            if "coord" in weather_data:
                for ele in weather_data["coord"]:
                    weather_data[ele] = weather_data["coord"][ele]
                del weather_data["coord"]
            if "main" in weather_data:
                for ele in weather_data["main"]:
                    weather_data[ele] = weather_data["main"][ele]
                del weather_data["main"]
            weather_data["weather"] = str(weather_data["weather"])
            if "wind" in weather_data:
                weather_data["wind_speed"] = weather_data["wind"]["speed"]
                weather_data["wind_deg"] = weather_data["wind"]["deg"]
                del weather_data["wind"]
            if "clouds" in weather_data:
                weather_data["clouds"] = weather_data["clouds"]["all"]
            if "sys" in weather_data:
                weather_data["sunrise"] = weather_data["sys"]["sunrise"]
                weather_data["sunset"] = weather_data["sys"]["sunset"]
                del weather_data["sys"]
            if "rain" in weather_data:
                for ele in weather_data["rain"]:
                    weather_data["rain_" + ele] = weather_data["rain"][ele]
                del weather_data["rain"]
            if "snow" in weather_data:
                for ele in weather_data["snow"]:
                    weather_data["snow_" + ele] = weather_data["snow"][ele]
                del weather_data["snow"]
            if "cod" in weather_data:
                del weather_data["cod"]
            errors = client.insert_rows(table_weather, [weather_data])
            if errors:
                return f"Error inserting weather data into BigQuery: {errors}"
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
    return "ok"
