import json
import pandas as pd

def one_hot_encode_weather_forecasts(forecasts):
    encoded_data = []
    for forecast in forecasts:
        encoded_forecast = {
            'Sunny': 1 if 'Sunny' in forecast['shortForecast'] or 'Clear' in forecast['shortForecast'] else 0,
            'Cloudy': 1 if 'Cloudy' in forecast['shortForecast'] else 0,
            'Rainy': 1 if 'Rain' in forecast['shortForecast'] else 0,
            'Fog': 1 if 'Fog' in forecast['shortForecast'] else 0,
            'Snow': 1 if 'Snow' in forecast['shortForecast'] else 0
        }
        encoded_data.append(encoded_forecast)
    return encoded_data

# Read JSON File
#　This is sample data of my PC
with open('/Users/nozomukitamura/Downloads/forecastHourly_MTR_95_85_2023-11-09T20-26-34-00-00.json', 'r') as file:
    data = json.load(file)

# One-hot encode weather forecasts
encoded_weather_forecasts = one_hot_encode_weather_forecasts(data['properties']['periods'])

weather_forecast_df = pd.DataFrame(encoded_weather_forecasts)

print(weather_forecast_df.head())

# Read csv File
#　This is sample data of my PC
df = pd.read_csv('/Users/nozomukitamura/Downloads/event.csv')

# One-hot encode event types
event_types = ['CONSTRUCTION', 'SPECIAL_EVENT', 'INCIDENT', 'WEATHER_CONDITION', 'ROAD_CONDITION']
for event_type in event_types:
    df[event_type] = df['event_type'].str.contains(event_type).astype(int)
# One-hot encode event types
encoded_event_types_df = df[event_types]

print(encoded_event_types_df.head())
