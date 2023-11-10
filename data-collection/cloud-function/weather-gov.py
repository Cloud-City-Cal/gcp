from requests import get
import json


# Sunnyvale, change this to any city you want
LAT = 37.386753200000044
LON = -122.01321459999997

grid = get(f"https://api.weather.gov/points/{LAT},{LON}").json()['properties']
print(grid['gridId'], grid['gridX'], grid['gridY']) # for full grids, see https://www.weather.gov/mtr/

weather_data = get(grid['forecastGridData']).json() # numerical data
weather = get(grid['forecastHourly']).json() # text data
print(weather_data["properties"]["updateTime"])

loc_and_time = f'{grid["gridId"]}_{grid["gridX"]}_{grid["gridY"]}_{weather_data["properties"]["updateTime"].replace(":", "-").replace("+","-")}'

with open(f'forecastGridData_{loc_and_time}.json', 'w', encoding='utf-8') as f:
    json.dump(weather_data, f, ensure_ascii=False, indent=4)

with open(f'forecastHourly_{loc_and_time}.json', 'w', encoding='utf-8') as f:
    json.dump(weather, f, ensure_ascii=False, indent=4)