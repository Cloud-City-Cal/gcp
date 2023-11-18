import requests
from bs4 import BeautifulSoup

BASE_URL = "https://pems.dot.ca.gov"
USERNAME = ""
PASSWORD = ""
def insert_into_bigquery(request):
    # station data could be downloaded from pems
    # df = pd.read_csv("stations.csv", sep="\t")
    # stations = df.iloc[:, 2]
    # Get the station id from big query or request
    stations = []

    session = requests.Session()
    # Log in
    session.post(
        BASE_URL,
        data={
            "username": USERNAME,
            "password": PASSWORD,
            "login": "Login",
        },
    )

    # Crawl Data
    for station_id in stations:
        params = {
            "report_form": 1,
            "dnode": "VDS",
            "content": "loops",
            "tab": "det_timeseries",
            "export": "",
            "station_id": station_id,
            # Get the data from the past 30 mins to now
            "s_time_id": int(time.time()) - 30 * 60,
            "e_time_id": int(time.time()),
            "tod": "all",
            "tod_from": 0,
            "tod_to": 0,
            "dow_0": "on",
            "dow_1": "on",
            "dow_2": "on",
            "dow_3": "on",
            "dow_4": "on",
            "dow_5": "on",
            "dow_6": "on",
            "holidays": "on",
            "q": "speed",
            "q2": "",
            "gn": "5min",
            "agg": "on",
            "lane1": "on",
            "lane2": "on",
            "lane3": "on",
            "lane4": "on",
            "lane5": "on",
            "lane6": "on",
            "lane7": "on",
            "html.x": 31,
            "html.y": 7,
        }
        res = session.post(
            BASE_URL,
            params=params,
        )
        soup = BeautifulSoup(res.text, "lxml")
        table = soup.find("table",{"class": "inlayTable"})
        tbody = table.find("tbody")
        trs = tbody.find_all("tr")
        # Iterate each row for different timestamp
        for tr in trs:
            tds = tr.find_all("td")
            l = len(tds)
            time = tds[0].text # string format
            # if the data at a given timestamp exist in database:
            #     break
            insert_data = {"time": time}
            insert_data["observed_percentage"] = tds[l-1].text
            insert_data["lane_points"] = tds[l-2].text
            insert_data["agg_speed"] = tds[l-3].text
            for i in range(1, l-3):
                insert_data[f"lane{i}"] = tds[i].text
            # print(insert_data)
            # insert data into database
