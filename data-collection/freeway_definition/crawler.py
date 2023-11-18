import requests, xmltodict, json
I880_route_id = ["74660", "156348"]

for route_id in I880_route_id:
    res = requests.get(f"https://www.openstreetmap.org/api/0.6/relation/{route_id}")
    ways = {}
    endpoints_to_way_ref = {}
    route_data = xmltodict.parse(res.text)
    for member in route_data["osm"]["relation"]["member"]:
        way_ref = member["@ref"]
        res = requests.get(f"https://www.openstreetmap.org/api/0.6/way/{way_ref}")
        way_data = xmltodict.parse(res.text)
        nodes = []
        for node in way_data["osm"]["way"]["nd"]:
            node_ref = node["@ref"]
            res = requests.get(f"https://www.openstreetmap.org/api/0.6/node/{node_ref}")
            node_data = xmltodict.parse(res.text)
            lat = node_data["osm"]["node"]["@lat"]
            lon = node_data["osm"]["node"]["@lon"]
            nodes.append([lon, lat])
        ways[way_ref] = nodes
        start_point = tuple(nodes[0])
        end_point = tuple(nodes[-1])
        for point in [start_point, end_point]:
            if point not in endpoints_to_way_ref:
                endpoints_to_way_ref[point] = [way_ref]
            else:
                endpoints_to_way_ref[point].append(way_ref)
    cnt = 0
    route_start_point = None
    for point in endpoints_to_way_ref:
        if len(endpoints_to_way_ref[point]) == 1:
            cnt += 1
            route_start_point = point
            route_start_ref = endpoints_to_way_ref[point][0]
        elif len(endpoints_to_way_ref[point]) > 2:
            raise Exception("these is an intersection")
    assert cnt == 2
    points = []
    cur_point = route_start_point
    cur_ref = route_start_ref
    while(len(endpoints_to_way_ref[cur_point]) > 1):
        if endpoints_to_way_ref[cur_point][0] == cur_ref:
            cur_ref = endpoints_to_way_ref[cur_point][1]
        else:
            cur_ref = endpoints_to_way_ref[cur_point][2]
        nodes = ways[cur_ref]
        if tuple(nodes[0]) != cur_point:
            nodes = nodes[::-1]
        if point:
            points += nodes[1:]
        else:
            points = nodes
        cur_point = tuple(points[-1])
    with open(f"points_{route_id}.json", "w") as f:
        json.dump(points, f)



