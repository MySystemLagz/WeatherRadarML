# Use point.within(shape) or shape.contains(point) to check if a point is within a shape
# .shp and .shx files rely on each other, keep them in the same directory at all times

from shapely.geometry import Polygon, Point, shape
import fiona

# A list of weather stations, name and location, represented as points
stations = [['KDWH', (30.062, -95.553)], ['KIAH', (29.985, -95.342)], ['KTME', (29.8050278, -95.8978889)], ['KHOU', (29.646, -95.279)], ['KSGR', (29.622, -95.657)], ['KLVJ', (29.521, -95.242)], ['KAXH', (29.5061389, -95.4769167)]]

# Extract information from shapefile here
collection = fiona.open("wwa_201801010000_201812312359.shp")

# Loop through the weather stations and check if any of them are in bounds of the polygon
counter = 0
station_counter = 0
in_bound = False
while counter < 4107:
    record = next(collection)
    coords = record['geometry']['coordinates'][0]
    if len(coords) >= 3:
        try:
            poly = Polygon(coords)
        except AssertionError:
            counter += 1
            continue
    else:
        counter += 1
        continue

    while station_counter < len(stations):
        location = Point(stations[station_counter][1])
        station = stations[station_counter][0]
        if location.within(poly):
            print(station, '- In bounds')
            in_bound = True
        station_counter += 1
        if station_counter == len(stations) and in_bound == True:
            print('-------------------')
    counter += 1
    station_counter = 0
    in_bound = False