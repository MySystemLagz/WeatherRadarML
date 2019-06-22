# Use Fiona, geopandas, or shape to read .shp files, but first get it working
# Use point.within(shape) or shape.contains(point) to check if a point is within a shape

from shapely.geometry import Polygon, Point, shape
import fiona

# A list of weather stations, name and location, represented as points
stations = [['KDWH', (30.062, -95.553)], ['KIAH', (29.985, -95.342)], ['KTME', (29.8050278, -95.8978889)], ['KHOU', (29.646, -95.279)], ['KSGR', (29.622, -95.657)], ['KLVJ', (29.521, -95.242)], ['KAXH', (29.5061389, -95.4769167)]]

# Extract information from shapefile here
collection = fiona.open("wwa_201801010000_201812312359.shp")
record = next(collection)
coords = record['geometry']['coordinates'][0]
# Convert to shapely geometry
poly = Polygon(coords)

# Loop through the weather stations and check if any of them are in bounds of "poly"
# Update "poly" to the actual bounds of the flash flood weather reports
# Later upgrade to 2D loop to also loop through "poly"
counter = 0
while counter < len(stations):
    location = Point(stations[counter][1])
    station = stations[counter][0]
    if location.within(poly):
        print(station, '- In bounds')
    else:
        print(station, '- Outta bounds')
    counter += 1