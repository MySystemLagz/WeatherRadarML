# Given an list of at least 3, calculate whether x is within the bounds of those points
# Use the shapely package
# Use Fiona or shape to read .shp files, but first get it working
# Use point.within(shape) or shape.contains(point) to check if a point is within a shape

from shapely.geometry import Polygon, Point
# import fiona

point = Point(2, 2)
poly = Polygon([(0, 0), (5, 0), (5, 5), (0, 5)])

# A list of stations, name and location, represented as points
stations = [['KDWH', (30.062, -95.553)], ['KIAH', (29.985, -95.342)], ['KTME', (29.8050278, -95.8978889)], ['KHOU', (29.646, -95.279)], ['KSGR', (29.622, -95.657)], ['KLVJ', (29.521, -95.242)], ['KAXH', (29.5061389, -95.4769167)]]

if point.within(poly):
    print('In bounds')
else:
    print('Outta bounds')