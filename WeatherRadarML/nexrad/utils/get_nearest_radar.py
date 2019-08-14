import logging

import numpy as np
from geopy.distance import geodesic
from shapely.geometry import Polygon, Point
from pyart.core import antenna_to_cartesian, cartesian_to_geographic

from .nexrad_station_info import nexrad_station_info

proj = {'proj' : 'pyart_aeqd', 'lon_0' : 0.0, 'lat_0' : 0.0}					# Base projection dictionary

def get_nearest_radar(lon, lat, max_range = 300):
	"""
	Purpose:
		Function to determine closest radar(s) based on lon/lat.
		If more than one radar found, they are sorted from nearest
		to farthest.
	Inputs:
		lon  : Longitude of point
		lat  : Latitude of point
	Keywords:
		max_range : Maximum distance (in kilometers) to look
						from a radar to see if the point is
						near it.
	Outputs:
		Returns a list containing tuples with (station id, lon, lat, altitude)
		if any stations found. If no stations found, list will be empty
	"""
	if (lon > 180.0): lon -= 360.0												# Convert longitude to -180 to 180 range
	point      = Point( [lon, lat] )											# Define shapely point
	xx, yy, zz = antenna_to_cartesian( max_range, np.arange(360), 0)			# Generate cartesian coordinates from radar site based on max_range; 360 gives circle
	statInfo   = nexrad_station_info(convert_lon = False)						# Get station information; False flag keeps longitudes in -180 to 180 range
	match      = []																# Initialize match to empty list
	for i in range( statInfo['statid'].size ):									# Iterate over all stations in the station information
		proj['lon_0'] = statInfo['lon'][i]										# Set longitude for projection to station longitude
		proj['lat_0'] = statInfo['lat'][i]										# Set latitude for projection to station latitude
		lonLat        = np.asarray( cartesian_to_geographic(xx, yy, proj) ).T	# Convert cartesiain radar range limits to geographic coordinates
		poly          = Polygon( lonLat )										# Generate polygon using lonLat
		if poly.contains( point ):												# If the polygon contains user requested point
			match.append( (statInfo['statid'][i], statInfo['lon'][i],
                   statInfo['lat'][i],    statInfo['alt'][i],) )				# Append tuple of station information to match list

	if (len(match) > 1):														# If the match list has more than 1 point
		origin = (lat, lon, )													# Define origin as user point
		match  = sorted( match, key = lambda x: geodesic(origin, x[2:0:-1] ) ) 	# Sort station list from closest to farthest 

	return match																# Return match
