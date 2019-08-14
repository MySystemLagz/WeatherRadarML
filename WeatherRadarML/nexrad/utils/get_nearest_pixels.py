import logging

import pyart
import numpy as np
from scipy.spatial import KDTree

from .get_nearest_radar import get_nearest_radar
from .nexrad_level2_directory import nexrad_level2_directory

def get_nearest_pixels(lon, lat, date, field, k = 9, max_dist = 1.0):
	"""
	Purpose:
		Function to get all pixels of given field closest to 
		user specified location and date
	Inputs:
		lon   : Longitude of point to get radar data for
		lat   : Latitude of point ot get radar data for
		date  : Datetime to get radar data for
		field : Radar data field to get
	Keywords:
		k     : Number of pixels to return.
					Default is the 9 closest pixels to user
					requested point
		max_dist : Maximum distance (km) a radar pixel can
					be from user specified point for it to be
					considered
	Outputs:
		Returns an [nsweep, k] array with the closest pixels to
		user specified points at each sweep angle
	"""

	radars        = get_nearest_radar( lon, lat )
	if (len(radars) == 0): return None

	dataDir       = nexrad_level2_directory(date, station = radars[0][0])

	# Must add code to find closest radar file based on time
	nexradFile    = '/data1/NEXRAD/level2/2019/201901/20190101/KHGX/KHGX20190101_000344_V06'
	radar         = pyart.io.read(nexradFile, delay_field_loading=True)			# Read in NEXRAD file; enable delayed field loading to save memory
	nSweeps       = radar.sweep_number['data'].size								# Number of sweeps in the file
	out           = np.empty( (nSweeps, k,), dtype = np.float32 )				# Initialize numpy array to hold data

	proj          = radar.projection											# Get radar projection
	proj['lon_0'] = radar.longitude['data']										# Set projection longitude
	proj['lat_0'] = radar.latitude['data']										# Set projecion lattitude
	xy            = pyart.core.geographic_to_cartesian(lon, lat, proj)			# Convert user point to cartesian coordinates
	xy            = np.asarray( xy ).T											# Convert to numpy array and transpose

	for sweep in range( nSweeps ):												# Iterate over all radar sweeps
		x, y, z      = radar.get_gate_x_y_z( sweep )							# Get x, y, z values for the sweep
		tree         = KDTree( np.dstack( [x.ravel(), y.ravel()] ).squeeze() )	# Create KDTree for finding nearest neighbor
		dist, ids    = tree.query( xy, k = k )									# Get closets k points to user point
		ids          = ids[ np.where( dist <= max_dist ) ]						# Limit closest points by max_dist
		if (ids.size > 0):														# If any points left
			nids = ids.size														# Number of points left
			ids  = np.unravel_index( ids, x.shape )								# Unravel the 1d incides
			out[sweep,slice(nids)] = radar.get_field(sweep, field)[ids]			# Store values in the output data array

	return out																	# Return out array

