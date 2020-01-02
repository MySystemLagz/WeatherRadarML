from WeatherRadarML.nexrad.utils.nexrad_station_info import nexrad_station_info
from WeatherRadarML import plotUtils as utils
import numpy as np
import matplotlib.pyplot as plt
from pyart.core import antenna_to_cartesian, cartesian_to_geographic

def radar_range(station, kmrange=300.0):
    '''
    Purpose:
        Plot a station and the range of its radar
    Inputs:
        station (String): The name of the station
        kmrange (Float): The radius of the radar's range in km
    Keywords:
        None.
    Outputs:
        Displays the plot to the user
    '''
    # Get stations
    nexradInfo = nexrad_station_info()

    # Get specific station
    lon = lat = None
    for index in range(nexradInfo['statid'].size):
        if nexradInfo['statid'][index] == station:
            lon = nexradInfo['lon'][index]
            lat = nexradInfo['lat'][index]

    if not lon:                                                                 # If longitude is None
        raise Exception('Station: {} NOT found in list of NEXRAD stations!'.format(station) )# Raise exception

    # Create a circle
    xx, yy, zz = antenna_to_cartesian(kmrange, np.arange(360), 0)
    proj = {'proj' : 'pyart_aeqd', 'lon_0' : lon, 'lat_0' : lat}
    xx, yy = cartesian_to_geographic(xx, yy, proj)

    # Create the map
    ax = utils.baseMap()
    plt.fill(xx, yy, alpha=.6)
    utils.plotStation(ax, station, [lon, lat])
    plt.show()
    

if __name__ == "__main__":
    radar_range('KHGX', 300)
