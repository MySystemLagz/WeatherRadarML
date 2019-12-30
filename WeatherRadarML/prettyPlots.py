from WeatherRadarML.ASOSInfo import ASOSInfo
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
    stations, locations = ASOSInfo().get_stations()

    location = (0, 0)

    # Get specific station
    for index in range(len(stations)):
        if stations[index] == station:
            location = locations[index]

    # Get the longitude and latitude of the station
    lon = location[0]
    lat = location[1]

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
    radar_range('HOU', 300)