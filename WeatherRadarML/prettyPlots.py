from WeatherRadarML.ASOSInfo import ASOSInfo
from WeatherRadarML import plotUtils as utils
import numpy as np
import matplotlib.pyplot as plt
from pyart.core import antenna_to_cartesian, cartesian_to_geographic

def radar_range(station):
    '''
    Purpose:
        Plot a station and the range of it's radar
    Inputs:
        station (String): The name of the station
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
    xx, yy, zz = antenna_to_cartesian(.003, np.arange(360), 0)
    # Offset the circle
    for index in range(len(xx)):
        xx[index] += lon
        yy[index] += lat

    # Create the map
    ax = utils.baseMap()
    plt.fill(xx, yy, alpha=.6)
    utils.plotStation(ax, station, [lon, lat])
    plt.show()
    

if __name__ == "__main__":
    test('HOU')