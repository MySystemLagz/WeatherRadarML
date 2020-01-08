#!/usr/bin/env python3
import os
from WeatherRadarML import plotUtils as utils
from WeatherRadarML.readWarnings import read_warnings
from WeatherRadarML.ASOSInfo import ASOSInfo
from datetime import datetime
from shapely.geometry import Polygon, Point, shape

def show_warnings(zipfile, start=None, end=None, show_only_stations_inside=True):
    '''
    Name:
        show_warnings
    Purpose:
        Display warnings and stations
    Inputs:
        zipfile (string):
    Keywords:
        None.
    Outputs:
    '''
    # Get the warning areas
    records = read_warnings(zipfile, start, end)
    # Initialize the map
    ax = utils.baseMap()

    # Get the stations and their locations
    stations, locations = ASOSInfo().get_stations()

    for record in records:
        # Plot the warning
        record.plot(ax)
        for location in locations:
            if show_only_stations_inside:
                # Convert location to a Point object
                station_location = Point(location)
                # Check if the station is within the bounds of the warning
                if record.is_valid and station_location.within(record):
                    # Plot the station
                    utils.plotStation(ax, stations[locations.index(location)], location, color='r')
            else:
                # Break the loop if show_only_stations_inside is False
                break
    
    # Plot all stations if show_only_stations_inside is False
    if not show_only_stations_inside:
        for location in locations:
            utils.plotStation(ax, stations[locations.index(location)], location, color='r')
    
    # Show plot to user
    utils.plt.show()

def show_stations_by_state(state):
    ax = utils.baseMap()

    stations = []
    locations = []

    info = ASOSInfo()._parseData()
    for item in info:
        if item['ST'] == state:
            stations.append(item['CALL'])
            locations.append((float(item['LON']), float(item['LAT'])))

    for location in locations:
            utils.plotStation(ax, stations[locations.index(location)], location, color='r')

    utils.plt.show()

if __name__ == "__main__":  
    # show_warnings('/home/allen/Downloads/2017_all.zip', start=datetime(2017, 8, 25), end=datetime(2017, 8, 29), show_only_stations_inside=True)
    show_warnings('WeatherRadarML/data/Pickles/2017082800-2017082803.pic', show_only_stations_inside=True)
    # show_stations_by_state('TX')