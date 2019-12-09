#!/usr/bin/env python3
import os
from WeatherRadarML import plotUtils as utils
from WeatherRadarML.readWarnings import read_warnings
from WeatherRadarML.ASOSInfo import ASOSInfo
from datetime import datetime
from shapely.geometry import Polygon, Point, shape

def show_warnings(zipfile, start=None, end=None, show_only_stations_inside=False):
    # Get the warning areas
    records = read_warnings(zipfile, start, end)
    # Initialize the map
    ax = utils.baseMap()

    # stations = []
    # locations = []
    inside_station_indexes = []

    # Get a list of all available stations and their locations
    # info = ASOSInfo()._parseData()
    # for item in info:
    #     stations.append(item['CALL'])
    #     locations.append((float(item['LON']), float(item['LAT'])))
    stations, locations = ASOSInfo().get_stations()

    for record in records:
        # Add the record to the plot
        record.plot(ax)
        # Get only the indexes of the stations inside the warnings
        if show_only_stations_inside:
            for index in range(len(stations)):
                station_location = Point(locations[index])
                if station_location.within(record):
                    inside_station_indexes.append(index)

    if show_only_stations_inside:
        # Remove dulplicates in inside_station_indexes
        inside_station_indexes = list(set(inside_station_indexes))
        for index in inside_station_indexes:
            # Plot stations
            utils.plotStation(ax, stations[index], locations[index], color='r')
    
    # Plot stations
    if not show_only_stations_inside:
        for index in range(len(stations)):
            utils.plotStation(ax, stations[index], locations[index], color='r')
    
    # Show plot to user
    utils.plt.show()

if __name__ == "__main__":  
    show_warnings('/home/allen/Downloads/1986_all.zip', start=datetime(1986, 1, 1), end=datetime(1986, 3, 1), show_only_stations_inside=True)