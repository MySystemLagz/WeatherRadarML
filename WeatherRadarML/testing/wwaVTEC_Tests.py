#!/usr/bin/env python3
import os
import fiona
import matplotlib.pyplot as plt
from WeatherRadarML.wwaVTEC import wwaVTEC
from WeatherRadarML import plotUtils

fileDir = os.path.dirname( os.path.abspath( __file__ ) )                        # Get absolute path to directory of this file
dataDir = os.path.join( os.path.dirname(fileDir), 'data' )                      # Set path to data directory
shpFile = os.path.join( dataDir, '1986_all', 'wwa_198601010000_198701010000.shp' )# Set path to shape file

if __name__ == "__main__":
    ax   = plotUtils.baseMap(extent = (-100, -92, 27, 36,) )                    # Create basemap

    i = 0                                                                       # Simple counter
    for record in fiona.open(shpFile):                                          # Open shape file and iterate over all records
        wwaVTEC(record).plot( ax )                                              # CReate wwaVTEC instance using record and plot on the map
        i += 1                                                                  # Increment i
        if i > 100: break                                                       # If i is > 100, then break loop
    plt.show()                                                                  # Show the plot
