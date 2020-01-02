import os
import numpy as np

from WeatherRadarML import dataDir

def nexrad_station_info(convert_lon = True):
    """
        Name:
		NEXRAD_STATION_INFO
        Purpose:
		This is a function to read a NEXRAD station info file. 
        Calling sequence:
		value = nexrad_station_info()
    Inputs:
		None.
    Output:
		Dictionary of station IDs and coordinates
    Keywords:
        	None.
    Author and history:
		Cameron R. Homeyer  2010-10-20.
				  2015-06-07. Pasted contents from look-up file within to support
							IDL vm scripts for real-time processing.
    """

    km_to_ft = 3280.84; #Set conversion factor for elevation
    infoFile = os.path.join( dataDir, 'nexrad-stations.txt' )
    with open( infoFile, 'r' ) as fid:
        lines = [line.rstrip() for line in fid.readlines()]                     # Read in all lines, stripping off cariage return from each line
    lines  = lines[2:]                                                          # Get rid of first 2 lines
    nlines = len( lines )
    data   = {  'statid' : np.empty( (nlines,), dtype='<U4' ),
                'lon'    : np.full(  (nlines,), np.nan, dtype=np.float32 ),
                'lat'    : np.full(  (nlines,), np.nan, dtype=np.float32 ),
                'alt'    : np.full(  (nlines,), np.nan, dtype=np.float32 )}
    for i in range( nlines ):
        data['statid'][i] = lines[i][9:13]
        data['lon'][i]    = float(lines[i][116:126])
        data['lat'][i]    = float(lines[i][106:115])
        data['alt'][i]    = float(lines[i][127:133])

    if convert_lon:
        data['lon']  = (data['lon'] + 360.0) % 360.0

    data['alt'] /= km_to_ft
    
    return data
