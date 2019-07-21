import os

from .. import NEXRAD_STATION_ID_LIST

def nexrad_level2_directory(date, station = NEXRAD_STATION_ID_LIST, root = '/data1/' ):
    """
    Name:
        nexrad_level2_directory
    Purpose:
        Return the full paths to the local directories containing NEXRAD
        Level 2 data for the specified date and station IDs
    Inputs:
        date    : Datetime for which the directory paths are to be created
    Keywords:
        station : Scalar string or string list containing the station IDs
                    for which directories are to be created.
                    DEFAULT: directories are created for all of the
                    stations in the nexrad.NEXRAD_STATION_ID_LIST
        root    : Top-level root directory. Default value depends on the
                    date of the data selected
    Returns:
        Returns three values:
            - Scalar string or list of strings containing full path(s) to 
                directory(s) of Level 2 files for the date and station(s)
                specified
            - The parent directory of the directories returned in first
                return value. This is the full path to those directories
                without the station ID component
            - The full root directory of Level 2 files for the 
                specified date
    """
    yyyy     = date.strftime( '%Y' )
    yyyymm   = date.strftime( '%Y%m' ) 
    yyyymmdd = date.strftime( '%Y%m%d' ) 

    root     = os.path.join( root, 'NEXRAD', 'level2' )
    parent   = os.path.join(root, yyyy, yyyymm, yyyymmdd, '');

    if not isinstance(station, (list, tuple,)): station = [station];              # If station variable is NOT a list or tuple, make it a tuple

    return [os.path.join( parent, s, '' ) for s in station], parent, root 
