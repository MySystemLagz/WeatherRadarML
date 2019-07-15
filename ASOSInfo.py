import os
from datetime import datetime
import requests
import csv

_dir  = os.path.realpath( os.path.dirname(__file__) )
_asos = os.path.join( _dir, 'data', 'asos-stations.txt' )


class ASOSInfo( object ):
    def __init__(self, infile = _asos ):
        self._file   = infile
        self._keys   = None
        self._colWid = None
        self.data    = self._parseData()
    
    ###########################################################################
    def countries(self):
        """
        Name:
            countries
        Purpose:
            A method to return a list of all countries from the file
        Inputs:
            None.
        Keywords:
            None.
        Returns:
            List of strings
        """
        return sorted( list( set( [d['COUNTRY'] for d in self.data] ) ) )          # Iterate over all dictionaries in the data list and get the COUNTRY, then get all unique countires using the set() function, convert that to a list using list() function, then sort values using sorted()
    ###########################################################################
    def states(self):
        """
        Name:
            states:
        Purpose:
            A method to return a list of all states from the file
        Inputs:
            None.
        Keywords:
            None.
        Returns:
            List of strings
        """
        return sorted( list( set( [d['ST'] for d in self.data] ) ) )               # Same as for countries, but using the 'ST" key for states
    ###########################################################################
    def getState(self, state):
        """
        Name:
            getState:
        Purpose:
            A method to return a list of all stations in a given state
        Inputs:
            state : As string containing the 2-character abbreviation for a state
        Keywords:
            None.
        Returns:
            List of dictionaries with infomation for each station in the state
        """
        if len(state) != 2:                                                        # If state is NOT 2 characters 
            print('Must use 2 character abbreviation for state')                   # Print message
            return []                                                              # Return empty string
        state = state.upper()                                                      # Convert state to upper case
        return [d for d in self.data if d['ST'] == state]                          # Iterate over all dictionaries in the data list, keeping only those where the 'ST' value matches state
    ###########################################################################
    def _headParser(self, header, headSep):
        """
        Name:
            _headParser
        Purose:
            A method to parse information from header
        Inputs:
            header   : Header line from file first line
            headSep  : Separator line between header and data second line
        Keywords:
            None.
        Returns:
            None. Sets _keys and _colWid attributes
        """
        self._colWid = [len(x) for x in headSep.split()]                               # Split the headSep line on space, iterate over each element of the list returned from .split() and get length
        self._keys   = self._lineParser( header )
    
    ###########################################################################
    def _lineParser( self, line ):
        """
        Name:
            _lineParser
        Purpose:
            A function to parse apart a line from the asos-stations.txt file
            obtained from the NCDC
        Inputs:
            line    : A string containing the line to parse
        Keywords:
            None.
        Returns:
            Returns a list where each element is the data from a column
        """
        data     = []                                                              # Empty list that will hold data for each column
        offset   = 0                                                               # Offset for extracting information from the line
        for i in range( len(self._colWid) ):                                       # Iterate over all columns i.e, the number of elements in the colWidth list
            info = line[ offset:offset+self._colWid[i] ].strip()                   # Extract information from the line start at character `offset` and end with character `offset+colWidth[i]`, .strip() removes spaces at beginning/end of string
            if (self._keys is not None) and (self._keys[i] == 'BEGDT'):
                info = datetime.strptime(info, '%Y%m%d')
            elif info.isdigit():                                                   # If the string is all digits (does not include decimals
                info = int(info)                                                   # Convert info to integer
            else:                                                                  # Else, it is either string of float
                try:                                                               # Try to...
                    info = float(info)                                             # Convert info to float
                except:                                                            # If the conversion fails must be a string
                    pass                                                           # Do nothing fail silently

            data.append( info )                                                    # Append info the string to the data list
            offset += self._colWid[i]+1                                            # Increment the counter by colWidth[i]+1, the +1 is to account for the space between columns
        return data                                                                # Return the data list from function
    
    ###########################################################################
    def _parseData(self):
        """
        Name:
            _parseData:
        Purpose:
            A 'private' method to parse the data file input by user.
        Inputs:
            None.
        Keywords:
            None.
        Returns:
            List of dictionaries containing data for each station.
        """
        with open(self._file, 'r') as fid:                                         # Open file for reading
            header  = fid.readline()                                               # Read first line of file this is the header
            headSep = fid.readline()                                               # Read second line of file this is hyphens that separates the header from the data
            self._headParser( header, headSep )                                    # Parse the header line this will set the _colWid and _keys attributes
            data    = []                                                           # Initialize list to store all data 
            for line in fid.readlines():                                           # Iterate over all lines in the file
                info = self._lineParser( line )                                    # Parse the line, returning a list of information
                info = dict( zip(self._keys, info) )                               # Zip up the _keys and info lists and convert to dictionary
                data.append( info )                                                # Append the dictionary to the data list
        return data                                                                # Return the data list

    def download_data(self, service='https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?', stations=['AXH', 'DWH', 'EFD', 'HOU', 'IAH', 'LVJ', 'MCJ', 'SGR', 'TME'], data=['tmpc'], year1='2018', month1='1', day1='1', year2='2018', month2='12', day2='31', tz='Etc/UTC', format='onlycomma', latlon='yes', missing='M', trace='T', direct='no', report_type=['1', '2'], file=os.path.join(_dir, 'data', 'scraper.txt')):
        """
        Name: 
            _get_page
        Purpose:
            Builds the page url to get data from IEM ASOS
        Inputs:
            service (string):
                The base url
            stations (list):
                A list of 3 or 4 letter station names
            data (string):
                A list of data to retrieve
            year1 (string):
                A 4 digit number of the starting year
            month1 (string):
                A 1 digit number of the starting month
            day1 (string):
                A 1 digit number of the starting day
            year2 (string):
                A 4 digit number of the ending year
            month2 (string):
                A 1 digit number of the ending month
            day2 (string):
                A 1 digit number of the ending day
            tz (string):
                Timezone
            format (string):
                How the data should be formated
            latlon (string):
                Whether or not the latitude and longitude should be shown
            missing (string):
                What to represent missing data as
            trace (string):
                What to represent trace reports as
            direct (string):
                Whether or not the data should be downloaded
            report_type (list):
                The report types to include
            file (string):
                The path to the file the downloaded data will be put into
        Keywords:
            None.
        Returns:
            A response that can be used to get information about the webpage
        """
        payload = {
            'station': stations, 
            'data': data, 
            'year1': year1,
            'month1': month1,
            'day1': day1,
            'year2': year2,
            'month2': month2,
            'day2': day2,
            'tz': tz,
            'format': format,
            'latlon': latlon,
            'missing': missing,
            'trace': trace,
            'direct': direct,
            'report_type': report_type
        }

        page = requests.get(service, params=payload)

        with open(file, 'wb') as fd:
            for chunk in page.iter_content(chunk_size=512):
                fd.write(chunk)

    def _parse_page(self, file=os.path.join(_dir, 'data', 'scraper.txt'), lonlat=True):
        """
        Name:
            _parse_page
        Purpose:
            Parse the data from the page into the dictionary structure
        Inputs:
            file (string):
                The path to the file the page has been downloaded to
            lonlat (bool):
                Whether or not the file includes information about the longitude and latitude of the event
        Keywords:
            None.
        Returns:
            A dictionary using the data from the file
        """
        data = {}

        with open(file, 'r') as f:
            csvfile = csv.reader(f) # Read as a csv file
            next(f) # Skip the header

            stations = [] # A list of unique stations

            for line in csvfile:
                stations.append(line[0]) # Add station to the stations list
                stations = list(set(stations)) # Get rid of any duplicates in the stations list

                line[1] = datetime.strptime(line[1], '%Y-%m-%d %H:%M') # Convert to a datetime object
                if lonlat:
                    line[2] = tuple([float(line[2]), float(line[3])]) # Convert lon and lat to tuple
                    del line[3] # Get rid of the extra value

                for station in stations:
                    if line[0] == station:
                        data.setdefault(station, []).append(line[1:len(line)]) # Append to the list of list of data that came from the station

            return data