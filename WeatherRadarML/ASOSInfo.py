import os
from datetime import datetime
import requests
import csv
import json
# from WeatherRadar import WeatherRadar

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
            if (self._keys is not None) and (self._keys[i] == 'BEGDT') and (info != 'BEGDT'):
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

    def _parse_page(self, file=os.path.join(_dir, 'data', 'scraper.txt'), destination=os.path.join(_dir, 'data', 'json.txt')):
        """
        Name:
            _parse_page
        Purpose:
            Parse the data from the page into the dictionary structure
        Inputs:
            file (string):
                The path to the file the page has been downloaded to
            destination (string):
                Where to put the JSON file
        Keywords:
            None.
        Returns:
            A JSON file
        """
        stations_dict = []

        with open(file, 'r') as f:
            # Allow the file to be read
            csvfile = csv.reader(f)

            # Skip the header
            next(f)

            # Get unique stations from the csv file
            stations = []
            for line in csvfile:
                stations.append(line[0])
            stations = list(set(stations))

            # Create a dictionary for each station
            for station in stations:
                station_name = station
                stations_dict.append({'station': {'name': station_name, 'coords': []}, 'date': [], 'precip': [], 'temp': []})

            # Go back to the top of the file and skip the header
            f.seek(0)
            next(f)

            # If the station is equivalent, append the information about it
            for line in csvfile:
                for station in stations_dict:
                    if line[0] == station['station']['name']:
                        station['station']['coords'] = [float(line[2]), float(line[3])]
                        station['date'].append(line[1])
                        station['precip'].append(line[4])
                        station['temp'].append(line[5])
            
            # Write to the JSON file
            with open('/home/allen/Desktop/Code/data.txt', 'w') as f:
                for station in stations_dict:
                    f.write(json.dumps(station, indent=4))
                    f.write('\n')

    def get_stations(self):
        """
        Name:
            get_stations
        Purpose:
            Generate a list of stations and their locations
        Inputs:
            None.
        Keywords:
            None.
        Returns:
            A list of stations and their locations
        """
        stations = []
        locations = []

        info = ASOSInfo()._parseData()
        for item in info:
            stations.append(item['CALL'])
            locations.append((float(item['LON']), float(item['LAT'])))

        return stations, locations