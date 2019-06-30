import os
from datetime import datetime

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
            countries:
        Purpose:
            A method to return a list of all countries from the file
        Inputs:
            None.
        Keywords:
            None.
        Returns:
            List of strings
        """
        return sorted( list( set( [d['COUNTRY'] for d in self.data] ) ) );          # Iterate over all dictionaries in the data list and get the COUNTRY, then get all unique countires using the set() function, convert that to a list using list() function, then sort values using sorted()
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
        return sorted( list( set( [d['ST'] for d in self.data] ) ) );               # Same as for countries, but using the 'ST" key for states
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
        if len(state) != 2:                                                         # If state is NOT 2 characters 
            print('Must use 2 character abbreviation for state');                   # Print message
            return [];                                                              # Return empty string
        state = state.upper();                                                      # Convert state to upper case
        return [d for d in self.data if d['ST'] == state];                          # Iterate over all dictionaries in the data list, keeping only those where the 'ST' value matches state
    ###########################################################################
    def _headParser(self, header, headSep):
        """
        Name:
            _headParser
        Purose:
            A method to parse information from header
        Inputs:
            header   : Header line from file; first line
            headSep  : Separator line between header and data; second line
        Keywords:
            None.
        Returns:
            None. Sets _keys and _colWid attributes
        """
        self._colWid = [len(x) for x in headSep.split()];                               # Split the headSep line on space, iterate over each element of the list returned from .split() and get length
        self._keys   = self.lineParser( header )
    
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
        data     = [];                                                              # Empty list that will hold data for each column
        offset   = 0;                                                               # Offset for extracting information from the line
        for i in range( len(self._colWid) ):                                        # Iterate over all columns; i.e,, the number of elements in the colWidth list
            info = line[ offset:offset+self._colWid[i] ].strip();                   # Extract information from the line; start at character `offset` and end with character `offset+colWidth[i]`, .strip() removes spaces at beginning/end of string
            if (self._keys is not None) and (self._keys[i] == 'BEGDT'):
                info = datetime.strptime(info, '%Y%m%d');
            elif info.isdigit():                                                    # If the string is all digits (does not include decimals
                info = int(info);                                                   # Convert info to integer
            else:                                                                   # Else, it is either string of float
                try:                                                                # Try to...
                    info = float(info);                                             # Convert info to float
                except:                                                             # If the conversion fails; must be a string
                    pass;                                                           # Do nothing; fail silently

            data.append( info );                                                    # Append info the string to the data list
            offset += self._colWid[i]+1;                                            # Increment the counter by colWidth[i]+1, the +1 is to account for the space between columns
        data = dict( zip(self._keys, data) );                                       # Zip up the _keys and data lists and convert to dictionary
        return data                                                                 # Return the data dictionary from function
    
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
        with open(self._file, 'r') as fid:                                          # Open file for reading
            header  = fid.readline();                                               # Read first line of file; this is the header
            headSep = fid.readline();                                               # Read second line of file; this is hyphens that separates the header from the data
            self._headParser( header, headSep );                                    # Parse the header line; this will set the _colWid and _keys attributes
            data    = [];                                                           # Initialize list to store all data 
            for line in fid.readlines():                                            # Iterate over all lines in the file
                info = self._lineParser( line );                                    # Parse the line, returning a dictionary of information
                data.append( info );                                                # Append the dictionary to the data list
        return data                                                                 # Return the data list

