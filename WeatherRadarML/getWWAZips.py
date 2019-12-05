#!/usr/bin/env python3
import os, time
from datetime import datetime
from urllib.request import urlopen

_baseURL   = 'https://mesonet.agron.iastate.edu/pickup/wwa/'                    # Base URL for all wwa zip files
_startYear = 1986                                                               # Start year for zip files
def getWWAZips( outDir ):
    if not os.path.isdir( outDir ): os.makedirs( outDir )                       # If output directory does NOT exist, create it

    t0      = time.time()
    totSize = 0
    for year in range(_startYear, datetime.now().year+1):                       # Iterate from start year to current year
        fileName = '{}_all.zip'.format(year)                                    # Set file name based on year
        outFile  = os.path.join( outDir, fileName )                             # Set outFile using outDir and fileName 
        url      = _baseURL + fileName                                          # Set url using _baseURL and file name
        try:                                                                    # Try to
            res  = urlopen( url )                                               # Open the url
            size = int(res.getheader('Content-Length') )                        # Get size of remote file
        except:                                                                 # On exception
            print( 'Failed to open URL: {}'.format( url ) )                     # Print error
            continue                                                            # Skip to next file

        if os.path.isfile( outFile ):                                           # If the output file exists
            if (os.stat(outFile).st_size == size):                              # If the local file's size matches the remote size
                print( 'File already downloaded: {}'.format(outFile) )
                continue                                                        # Skip to next file
            else:
                print( 'File size missmatch, redownloading: {}'.format(outFile) )

        print( 'Downloading: {} --> {}'.format(url, outFile) )                  # Print information
        with open(outFile, 'wb') as fid:                                        # Open local file for writing
            fid.write( res.read() )                                             # Read data from URL and write to file
        totSize += size                                                         # Increment total size of download

    if (size > 0):                                                              # If size greater than zero
        dt = time.time()-t0                                                     # Compute download time
        print( 'Downloaded {} bytes in {} seconds'.format(totSize,dt) )         # Print information


if __name__ == "__main__":
    import sys
    if (len(sys.argv) != 2):
        print( 'Must enter output directory' )
        exit(1)
    getWWAZips( sys.argv[1] )
