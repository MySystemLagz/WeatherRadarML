import logging
import os, time

_colFMT = '{:>9}{:18.2f}{:12d}{:18.2f}'

###############################################################################
def noaa_wct_monitor( cache_dir, statInfo, event, log_unit_num = None ):
    """
    Name:
        noaa_wct_monitor
    Purpose:
        A function to monitor the progress of the NOAA WCT conversion
    Inputs:
        cache_dir  : Path to cache directory
        statInfo   : Dictionary containing information about stations
        event      : A threading.Event object used to make sure thread stops
    Keywords:
        log_unit_num : Log file to write information to
    Outputs:
        None.
    """
    statDone  = {};                                                              # Initialize statDone dictionary; data from statInfo will be moved to this dictionary when all files for station done
    toDelete  = []

    while (len(statInfo) > 1) and (not event.is_set()):                         # Iterate while there are stations left to process AND the event is NOT set
        cache_files = []
        for root, dirs, files in os.walk(cache_dir):
            for file in files:
                path = os.path.join(root, file)
                if os.path.isfile(path) and (file[0] != '.'):
                    cache_files.append( path )
        files  = cache_files
        nfiles = len(files)
        
        if (nfiles == 0):                                                       # If no file found, then sleep
            time.sleep(0.1);                                                    # Sleep 100 ms
        else:                                                                   # Else
            for file in files:                                                  # Iterate over all the files
                if (file in toDelete): continue;                                # Skip files that we need to delete
                station = file.split('%2F')[-2];                                # Get station name from cached file; second to last element when split on %
                if (station in statInfo):                                       # If the station is in the statInfo dictionary
                    statInfo[station]['numConverted'] += 1;                     # Increment the number of files convertd for the station
                    fsize0 = os.stat( file ).st_size;                           # Get size of the file
                    while True:                                                 # Iterate forever
                        time.sleep(0.1);                                        # Sleep 100 ms
                        fsize1 = os.stat(file).st_size;                         # Get size of cache file again
                        if (fsize1 > 0):
                            if (fsize1 == fsize0):                              # If sizes are the same
                                break;                                          # Break the infinite loop
                            else:                                               # Else
                                fsize0 = fsize1;                                # Set fsize0 to fsize1
                    toDelete.append( file );                                    # Append file to list of files we must delete
                    statInfo[station]['sizeConvert'] += fsize1;                 # Increment the total size of all files for station by fsize1
                    if statInfo[station]['numToConvert'] == statInfo[station]['numConverted']: # If the number of stations to convert matches the number of stations converted
                        info                     = statInfo.pop(station);       # Pop off subdictionary at station key
                        statInfo['sizeConvert'] += info['sizeConvert'];         # Increment the global cache usage variable in statDone by the size of the station
                        statDone[station]        = info;                        # Put the station dictionary into the statDone dictionary
                        msg = _colFMT.format(station, info['compressedSize'] / 1.0e3,
                                info['numConverted'], info['sizeConvert']    / 1.0e6 ); # Log some output
                        if (log_unit_num is not None):
                            log_unit_num.write( msg + '\n' );                   # Log some output

            time.sleep(1.0);                                                    # Sleep for one second
            while (len(toDelete) > 0): os.remove( toDelete.pop() );             # While files to delete, pop file off the list and delete it

    statInfo.update( statDone );                                                # Put information back into statInfo; I think this will be avaiable in main thread?

    return;                                                                     # Return from function

###############################################################################
def pyart_monitor( queue, statInfo, log_unit_num = None ):
    """
    Name:
        pyart_monitor
    Purpose:
        A function to monitor the progress of pyart nexrad conversions
    Inputs:
        queue    :  A multiprocessing queue, must be same one passed to
                        the returnQueue of pyart_level2_to_nc
        statInfo :  Dictionary containing information about stations
    Keywords:
        log_unit_num : Log file to write information to
    Ouputs:
        None.
    """
    statDone = {};                                                              # Dictionary to store  
    while len(statInfo) > 1:                                                    # While stations are processing
        result  = queue.get();                                                   # Get outFile path from the queue; this the result of the pyart_level2_to_nc function
        if (result is None): break;                                             # If result is None, then break while loop; force kill
        status  = result[0]
        outFile = result[1]
        station = os.path.basename(outFile)[:4];                                # Get station ID from file base name

        if (station in statInfo):                                               # If the station is in statInfo
            statInfo[station]['numConverted'] += 1;                             # Increment number of converted files
            if status:                                                          # If conversion success
                fsize = os.stat( outFile ).st_size;                             # Get file size
            else:
                fsize = 0
            statInfo[station]['sizeConvert'] += fsize;                          # Increment total size of all stations
            if statInfo[station]['numToConvert'] == statInfo[station]['numConverted']:  # If # files to convert matches # files converted
                info                     = statInfo.pop(station);               # Pop subdictionary off statInfo
                statInfo['sizeConvert'] += info['sizeConvert'];                 # Increment global size
                statDone[station]        = info;                                # Add station subdictionary to statDone
                msg = _colFMT.format(station, info['compressedSize'] / 1.0e3,
                        info['numConverted'], info['sizeConvert']    / 1.0e6 ); # Log some output
                if (log_unit_num is not None):
                    log_unit_num.write( msg + '\n' );                           # Log some output
    
    statInfo.update( statDone );                                                # This should make all data available at main thread
    return;                                                                     # Return
