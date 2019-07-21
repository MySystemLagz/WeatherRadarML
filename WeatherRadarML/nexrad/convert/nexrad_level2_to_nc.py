import logging
from logging.handlers import QueueHandler

import os, shutil, time, secrets, warnings

from datetime import datetime, timedelta
from subprocess import Popen, PIPE, STDOUT, DEVNULL
from multiprocessing import Process, Queue
from threading import Thread, Event

import DCOTSS_py
from DCOTSS_py.utils.handlers import mpLogHandler
from DCOTSS_py.utils.managers import processManager

from DCOTSS_py.nexrad.utils.nexrad_level2_directory import nexrad_level2_directory

from .convert_monitors import *
from .wct_export import wct_export

_dateFMT = '%Y%m%d_%H%M%S'

def nexrad_level2_to_nc(stations, date0, date1, 
        dynamics               = False,
        dealias                = False,
        concurrency            = 1,
        nexrad_level2_root     = None,
        nexrad_level2_tmp_root = None,
        noaa_wct_export        = '/data1/',
        noaa_wct_batch_config  = DCOTSS_py.NOAA_WCT_BATCH_CONFIG,
        log_unit_num           = None):
    
    log = logging.getLogger( __name__ )
    t0  = time.time()
    
    if not isinstance( stations, (list, tuple,) ):                                  # If stations is NOT a list or tuple (i.e., only single station), make it one
        stations = [stations]


    log.info( 'NEXRAD_LEVEL2_RADAR_TO_NC - convert NEXRAD Level 2 file to netCDF using wct' )
    log.info( '   Station ID                        : {}'.format( ','.join(stations) ) )
    log.info( '   Analysis window start time        : {}'.format(date0) )
    log.info( '   Analysis window end time          : {}'.format(date1)) )
    log.info( '   NEXRAD Level2 root directory      : {}'.format(nexrad_level2_root) )
    log.info( '   NEXRAD Level2 tmp root directory  : {}'.format(nexrad_level2_tmp_root) )
    
    statsInfo = {'sizeConvert' : 0};                                                # Iniialize statsInfo dictionary with sizeCovert tag, this is the total size of all converted files
    outdir    = [];                                                                 # List for output directories
    for i in range( len(stations) ):                                                # Iterate over all stations
        statsInfo[stations[i]] = {
            'compressedSize' : 0,
            'sizeConvert'    : 0,
            'numToConvert'   : 0,
            'numConverted'   : 0};                                                  # Dictionary with information about files for given station
        outdir.append( os.path.join( nexrad_level2_tmp_root, 'netcdf', stations[i] ) );     # Generate output directory and append to outdir list
        if not os.path.isdir( outdir[-1] ):                                         # If directory not exist
            os.makedirs( outdir[-1] );                                              # Make directory tree
        os.chmod( outdir[-1], 0o777 );                                              # Change directory permissions to rwx for ugo

    numConvert     = 0
    compressedSize = 0
    inFiles        = []
    outDirs        = []
    date           = datetime(date0.year, date0.month, date0.day, 0)
    
    while (date1 >= date):
        indir, root_dir_test, nexrad_level2_out_root = nexrad_level2_directory(
                    date, station = stations, root = nexrad_level2_root);       # Get input directory information
        
        for i in range( len(indir) ):                                           # Iterate over all input directories
            filelist = []
            for root, dirs, files in os.walk( indir[i] ):
                for file in files:
                    path = os.path.join(root, file)
                    if os.path.isfile(file) and (file[0] != '.'):
                        filelist.append( path )
            files  = filelist
            nfiles  = len(files)
            for f in files:                                                     # Iterate over all files in the directory
                filebase = os.path.basename(f)                                  # Get base name of file
                fdate    = datetime.strptime(filebase[4:17], '%Y%m%d_%H%M')     # Get datetime from file name; IDL version only checks to minutes; do not know why
                if (fdate >= date0) and (fdate <= date1):                       # If the file date is between date1 and date2
                    inFiles.append(  f );                                       # Append path to input file to inFiles list
                    outDirs.append(  outdir[i] )
                    
                    finfo           = os.stat( inFiles[-1] );                   # Get information about the file
                    compressedSize += finfo.st_size;                            # Increment compressedsize var by size of file
                    numConvert     += 1;                                        # Increment number of files to convert by one (1)
                    statsInfo[stations[i]]['compressedSize'] += finfo.st_size
                    statsInfo[stations[i]]['numToConvert']   += 1
        date = date + timedelta( days = 1 );                                    # Increment time by one (1) day
    #return statsInfo

    log.debug(
        '   Found {:4d} NEXRAD Level 2 files to convert to netCDF'.format(len(inFiles))
    )
    if dynamics is False:
        status = noaa_wct_level2_to_nc(inFiles, outDirs, statsInfo, 
                    noaa_wct_export = noaa_wct_export,
                    noaa_wct_batch_config = noaa_wct_batch_config,
                    noaa_wct_cache_dir    = nexrad_level2_tmp_root,
                    log_unit_num          = log_unit_num,
                    concurrency           = concurrency) 
    else:    
        status = pyart_level2_to_nc(inFiles, outDirs, statsInfo,
                    dealias      = dealias, 
                    log_unit_num = log_unit_num,
                    concurrency  = concurrency)    

    compressedSize /= 1.0e6
    sizeConvert     = statsInfo['sizeConvert'] / 1.0e9

    msg = '{:10}{:15.2f} M{:12}{:16.2f} G'.format('Totals',compressedSize, numConvert, sizeConvert) 
    if (log_unit_num is not None):
        log_unit_num.write( msg + '\n' );
        log_unit_num.write( '{:=<79}\n'.format('') )

    log.info( '   Conversion completed in {:0.1f} s'.format(time.time()-t0) )

    return [ os.path.basename(f) for f in inFiles ];                                # Return base names of all input files

##############################################################################
def noaa_wct_level2_to_nc(inFiles, outDirs, statsInfo, 
        output_format         = 'rnc',
        noaa_wct_export       = DCOTSS_py.NOAA_WCT_EXPORT,
        noaa_wct_batch_config = DCOTSS_py.NOAA_WCT_BATCH_CONFIG,
        noaa_wct_cache_dir    = None,
        log_unit_num          = None,
        concurrency           = DCOTSS_py._nCPU):
    """
    Name:
        noaa_wct_level2_to_nc
    Purpose:
        Function to spawn NOAA Weather and Climate Tool kit instances
        using a batch file as input
    Input:
        inFiles  : List of input files to convert
        outFiles : List of output directories for converted files
    Outputs:
        Returns a Popen instance
    Keywords:
        output_format         : Output format for data
        noaa_wct_export       : Path to the wct-export CLI
        noaa_wct_batch_config : Path to configure file to use
        noaa_wct_cache_dir    : Path to directory to place cached files.
                                    Note that cache files are placed in the
                                    'wct-cache/data/' directory within this
                                    directory.
        log_unit_num          : File handle to write some logging informaiton 
                                    to; This is just passed to the montor thread
        concurrency           : Number of simultaneous wct-export instances
                                    to run.
    """
    log = logging.getLogger( __name__ );

    if not isinstance(inFiles, (list,tuple,)):                                  # If inFile is not an iterable
        inFile = [inFiles];                                                     # Convert to list
    if not isinstance(outDirs, (list,tuple,)):                                  # If outDir is not an iterable
        inDir  = [outDirs]                                                      # Convert to list

    if (noaa_wct_batch_config is None):                                          # If batch config is None
        noaa_wct_batch_config = os.path.join(
            os.path.dirname(noaa_wct_export),
            'wctBatchConfig.xml'
        );                                                                      # Use standard batch file in NOAA WCT distribution

    log.info( '   NOAA wct-export                   : {}'.format(noaa_wct_export) )
    log.info( '   NOAA wct-export batch config file : {}'.format(noaa_wct_batch_config))

    lines = [];                                                                 # Initialize empty list to hold lines for batch file(s)
    for i in range(len(inFiles)):                                               # Iterate over input files
        lines.append( '{},{},{}'.format(inFiles[i], outDirs[i], noaa_wct_batch_config) ); # Create line for batch file and append to lines list

    if (len(lines) == 0):                                                        # If there are elements in lines list, then found files to convert
        log.info('No files to convert');                                        # Log information
        return True;                                                            # Return True

    randomHex  = secrets.token_hex(6);                                          # Create random hex
    batch_file = 'nexrad_wct_script_{}'.format( randomHex );                    # Batch file base name for wct-export
    batch_file = os.path.join('/', 'tmp', batch_file);                          # Full path for batch file base 


    numconvert     = 0
    sizeconvert    = 0
    compressedsize = 0
    
    cache_list     = []

    cacheDir = 'wct-cache-{}'.format(randomHex);                                # Name of cache dir for current conversion
    my_env   = os.environ.copy();                                               # Copy the user's environment
    if (noaa_wct_cache_dir is not None):                                        # If the cache dir has been specified
        noaa_wct_cache_dir = os.path.join( noaa_wct_cache_dir, cacheDir )
    else:
        noaa_wct_cache_dir = os.path.join( '/tmp', cacheDir )
    my_env['_JAVA_OPTIONS'] = '-Djava.io.tmpdir={}'.format(noaa_wct_cache_dir); # Set java cache dir through environment variable

    # Set up/start thread for monitoring progress of conversion
    cacheDir = os.path.join(noaa_wct_cache_dir, 'wct-cache', 'data');            # Build full cache directory path
    event    = Event();                                                          # Create a threading event
    monitor  = Thread( target = noaa_wct_monitor, 
                       args   = (cacheDir, statsInfo, event,),
                       kwargs = {'log_unit_num' : log_unit_num});                # Initialize monitor thread
    monitor.start();                                                            # Start monitor thread

    batch_files = []
    procCodes   = []

    # Create batch files and start converting files
    procs  = [];                                                                 # Initialize list to hold Popen instances
    nLines = len(lines)
    nn     = nLines // concurrency;                                             # Compute number of files to convert in each process
    if (nn == 0): nn = 1                                                        # If nn is zero, then make 1
    nn     = [i * nn for i in range(concurrency+1)];                            # Generate indices for which lines to write to which batch files
    if (nn[-1] < nLines): nn[-1] = nLines
    for i in range( concurrency ):                                              # Iterate over the indices
        batch_files.append( '{}-{}'.format(batch_file,i) );                     # Build batch file name and append the batch_files list
        log.info( '   NOAA wct-export batch file        : {}'.format(batch_files[-1]) )
        with open(batch_files[i], 'w') as ounit:                               # Open the batch file for writing
            for j in range(nn[i], nn[i+1]):                                     # Iterate over line indices
                ounit.write( lines[j] + '\n' );                                 # Write line to the file
        proc = wct_export(batch_files[i], output_format, 
                    noaa_wct_export    = noaa_wct_export,
                    env                = my_env)
        procs.append( proc );                                                   # Open a subrpcess (piping output to /dev/null) and append Popen to procs list

    for proc in procs:                                                          # Iterate over all the processes
        procCodes.append( proc.wait()  == 0 )

    monitor.join(timeout = 60);                                                 # Give the thread 2 seconds to finish up
    event.set();                                                                # Set the event, this will force thread to exit while loop
    monitor.join();                                                             # Join, the thread will stop on this call

    for batch in batch_files:                                                   # Iterate over list of batch files
        if os.path.isfile( batch ):
            os.remove( batch );                                                 # Delete each batch file

    if os.path.isdir( noaa_wct_cache_dir ):
        try:
            shutil.rmtree( noaa_wct_cache_dir);                                     # Delete the cache directory
        except:
            log.warning('  Failed to remove wct-cache directory : {}'.format(noaa_wct_cache_dir) )

    if not all( procCodes ):        
        log.error('Some wct-export commands did not exit cleanly')
        return False
   
    return True

##############################################################################
def pyart_level2_to_nc(inFiles, outDirs, statsInfo, 
        dealias      = False, 
        log_unit_num = None,
        concurrency  = DCOTSS_py._nCPU):
    """
    Name:
        nexrad_level2_to_nc
    Purpose:
        A function convert NEXRAD native radar file(s) to native netCDF file(s)
        using the Python-ARM Radar Toolkit (Py-ART).
        During file conversion, radial velocity can be de-aliased.  
    Input:
        inFiles  : List of input files to convert
        outFiles : List of output directories for converted files
    Output:
        Converted native radar files to native netCDF files.
    Keywords:
        dealias      : If set, de-alias wind fields prior to writing using Py-ART tools.
        log_unit_num : File handle to write some logging informaiton to; This is
                        just passed to the monitor threads
        concurrency  : Number of concurrent conversion to run at once.
    Author and history:
        Cameron R. Homeyer  2016-03-08.
                            2016-04-13. Updated to make de-aliasing winds optional.
        Kyle R. Wodzicki    2019-06-27. Ported from IDL to python3
    """
    # Import pyart, suppressing FutureWaring. Reset warnings after import
    # It is bad practice to import inside function, but pyart based conversions
    # do not seem to be very frequent and pyart messes up the user's path
    # for some reason
    warnings.filterwarnings('ignore', category=FutureWarning)
    import pyart
    warnings.resetwarnings()

    log = logging.getLogger(__name__)
    if not isinstance(inFiles, (list,tuple,)):                                  # If inFile is not an iterable
        inFile = [inFiles];                                                     # Convert to list
    if not isinstance(outDirs, (list,tuple,)):                                  # If outDir is not an iterable
        inDir  = [outDirs]                                                      # Convert to list

    procs     = []
    logQueue  = Queue()
    retQueue  = Queue()
    monitor = Thread( target = pyart_monitor, 
                      args   = (retQueue, statsInfo,),
                      kwargs = {'log_unit_num' : log_unit_num});                # Initialize monitor thread
    logThread = Thread(target = mpLogHandler,  args = (logQueue, ))
    monitor.start()
    logThread.start()         

    for i in range( len(inFiles) ):                                             # Iterate over input files
        procs, _ = processManager( procs, concurrency = concurrency );          # Check that there aren't too many processes running; block until one finishes if too many
        proc     = Process(target = pyart_level2_to_nc_file, 
                        args   = (inFiles[i], outDirs[i],),
                        kwargs = {'dealias'     : dealias, 
                                  'returnQueue' : retQueue,
                                  'logQueue'    : logQueue});                   # Iniialize multiprocess process
        proc.start();                                                           # Start the process
        procs.append(proc);                                                     # Append process handle to procs list
    procs, _ = processManager( procs, waitall = True );                         # Wait for all remaining processes to finish
    logQueue.put(None);                                                         # Put None in the log queue, this will kill it
    retQueue.put(None);                                                         # Put None in the return queue, this will kill it
    logThread.join();                                                           # Join the logger thread; wait for it to fully finish
    monitor.join();                                                             # Join the monitor thread; wait for it to fully finish
    logQueue.close();                                                           # Close the log queue
    retQueue.close();                                                           # Close the return queue
    
    return True


##############################################################################
def pyart_level2_to_nc_file(inFile, outDir, 
        dealias     = False, 
        returnQueue = None, 
        logQueue    = None):
    """
    Name:
        nexrad_level2_to_nc_file
    Purpose:
        A function convert NEXRAD native radar file to native netCDF file
        using the Python-ARM Radar Toolkit (Py-ART).
        During file conversion, radial velocity can be de-aliased.  
    Input:
        inFile  : Path to level2 NEXRAD file
        outFile : Path to netCDF version of level2 NEXRAD file    
    Output:
        Converted native radar files to native netCDF files.
    Keywords:
        dealias     : If set, de-alias wind fields prior to writing using Py-ART tools.
        returnQueue : Set to a multiprocessing.Queue instance to pass return values to
                        main process when running as multiprocess.
        logQueue    : Set to a multiprocessing.Queue instance to pass logs to
                        main process when running as multiprocess.
    Author and history:
        Cameron R. Homeyer  2016-03-08.
                            2016-04-13. Updated to make de-aliasing winds optional.
        Kyle R. Wodzicki    2019-06-27. Ported from IDL to python3
    """
    log     = logging.getLogger(__name__);
    status  = False;
    outFile = None;
    if logQueue is not None:
        log.addHandler( QueueHandler( logQueue ) )

    if not os.path.isfile( inFile ):
        log.error('Input file does NOT exist: {}'.format(inFile));
    else:
        outFile = os.path.join(outDir, os.path.basename(inFile) + '.nc')
        if os.path.isfile( outFile ):
            log.warning(
                'Ouput file already exists, deleting it: {}'.format(outFile)
            )
            os.remove( outFile );

        try:
            radar = pyart.io.read( inFile )
        except:
            log.exception( 'Failed to open file: {}'.format(inFile) )
        else:
            if dealias:
                log.debug('Dealiasing data...')
                dealias_data = pyart.correct.dealias_region_based(radar)
                radar.add_field('corrected_velocity', dealias_data)

            try:
                pyart.io.cfradial.write_cfradial(outFile, radar, 
                    format             = 'NETCDF4',
                    arm_time_variables = True);
            except Exception as err:
                log.error('Failed to write netCDF file: {}'.format(outFile) )
                if os.path.isfile( outFile ):
                    os.remove( outFile );
            else:
                status  = True

    if (returnQueue is not None):
        returnQueue.put( (status, outFile) );
    return status, outFile

