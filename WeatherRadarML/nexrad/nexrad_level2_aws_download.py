import logging
from logging.handlers import QueueHandler

import boto3
import boto3.session
import os, shutil, glob, time
from datetime import datetime, timedelta

from threading import Thread
from multiprocessing import Process, Event, Queue

from .utils.nexrad_level2_directory import nexrad_level2_directory

_dateFMT   = "%Y%m%d_%H%M%S";                                                   # Time format in NEXRAD files

###############################################################################
def mpLogHandler( queue ):
	while True:
		record = queue.get()
		if record is None:
			break

###############################################################################
class nexrad_aws_downloader( Process ):
    def __init__(self, resource, bucketName, fileQueue, logQueue, event, *args, **kwargs):
        """
        Inputs:
            resource   : The AWS resource to use for boto3 initialization
            bucketName : Name of the bucket to download data from
            fileQueue  : A multiprocess.Queue object for passing download
                            info into the process
            logQueue   : A multiprocess.Queue object for logging to
            event      : A multiprocess.Event object used to cleanly
                            end process
            All other arguments accepted by multiprocess.Process
        Keywords:
            attempts   : Maximum number of times to try to download
                            a file. Default is 3
            clobber    : Set to overwrite exisiting files.
                            Default is False
            All other keywords accepted by multiprocess.Process
        """
        attempts = kwargs.pop('attempts', 3)
        clobber  = kwargs.pop('clobber', False)

        super().__init__(*args, **kwargs);
        self._resource   = resource
        self._bucketName = bucketName
        self._queue      = fileQueue
        self._logQueue   = logQueue
        self._event      = event
        self._attempts   = attempts
        self._clobber    = clobber
    ###########################################################################
    def run(self):
        log     = logging.getLogger(__name__)
        log.addHandler( QueueHandler( self._logQueue ) );                       # Add Queue Handler to the log
        session = boto3.session.Session();                                      # Create own session as per https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
        s3conn  = boto3.resource(self._resource );                              # Start client to AWS s3
        bucket  = s3conn.Bucket( self._bucketName )                             # Connect to bucket

        nFail    = 0
        nSuccess = 0
        totSize  = 0

        statSize = 0
        dt       = 0.0;
        while (not self._event.is_set()) or (not self._queue.empty()):          # While the event is NOT set OR the queue is NOT empty
            try:
                station, key, size, localFile = self._queue.get(timeout = 0.5); # Try to get information from the queue, waiting half a second
            except:                                                             # If failed to get something from the queue
                continue;                                                       # Continue to beginning of while loop
            
            if (key is None):                                                   # If the key grabbed from the queue is None
                dlRate = (statSize / 1.0e6 / dt) if (dt > 0.0) else 0.0         # Compute the download rate for the station
                log.info(
                    '      {} sync complete at {}. Rate: {:5.1f} MB/s'.format(
                    station, datetime.now(), dlRate)
                );                                                              # Log time at which download for station finished
                statSize = 0;                                                   # Reset station download size
                dt       = 0.0;                                                 # Reset station download start time
                continue;                                                       # Continue to beginning of while loop

            if (len( glob.glob('{}*'.format(localFile)) ) > 0) and self._clobber is False:  # If the file has already been downloaded, or is being downloaded; aws puts .RANDOMHASH on file names while downloading
                log.debug(
                    '        File already downloaded : {}'.format(key))
                nSuccess += 1;                                                  # Increment number of successful downloads
                totSize  += size;                                               # Increment total size of all downloads
                statSize += size;
            else:                                                               # Else, we will try to download it
                statObj = bucket.Object( key );
                attempt = 0
                while (attempt < self._attempts):                               # While we have not reached maximum attempts
                    log.debug(
                        '        Download attempt {:2d} of {:2d} : {}'.format(
                                    attempt+1, self._attempts, key));           # Log some info
                    t0 = time.time()
                    try:
                        statObj.download_file(localFile);                       # Try to download the file
                        info = os.stat(localFile)                               # Get file info
                        if (info.st_size != size):                              # If file size is NOT correct OR clobber is set
                            raise Exception('File size mismatch!')
                    except:
                        attempt += 1;                                           # On exception, increment attempt counter
                    else:
                        dt      = dt + (time.time() - t0)
                        attempt = self._attempts + 1;                           # Else, file donwloaded so set attempt to 1 greater than maxAttempt
                
                if (attempt == self._attempts):                                 # If the download attempt matches maximum number of attempts,then all attempts failed
                    nFail += 1;                                                 # Number of failed donwloads for thread
                    log.error('        Failed to download : {}'.format(key) );  # Log error
                    try:
                        os.remove( localFile );                                 # Delete local file if it exists
                    except:
                        pass;
                else:
                    nSuccess += 1;                                              # Number of successful downloads for thread
                    totSize  += size;                                           # Size of downloads for thread
                    statSize += size
                statObj = None;                                                 # Set to None for garbage collection of object

        # Set all to None for garbage collection; may fix the SSLSocket error issue
        bucket  = None; 
        s3conn  = None;
        session = None;      

        self._queue.put( (nSuccess, nFail, totSize,) );                         # Return # success, # failed, and download size to the queue



###############################################################################
def nexrad_level2_aws_download( 
        date1       = datetime(2011, 2, 28),
        date2       = None,
        station     = 'KHGX',
        resource    = 's3',
        bucketName  = 'noaa-nexrad-level2',
        outroot     = '/data1/',
        no_MDM      = True,
        no_tar      = True,
        clobber     = False,
        maxAttempt  = 3,
        verbose     = False,
        concurrency = 4):
    """
    Name:
        nexrad_aws_level2
    Purpose:
        Function for downloading NEXRAD Level 2 data from AWS.
    Inputs:
        None.
    Keywords:
        date       : datetime object with starting date/time for
                        data to download.
                            DEFAULT: Current UTC day at 00Z
        station    : Scalar string or list of strings containing 
                        radar station IDs in the for KXXX
        resource   : AWS resource to download from. Default is s3
        bucketName : Name of the bucket to download data from.
                        DEFAULT: noaa-nexrad_level2
        outroot    : Top level output directory for downloaded files.
                        This function will create the following directory
                        structure in the directory:
                            <outroot>/YYYY/YYYYMM/YYYYMMDD/KXXX/
        no_MDM     : Set to True to exclude *_MDM files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        no_tar     : Set to True to exclude *tar files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        clobber    : Set to True to re download files that exist.
        maxAttempt : Maximum number of times to try to download
                        file. DEFAULT: 3
        concurrency: Number of concurrent downloads to allow
    Author and History:
        Kyle R. Wodzicki     Created 2019-07-06
    """
    log = logging.getLogger(__name__)
    t0  = time.time()

    if not isinstance( station, (list,tuple,) ): station = [station];            # If stations is not an iterable, assume is string and make iterable
    if (concurrency > 10): concurrency = 10

    stationdir, outdir, _ = nexrad_level2_directory(date1, station, root=outroot)

    log.info( 'NEXRAD_LEVEL2_AWS_DOWNLOAD - sync NEXRAD Level 2 data from AWS' )
    log.info( '   Sync start date  : {}'.format(date1))
    log.info( '   Sync end date    : {}'.format(date2))
    log.info( '   Output directory : {}'.format(outdir) )
 
    if os.path.isdir(outdir) and clobber:
        log.info( '   Deleting existing output directory and its contents' )
        shutil.rmtree( outdir )
    if not os.path.isdir( outdir ): os.makedirs( outdir )

    s3conn = boto3.resource(resource);                                              # Start client to AWS s3
    bucket = s3conn.Bucket(bucketName);                                             # Connect to bucket
    
    logQueue   = Queue();                                                           # multiprocessing.Queue for passing logs to main process
    logThread  = Thread(target=mpLogHandler, args=(logQueue,));                     # Initialize thread to consume log message from queue
    logThread.start();                                                              # Start the thread

    quitEvent  = Event();                                                           # multiprocessing.Event object for halting download processes
    tids       = [];                                                                # List to store download process objects
    fileQueues = [];                                                                # List to store fileQueues for the process objects
    for i in range( concurrency ):                                                  # Iterate over number of concurrency allowed
        fileQueues.append( Queue( maxsize = 500 ) );                                # Create queue with depth of 500; should be enough for one days worth of NEXRAD files
        tid = nexrad_aws_downloader(resource, bucketName, fileQueues[-1], logQueue, quitEvent, 
                    attempts = maxAttempt,
                    clobber  = clobber) ;                                           # Initialize a download process
        tid.start();                                                                # Start the process
        tids.append( tid );                                                         # Append process to the list of processes

    date  = datetime(date1.year, date1.month, date1.day, 0);                        # Create date for current date with hour at 0
    if (date2 is None):                                                             # If date2 is None
        date2 = date + timedelta(days=1);                                           # Set date2 to one day after date

    queueIndex = 0;                                                                 # Index for which queue to put files in
    while date2 > date:                                                             # While the end date is greater than date

        datePrefix = date.strftime('%Y/%m/%d/');                                    # Set date prefix for key filtering of bucket
        
        for i in range( len(station) ):                                             # Iterate over all stations in the station list
            if not os.path.isdir( stationdir[i] ): os.makedirs( stationdir[i] );    # If the output diretory does NOT exist, create it

            statPrefix = datePrefix + station[i];                                   # Create station prefix for bucket filter using datePrefix and the station ID
            statKeys   = bucket.objects.filter( Prefix = statPrefix );              # Apply filter to bucket objects
            for statKey in statKeys:                                                # Iterate over all the objects in the filter
                fBase = statKey.key.split('/')[-1];                                 # Get the base name of the file
                if (no_MDM and fBase.endswith('MDM')): continue;                    # If the no_MDM keyword is set and the file ends in MDM, then skip it
                if (no_tar and fBase.endswith('tar')): continue;                    # If the no_tar keyword is set and the file ends in tar, then skip it
                fDate = datetime.strptime(fBase[4:19], _dateFMT);                   # Create datetime object for file using information in file name
                if (fDate >= date1) and (fDate <= date2):                           # If the date/time of the file is within the date1 -- date2 range
                    localFile = os.path.join(stationdir[i], fBase);                 # Create local file path
                    
                    fileQueues[queueIndex].put( 
                        (station[i], statKey.key, statKey.size, localFile,)
                    );                                                              # Put information into queue for downloader process(s) because must be downloaded
            
            fileQueues[queueIndex].put( (station[i], None, None, None,) )           # Put some None objects into the queue to signal that downloads for given station and date are done
            queueIndex = (queueIndex+1) % concurrency;                              # Update the queueIndex for which queue to put the next station's info in

        date += timedelta(days = 1);                                                # Increment date by one (1) day

    quitEvent.set();                                                                # Set the event to kill the download processes once their queue empties
    nSuccess = 0;                                                                   # Initialize number of successful downloads to zero
    nFail    = 0;                                                                   # Initialize number of failed downloads to zero
    Size     = 0;                                                                   # Initiailze total size of downloads to zero
    for i in range( len(tids) ):                                                    # Iterate over the process objects
        tids[i].join();                                                             # Join process, blocks until finished
        nS, nF, s = fileQueues[i].get();                                            # Get statistics from the fileQueue; just before the download process finishes, it dumbs some information into the queue
        fileQueues[i].close();                                                      # Close the queue, this is good practice
        nSuccess += nS;                                                             # Increment number of successful downloads
        nFail    += nFail;                                                          # Increment number of failed downloads
        Size     += s;                                                              # Increment total size of downloads

    logQueue.put(None);                                                             # Put None in to the logQueue, this will cause the thread the stop
    logThread.join();                                                               # Join the thread to make sure it finishes 
    logQueue.close();                                                               # Close the log queue

    elapsed = time.time() - t0;                                                     # Compute elpased time
    log.info( 'NEXRAD_LEVEL2_AWS_DOWNLOAD - complete' )
    log.info( '   Downloaded       : {:10d} files'.format(nSuccess) )
    log.info( '   Failed           : {:10d} files'.format(nFail))
    log.info( '   Data transferred : {:10.1f} MB'.format(Size/1.0e6))
    log.info( '   Transfer Rate    : {:10.1f} MB/s'.format( Size / 1.0e6 / elapsed ) )
    log.info( '   Elapsed time     : {:10.1f} s'.format(elapsed))
    if (nFail == 0):
        log.info('No failed file syncs.');
    else:
        log.warning('Some files failed to sync!')


    filelist = []
    for root, dirs, files in os.walk(outdir):
        for file in files:
            path = os.path.join( root, file )
            if os.path.isfile(path) and (file[0] != '.'):
                filelist.append( path )
    
    return outdir, filelist, Size
