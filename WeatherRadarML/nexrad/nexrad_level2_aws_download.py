import logging
from logging.handlers import QueueHandler
import signal
import os, shutil, glob, time
from datetime import datetime, timedelta

import boto3
import boto3.session

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
    def __init__(self, resource, bucketName, fileQueue, logQueue, stopEvent, killEvent, *args, **kwargs):
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
        self._stopEvent  = stopEvent
        self._killEvent  = killEvent
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
        while (not self._stopEvent.is_set() or not self._queue.empty()) and not self._killEvent.is_set():  # While the event is NOT set OR the queue is NOT empty
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
                nSuccess += 1;                                                  # Increment number of successful downloads; size variables NOT incremented because didn't download anything
            else:                                                               # Else, we will try to download it
                statObj = bucket.Object( key );                                 # Get object from bucket so that we can download
                attempt = 0                                                     # Set attempt number to zero
                while (attempt < self._attempts):                               # While we have not reached maximum attempts
                    log.debug(
                        '        Download attempt {:2d} of {:2d} : {}'.format(
                                    attempt+1, self._attempts, key));           # Log some info
                    t0 = time.time()                                            # Start time of download
                    try:
                        statObj.download_file(localFile);                       # Try to download the file
                        info = os.stat(localFile)                               # Get file info
                        if (info.st_size != size):                              # If file size is NOT correct OR clobber is set
                            raise Exception('File size mismatch!')
                    except:
                        attempt += 1;                                           # On exception, increment attempt counter
                    else:
                        dt      = dt + (time.time() - t0)                       # Increment dt by the time it took to download current file
                        attempt = self._attempts + 1;                           # Else, file donwloaded so set attempt to 1 greater than maxAttempt
                
                if (attempt == self._attempts):                                 # If the download attempt matches maximum number of attempts,then all attempts failed
                    nFail += 1;                                                 # Number of failed donwloads for thread
                    log.error('        Failed to download : {}'.format(key) );  # Log error
                    try:
                        os.remove( localFile );                                 # Delete local file if it exists
                    except:
                        pass;
                else:
                    nSuccess += 1                                               # Number of successful downloads for thread
                    totSize  += size                                            # Size of downloads for thread
                    statSize += size                                            # Size of downloads for given station
                statObj = None;                                                 # Set to None for garbage collection of object

        # Set all to None for garbage collection; may fix the SSLSocket error issue
        bucket  = None; 
        s3conn  = None;
        session = None;      
        if self._killEvent.is_set():                                            # If killEvent set
            log.error('Received SIGINT; download cancelled.');                  # Log an errory
            while not self._queue.empty():                                      # While the queue is NOT empty
                station, key, size, localFile = self._queue.get();              # Dequeue items
                if (key is not None): nFail += 1;                               # Increment nFail if key is NOT None
        log.debug( '     AWS Download process finished' )
        self._queue.put( (nSuccess, nFail, totSize,) );                         # Return # success, # failed, and download size to the queue


###############################################################################
class nexrad_aws_scheduler(object):
    """
    This class was developed to cleanly close downloading processes when
    an interupt signal is received. On interupt, an event is set that
    tells the download processes to stop. While this will not happen
    instantly, trust that the processes are finishing what they are
    working on and closing.
    """
    def __init__(self):
        """
        Name:
            nexrad_aws_scheduler
        Purpose:
            Class that schedules downloads from AWS. Class used to enable
            SIGINT from user to cancel downloads as multiprocessing used
            and if not properly close processes, will get deadlock.
        Inputs:
            None.
        Keywords:
            None.
        """
        self.log = logging.getLogger(__name__);                                 # Initialize logger for the class

        self.s3conn      = None                                                 # Attribute for aws connection
        self.bucket      = None                                                 # Attribute for aws bucket
        self.outdir      = None                                                 # Attribute for output directory
        self.t0          = None                                                 # Attribute for start time of download 

        self.logQueue    = None                                                 # Attribute for queue for logging from processes
        self.logThread   = None                                                 # Atrribute for thread that dequeues logging records from logQueue
        self.tids        = None                                                 # Attribute for storing process objects
        self.fileQueues  = None                                                 # Attribute for storing queues for passing file information to download process

        self.stopEvent   = Event()                                              # Event for stopping download processes after all files downloaded
        self.killEvent   = Event()                                              # Event for killing download processes

        signal.signal( signal.SIGINT, lambda a, b: self.killEvent.set() );      # On SIGINT, set the killEvent


    ############################################################################
    def download(self,
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
            download
        Purpose:
            Method to download NEXRAD Level 2 data from AWS. Is a wrapper
            for the _enqueueFiles and _wait methods.
        Inputs:
            None.
        Keywords:
            date1      : datetime object with starting date/time for download.
                            DEFAULT: Current UTC day at 00Z
            date2      : datetime object with ending date/time for download.
                            DEFAULT: End of date1 day.
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
        Outputs:
            Returns output directory for data files, # successful downloads,
            # failed downloads, and total size of all downloaded files.
        """
        
        self._enqueueFiles( date1, date2, station, resource, bucketName, 
            outroot, no_MDM, no_tar, clobber, maxAttempt, verbose, concurrency)
        return self._wait() 
 
    ############################################################################
    def _initProcesses(self, resource, bucketName, clobber, maxAttempt, concurrency):
        """
        Name:
            _initProcesses
        Purpose:
            Private method to initialize downloader processes for concurrent
            downloading of data.
        Inputs:
            resource    : The AWS resource to use
            bucketName  : Name of the AWS bucket to use
            clobber     : Boolean that enables/disables file clobbering
            maxAttempt  : Integer maximum number of download retries
            concurrency : Integer number of concurrent downloads to allow
        Keywords:
            None.
        Outputs:
            None; updates class attributes
        """
        self.s3conn     = boto3.resource(resource);                             # Start client to AWS s3
        self.bucket     = self.s3conn.Bucket(bucketName);                       # Connect to bucket

        self.logQueue   = Queue();                                              # multiprocessing.Queue for passing logs to main process
        self.logThread  = Thread(target=mpLogHandler, args=(self.logQueue,));   # Initialize thread to consume log message from queue
        self.logThread.start();                                                 # Start the thread

        self.stopEvent.clear()                                                  # Event for signaling stop to process
        self.killEvent.clear()                                                  # Event for killing process
 
        self.tids       = [];                                                   # List to store download process objects
        self.fileQueues = [];                                                   # List to store fileQueues for the process objects

        for i in range( concurrency ):                                          # Iterate over number of concurrency allowed
            self.fileQueues.append( Queue( maxsize = 500 ) );                   # Create queue with depth of 500; should be enough for one days worth of NEXRAD files
            tid = nexrad_aws_downloader(
                    resource, bucketName, self.fileQueues[-1], 
                    self.logQueue, self.stopEvent,  self.killEvent, 
                    attempts = maxAttempt,
                    clobber  = clobber) ;                                       # Initialize a download process
            tid.start();                                                        # Start the process
            self.tids.append( tid );                                            # Append process to the list of processes
        

    ############################################################################
    def _enqueueFiles(self, date1, date2, station, resource, bucketName, 
			outroot, no_MDM, no_tar, clobber, maxAttempt, verbose, concurrency):

        """
        Name:
            _enqueueFiles
        Purpose:
            Private method to determine which files to download from AWS
            based on date1, date2, and station. Files to be downloaded
            are put in queues to processes that down the acutal downloading
        Inputs:
            None.
        Keywords:
            date1      : datetime object with starting date/time for
                            data to download.
                            DEFAULT: Current UTC day at 00Z
            date2      : datetime object with ending date/time for download.
                            DEFAULT: End of date1 day.
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
        Outputs:
            None.
        """                   
        if concurrency > 10: concurrency = 10
        
        self.t0  = time.time()
        
        if not isinstance( station, (list,tuple,) ): 
            self.station = [station];									            # If stations is not an iterable, assume is string and make iterable
        else:
            self.station = station
        stationdir, self.outdir, _ = nexrad_level2_directory(date1, self.station, root=outroot)
        
        self.log.info( 'NEXRAD_LEVEL2_AWS_DOWNLOAD - sync NEXRAD Level 2 data from AWS' )
        self.log.info( '   Sync date        : {}'.format(date1.strftime('%Y %m %d') ))
        self.log.info( '   Output directory : {}'.format(self.outdir) )
        
        self._initProcesses( resource, bucketName, clobber, maxAttempt, concurrency) 

        if os.path.isdir(self.outdir) and clobber:
            self.log.info( '   Deleting existing output directory and its contents' )
            shutil.rmtree( self.outdir )
        if not os.path.isdir( self.outdir ): os.makedirs( self.outdir )

        date  = datetime(date1.year, date1.month, date1.day, 0);                        # Create date for current date with hour at 0
        if (date2 is None):                                                             # If date2 is None
            date2 = date + timedelta(days=1);                                           # Set date2 to one day after date

        queueIndex = 0;                                                                 # Index for which queue to put files in
        while (date2 > date) and (not self.killEvent.is_set()):                         # While the end date is greater than date
            stationdir, self.outdir, _ = nexrad_level2_directory(date, self.station, root=outroot)

            datePrefix = date.strftime('%Y/%m/%d/');                                    # Set date prefix for key filtering of bucket
        
            for i in range( len(stationdir) ):                                          # Iterate over all stations in the station list
                if not os.path.isdir( stationdir[i] ): os.makedirs( stationdir[i] );    # If the output diretory does NOT exist, create it

                statPrefix = datePrefix + self.station[i];                              # Create station prefix for bucket filter using datePrefix and the station ID
                statKeys   = self.bucket.objects.filter( Prefix = statPrefix );         # Apply filter to bucket objects
                for statKey in statKeys:                                                # Iterate over all the objects in the filter
                    fBase = statKey.key.split('/')[-1];                                 # Get the base name of the file
                    if (no_MDM and fBase.endswith('MDM')): continue;                    # If the no_MDM keyword is set and the file ends in MDM, then skip it
                    if (no_tar and fBase.endswith('tar')): continue;                    # If the no_tar keyword is set and the file ends in tar, then skip it
                    fDate = datetime.strptime(fBase[4:19], _dateFMT);                   # Create datetime object for file using information in file name
                    if (fDate >= date1) and (fDate <= date2):                           # If the date/time of the file is within the date1 -- date2 range
                        localFile = os.path.join(stationdir[i], fBase);                 # Create local file path
                        while (not self.killEvent.is_set()):                            # While the killEvent is NOT set
                            try:                                                        # Try to put information into the queue with a timeout; this is done so that we don't wait forevery if trying to kill the code
                                self.fileQueues[queueIndex].put( 
                                    (station[i], statKey.key, statKey.size, localFile,),
                                    timeout = 1.0);                                     # Put information into queue for downloader process(s) because must be downloaded
                            except:                                                     # One exception
                                pass;                                                   # Quietly fail
                            else:                                                       # If successfully put data to queue
                                break;                                                  # Break while loop
                    if self.killEvent.is_set(): return;                                 # If the killEvent is set, then return from method; we don't want to put anything else into the queue
 
                self.fileQueues[queueIndex].put( (station[i], None, None, None,) )      # Put some None objects into the queue to signal that downloads for given station and date are done
                queueIndex = (queueIndex+1) % concurrency;                              # Update the queueIndex for which queue to put the next station's info in

            date += timedelta(days = 1);                                                # Increment date by one (1) day

    ############################################################################
    def _wait(self):
        """
        Name:
            _wait
        Purpose:
            Private method to wait for all download processes to finish
        Inputs:
            None.
        Keywords:
            None.
        Outputs:
            Returns output directory for data files, # successful downloads,
            # failed downloads, and total size of all downloaded files.
        """

        self.stopEvent.set();                                                           # Set the event to kill the download processes once their queue empties
        nSuccess = 0
        nFail    = 0
        Size     = 0
        for i in range( len(self.tids) ):                                               # Iterate over the process objects
            self.tids[i].join();                                                        # Join process, blocks until finished
            nS, nF, s = self.fileQueues[i].get();                                       # Get statistics from the fileQueue; just before the download process finishes, it dumbs some information into the queue
            self.fileQueues[i].close();                                                 # Close the queue, this is good practice
            nSuccess += nS;                                                             # Increment number of successful downloads
            nFail    += nF;                                                             # Increment number of failed downloads
            Size     += s;                                                              # Increment total size of downloads

        self.logQueue.put(None);                                                        # Put None in to the logQueue, this will cause the thread the stop
        self.logThread.join();                                                          # Join the thread to make sure it finishes 
        self.logQueue.close();                                                          # Close the log queue

        elapsed = time.time() - self.t0;                                                # Compute elpased time
        self.log.info( 'NEXRAD_LEVEL2_AWS_DOWNLOAD - complete' )
        self.log.info( '   Downloaded       : {:10d} files'.format(  nSuccess) )
        self.log.info( '   Failed           : {:10d} files'.format(  nFail))
        self.log.info( '   Data transferred : {:10.1f} MB'.format(   Size / 1.0e6))
        self.log.info( '   Transfer Rate    : {:10.1f} MB/s'.format( Size / 1.0e6 / elapsed ) )
        self.log.info( '   Elapsed time     : {:10.1f} s'.format(elapsed))

        if (nFail == 0):
            self.log.info('No failed file syncs.');
        else:
            self.log.warning('Some files failed to sync!')

        return self.outdir, nSuccess, nFail, Size

 
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
        nexrad_aws_level2_download
    Purpose:
        Function for downloading NEXRAD Level 2 data from AWS.
    Inputs:
        None.
    Keywords:
        date1      : datetime object with starting date/time for
                        data to download.
                            DEFAULT: Current UTC day at 00Z
        date2      : datetime object with ending date/time for download.
                            DEFAULT: End of date1 day.
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
    log = logging.getLogger( __name__ )

    scheduler = nexrad_aws_scheduler() 
  

    outdir, nSuccess, nFail, size = scheduler.download( 
        date1       = date1,
        date2       = date2,
        station     = station,
        resource    = resource,
        bucketName  = bucketName,
        outroot     = outroot,
        no_MDM      = no_MDM,
        no_tar      = no_tar,
        clobber     = clobber,
        maxAttempt  = maxAttempt,
        verbose     = verbose,
        concurrency = concurrency)
    
    filelist = glob.glob( os.path.join(outdir, '*') );                                  # Get list of all files that downloaded
    nfiles   = len(filelist)
    
    return outdir, filelist, size 
