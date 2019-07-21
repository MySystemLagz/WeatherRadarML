import logging
from threading import Thread
from subprocess import Popen, STDOUT, PIPE

###############################################################################
class wctLogHandler( Thread ):
    """
    Name:
        wctLogHandle
    Purpose:
        A subclass of threading.Thread that reads info from 
        a subprocess.PIPE object and sends it to a logger
        so that user can see what is happing in the wct-export
        process.
    """
    def __init__(self, pipe, logLevel, *args, **kwargs):
        """
        Inputs:
            pipe     : The pipe to read data from subprocess.
                        Should set stdout=PIPE, stderr=STDOUT on
                        subprocess.Popen call, then pass in
                        process.stdout to this argument.
            logLevel : The log level to use for information from
                        subprocess; i.e., logging.INFO
            All other arguements accepted by Thread, however
            shouldn't need
        Keywords:
            All keywords accecpted by Thread, but shouldn't need
        """
        super().__init__(*args, **kwargs);                                      # Initialize super class
        self._log      = logging.getLogger(__name__);                           # Get a logger
        self._pipe     = pipe;                                                  # Store the pipe
        self._logLevel = logLevel;                                              # Store the log level
    # Overload run method; this is code that runs when .start() called
    def run(self):
        fullMsg = [];                                                           # Array for full message; wct-export prints line for what file is being processed, then line for status. Since multiple instances could be running, dont want progresses to be mingled 
        line    = self._pipe.readline();                                        # Read line from the pipe
        while (line != ''):                                                     # While the line is NOT empty
            if ('PROCESSING' in line):                                          # If 'PROCESSING' is in the line, then it's beginning of information about file
                while ('100%' not in line):                                     # While 100% NOT in line; i.e., file is NOT done processing
                    fullMsg.append( line );                                     # Add the line to the fullMsg list
                    line = self._pipe.readline();                               # Read another line
                fullMsg.append( line );                                         # Append line to fullMsg; the line with 100% will cause while loop to break, but still want that line in the message
                self._log.log( self._logLevel, ''.join(fullMsg).rstrip() );     # Join lines using '', strip off the carriage return, then log the message
                fullMsg = [];                                                   # Reset fullMsg to empty list
            else:                                                               # Else, normal line
                self._log.log( self._logLevel, line.rstrip() );                 # Strip carriage return and log line
            line = self._pipe.readline();                                       # Read the next line

###############################################################################
class wct_export( object ):
    """
    Name:
        wct_export
    Purpose:
        Class for calling the NOAA Weather and Climate Toolkit
        wct-export command line utility.
        Main reasons for this is class is to have an object that handles
        calling the subprocess and piping the process' stdout and stderr
        to logging. Also, that the script seems to have a
        blank first line, which subprocess doesn't like. A method in
        the class handles that.
   """
    def __init__(self, batchFile, outputFormat,
            noaa_wct_export = '/usr/local/wct-4.3.1/wct-export', **kwargs):
        """
        Inputs:
            batchFile    : Batch file to use when running wct-export
            outputFormat : Ouput format of converted files.
        Keywords:
            noaa_wct_export : Path to the wct-export command to use
            All keywords accecpted by Popen
            Note that stdout, stderr, and unversal_newlines will have no
                effect as those kewyords are fixed values of PIPE, STDOUT,
                and True, respectively.
        """ 
        self.log                     = logging.getLogger(__name__)
        kwargs['stdout']             = PIPE;                                    # Override stdout with PIPE for logging
        kwargs['stderr']             = STDOUT;                                  # Override stderr with STDOUT for logging
        kwargs['universal_newlines'] = True;                                    # Override universal_newlines with True for logging
        
        cmdBase         = self._fixScript( noaa_wct_export );                   # 'Fix' the wct-export script by determining what shell to use
        cmd             = cmdBase + [batchFile, outputFormat];                  # Add the batchFile and outputFormat to the base command
        self.log.debug( '   NOAA wct-export command           : {}'.format( cmd ) )

        self.process    = Popen( cmd, **kwargs );                               # Run the command
        self.thread     = wctLogHandler( self.process.stdout, logging.INFO );   # Initialize logging thread
        self.thread.start();                                                    # Start thread

    ###########################################################################
    def wait(self):
        """
        A method to wait for the process to finish, then wait for the logging
        thread to finish, then return the subprocess' returncode
        """
        self.thread.join();                                                     # Wait for logger thread to finish
        self.process.communicate();                                             # Communicate with process; make sure everything closes
        return self.process.returncode;                                         # Return process retrun code

    ###########################################################################
    def _fixScript(self, noaa_wct_export):
        """
        Method for determining what shell to run the wct-export
        script in.
        """
        with open(noaa_wct_export, 'r') as fid:                                 # Open the exporter script for reading
            line = fid.readline();                                              # Read line from file
            while ('#!' not in line):                                           # While '#!' is NOT in the line
                line = fid.readline();                                          # Read another line

        shell = line.strip().replace('#!', '');                                 # Strip leading/trailing spaces from the line and replace the '#!' string with nothing; this is shell to use
        return [shell, noaa_wct_export]                                         # Create command to run, specifying shell to use, the wct-export path, and return it
