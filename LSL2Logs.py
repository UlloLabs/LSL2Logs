
# Fetching data from LSL, writing to CSV

# Warning: if there are too many streams, with a throughtput too high, script might get overwhelmed and start to get late

import argparse, csv, time, os
from pylsl import ContinuousResolver, StreamInlet, local_clock, LostError
from datetime import datetime

class LSL2Logs:
    """
    Recording LSL streams data to CSV files. Each filename will be timestamped.

    Recording could other occur at launch, until stopRecording() is called from an external thread. Or it could be handled manually with startRecording() / loop() / stopRecording(). Each new recording session will create a new output file. stopRecording() should be called in "manual" mode to make sure that the output file is closed correctly.
    
    For manual recording, it is advised to catch KeyboardInterrupt to properly call stopRecording() upon termination, using "signal" to also catch SIGINT and SIGTERM signals. See example in comment at the end of the file.
    """
    def __init__(self, pred = "", record_on_start = True, verbose=False, inlet_buflen=10):
        """
        pred: A predicate to use to filter streams. E.g. "type='EEG'", "type='EEG' and name='BioSemi'", "(type='EEG' and name='BioSemi') or type='HR'". Note that that predicat is case-sensitive. Default: empty, record all streams
        verbose: if True, will print on stdout debug info (e.g. echoes everything which is written, can be a lot)
        record_on_start: start to record data upon init, a blocking call until stopRecording() is called
        inlet_buflen: how many data should be buffered in the background for each LSL inlet until samples are pulled. In seconds (or x100 sample if not sampling rate set, see LSL doc). Each new recording session will first fetch data from buffer.
        """
        # we will consider that an empty pred is meant to record everything
        if pred == "":
            print("Feching all streams")
            self._cr = ContinuousResolver(forget_after = 5)
        else:
            print("Using predicate: ", pred)
            self._cr = ContinuousResolver(pred=pred, forget_after = 5)
        # flag to determine if we are already recording or not
        self._recording = False
        self._inlet_buflen = inlet_buflen
        self.verbose = verbose

        # information that will be written to file
        self._fieldnames_csv =  ['date_local', 'timestamp_local', 'timestamp_sample', 'type', 'name', 'hostname', 'source_id', 'nominal_srate', 'data']    
        # CSV file where we will write info, initialized and used only in "manual" mode
        self._csvfile = None
        # CSV writer, be initialized later on, used to factorize code between bloking and non-blocking calls
        self._writer = None

        # will hold info about known streams, because it is resource consuming to create inlets
        self._streams = {}

        if record_on_start:
            self.record()

    def _initFile(self):
        """
        return filename for a new recording, create new output file if necessary. If already recording, returns an empty string
        TODO: should raise something if could not init file?
        """
        if self._recording:
            return ""
        timestamp_start = datetime.now().isoformat()
        filename_csv = './logs/data_' + timestamp_start + '.csv'
        print("Writing data to:" + filename_csv)
        # create file if necessary  
        if not os.path.exists(filename_csv) :
            with open(filename_csv, 'w') as csvfile :
                writer = csv.DictWriter(csvfile, fieldnames=self._fieldnames_csv)
                writer.writeheader()
        return filename_csv

    def _updateStreams(self):
        """
        update internal stream list
        FIXME: takes time, especially when there is a new inlet to create, should be ran in background
        """
        # fetch current streams
        current_streams = {}
        for i in self._cr.results():
            current_streams[i.uid()] = i
       
        # prune streams that do not exist anymore
        streams_outdated = set(self._streams) - set(current_streams)
        for o in streams_outdated:
            print("Lost stream:", self._streams[o]['info'].name(), self._streams[o]['info'].type(), self._streams[o]['info'].hostname())
            # remove item, explicitely delete corresponding inlet
            s = self._streams.pop(o)
            del(s['inlet'])
   
        # add new streams
        streams_new = set(current_streams) - set(self._streams)
        for n in streams_new:
            print("Got new stream:", current_streams[n].name(), current_streams[n].type(), current_streams[n].hostname())
            # add stream to list, creating inlet
            self._streams[current_streams[n].uid()] = {"info": current_streams[n], "inlet": StreamInlet(current_streams[n], max_buflen=self._inlet_buflen)}

    def _writeCSV(self):
        """
        Fetch data from registered stream and write to file
        Warning: handling of writing error / exception should be made by caller
        """
        if self._recording and self._writer is not None:
            # loop all current streams
            for s in self._streams.values():
                inlet = s['inlet']
                try:
                    sample, timestamp = inlet.pull_sample(timeout=0)
                except LostError:
                    # stream broke, but wait for resolver to remove it from list
                    print("stream broke")
                    sample = None
                
                # fetch all samples since last visit
                while sample is not None:
                    data = {
                        'date_local': datetime.now().isoformat(),
                        'timestamp_local': local_clock(),
                        'timestamp_sample': timestamp,
                        'type': s['info'].type(),
                        'name': s['info'].name(),
                        'hostname': s['info'].hostname(),
                        'source_id': s['info'].source_id(),
                        'nominal_srate': s['info'].nominal_srate(),
                        'data': sample
                    }
                    if self.verbose:
                        print(data)
                    self._writer.writerow(data)
                    try:
                        sample, timestamp = inlet.pull_sample(timeout=0)
                    except LostError:
                        sample = None

    def record(self):
        """
        blocking call, create new file and start to record data, looping at about 100hz
        """
        if self._recording:
            print("Error: already recording.")
            return
       
        with open(self._initFile(), 'a') as csvfile:
            self._writer = csv.DictWriter(csvfile, fieldnames=self._fieldnames_csv)
            self._recording = True
            while self._recording:
                self._updateStreams()
                self._writeCSV()
                # might want to tune value depending on the tradeoff resources consumes / resolution of local timestamp
                time.sleep(0.01)
        self.stopRecording()

    def startRecording(self):
        """
        manually start the recording session
        WARNING: If a previous recording sessions occurred, some inlets were likely to be created, and at the beginning of subsequent recording sessions all values in the buffer will be fetched first.
        TODO: new method to clean buffer?
        """
        if self._recording:
            print("Error: already recording.")
            return
        # attempts to open file
        try:
            self._csvfile = open(self._initFile(), 'a')
            self._writer = csv.DictWriter(self._csvfile, fieldnames=self._fieldnames_csv)
            self._recording = True
        except OSError:
            print("Error: cannot open " + args.output_csv)
   
    def loop(self):
        """
        update stream list. If currently recording will also fetch last values from LSL, write to file. To be called periodically.
        """
        self._updateStreams()
        if self._recording:
            self._writeCSV()

    def stopRecording(self):
        """
        Should be called when logger is used manually with startRecording() / loop() to properly close the file
        """
        self._recording = False
        if self._csvfile is not None:
            self._csvfile.close()
            # reset internal states
            self._csvfile = None
            self._writer = None
        print("Recording stopped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Record data sent in LSL to CSV format.")
    parser.add_argument("--pred", type = str, default = "", help = """A predicate to use to filter streams. E.g. "type='EEG'", "type='EEG' and name='BioSemi'", "(type='EEG' and name='BioSemi') or type='HR'". Note that that predicat is case-sensitive. Default: empty, record all streams.""")
    parser.add_argument("-v", "--verbose", action='store_true', help="Print more verbose information.")
    args = parser.parse_args()

    logger = LSL2Logs(args.pred, record_on_start=True, verbose=args.verbose)

# Below, an example of how to properly use the class for manual recording.
#
#import signal
#signal.signal(signal.SIGINT, signal.default_int_handler)
#signal.signal(signal.SIGTERM, signal.default_int_handler)
#logger = LSL2Logs()
#try:
#    logger.startReording()
#    while True:
#        logger.loop()
#        time.sleep(0.01)
#except KeyboardInterrupt:
#    print("Catching Ctrl-C or SIGTERM, bye!")
#finally:
#    logger.stopRecording()
