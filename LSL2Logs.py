
# Fetching data from LSL, writing to CSV

# Warning: if there are too many streams, with a throughtput too high, script might get overwhelmed and start to get late

import argparse, csv, time, os
from pylsl import ContinuousResolver, StreamInlet, local_clock, LostError
from datetime import datetime

class LSL2Logs:
    """
    Recording LSL streams data to CSV files. Each filename will be timestamped.
    """
    def __init__(self, pred = "", record_on_start = True):
        """
        pred: A predicate to use to filter streams. E.g. "type='EEG'", "type='EEG' and name='BioSemi'", "(type='EEG' and name='BioSemi') or type='HR'". Note that that predicat is case-sensitive. Default: empty, record all streams
        record_on_start: start to record data upon init
        """
        # we will consider that an empty pred is meant to record everything
        if pred == "":
            print("Feching all streams")
            self.cr = ContinuousResolver(forget_after = 5)
        else:
            print("Using predicate: ", pred)
            self.cr = ContinuousResolver(pred=pred, forget_after = 5)
        # flag to determine if we are already recording or not
        self.recording = False
        if record_on_start:
            self.record()

    def record(self):
        """
        blocking call, create new file and start to record data
        """
        if self.recording:
            return
 
        self.recording = True
        timestamp_start = datetime.now().isoformat()
        filename_csv = './logs/data_' + timestamp_start + '.csv'
        fieldnames_csv =  ['date_local', 'timestamp_local', 'timestamp_sample', 'type', 'name', 'hostname', 'source_id', 'nominal_srate', 'data']    
        print("Writing data to:" + filename_csv)
       
        # create file if necessary  
        if not os.path.exists(filename_csv) :
            with open(filename_csv, 'w', newline='') as csvfile :
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames_csv)
                writer.writeheader()
               
        # will hold info about known streams, because it is resource consuming to create inlets
        streams = {}
    
        with open(filename_csv, 'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_csv)
            while self.recording:
            
                # update streams
                # FIXME: takes time, especially when there is a new inlet to create, should be ran in background
                current_streams = {}
                for i in self.cr.results():
                    current_streams[i.uid()] = i
               
                # prune streams that do not exist anymore
                streams_outdated = set(streams) - set(current_streams)
                for o in streams_outdated:
                    print("Lost stream:", streams[o]['info'].name(), streams[o]['info'].type(), streams[o]['info'].hostname())
                    # remove item, explicitely delete corresponding inlet
                    s = streams.pop(o)
                    del(s['inlet'])
           
                # add new streams
                streams_new = set(current_streams) - set(streams)
                for n in streams_new:
                    print("Got new stream:", current_streams[n].name(), current_streams[n].type(), current_streams[n].hostname())
                    # add stream to list, creating inlet
                    streams[current_streams[n].uid()] = {"info": current_streams[n], "inlet": StreamInlet(current_streams[n])}
           
                # loop all current streams
                for s in streams.values():
                    inlet = s['inlet']
                    try:
                        sample, timestamp = inlet.pull_sample(timeout=0)
                    except LostError:
                        # stream broke, but wait for resolver to remove it from list
                        print("stream broke")
                        sample = None
                        pass
                    
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
                        print(data)
                        writer.writerow(data)
                        try:
                            sample, timestamp = inlet.pull_sample(timeout=0)
                        except LostError:
                            sample = None
            
                # might want to tune value depending on the tradeoff resources consumes / resolution of local timestamp
                time.sleep(0.01)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Record data sent in LSL to CSV format.")
    parser.add_argument("--pred", type = str, default = "", help = """A predicate to use to filter streams. E.g. "type='EEG'", "type='EEG' and name='BioSemi'", "(type='EEG' and name='BioSemi') or type='HR'". Note that that predicat is case-sensitive. Default: empty, record all streams.""")
    args = parser.parse_args()

    logger = LSL2Logs(args.pred, record_on_start=True)
