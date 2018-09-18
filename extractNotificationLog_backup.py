# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import csv
import os
import sys
import json
import collections
import re
import time
import re 
import logging 
import logging.handlers
import glob
import datetime
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import multiprocessing as mp
from pprint import pprint
from itertools import islice
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import traceback
from filehandler import Handler 
from Utils import mkdir_p
from multiprocessing import Pool

LOG = logging.getLogger(__name__);

class ExtractNotificationLog():
    """Extracts Notififcation logs.
    This class fetches  logs from hourly stored notifications files. The
    logs are passed through log cleaning functions, converetged to pandas dataframe 
    for further analysis. The connection function will be extended to directly 
    connect to rabbiMQ for real time processing. 
   
    """
    def __init__(self, zone, config):
        
        # Initialises the source/destination of the log messages
        self.data_source_path       = config['rabbitmq_input']['dir'];
        self.encapsulate_dest_path  = zone + config['encalupated_out']['dir'] + '/'
        self.data_source            = config['rabbitmq_input']['type']
        mkdir_p(self.data_source)
        self.n_mprocessing          = config['processing']['ncores_per_zone']
        
        self.part = 0; #count number of the file 
        
        rep_tokens      = {'\\': '',  '"{': '{',  '}"': '}', '\'{': '{',   '}\'': '}', '{}':'"NA"'}
        self.rep_tokens = dict((re.escape(k), v) for k, v in rep_tokens.iteritems())
        self.pattern_tokens = re.compile("|".join(self.rep_tokens.keys()))
        
        rep_for_exchange            = r'"oslo.message"'
        self.pattern_for_exchange   = re.compile(rep_for_exchange)
       

        if self.data_source != 'File':
            raise ValueError("Data source other than File is not implemented");
        if self.data_source == 'File':
            self.files_to_load = None; 
        # NOTE(ab981s) change number of processors to modify the processing speed 
        self.pool = Pool(self.n_mprocessing);
        
        LOG.debug("Completed initialization ")
    
    def load_and_process_new_logs(self,filename, path_name_out):
          #get the latest file in the path;
        #loop over all the files   
       
        funclist = []
       
        reader = open(filename,'r') 
        while True:
                nlines = [x.strip() for x in islice(reader, 10000)]
                if not nlines:
                    logging.debug("breaking " + filename + " date " )
                    break       
                f = self.pool.apply_async(self.process_frame,[nlines, self.part, self.encapsulate_dest_path, filename,])
                funclist.append(f)
               # self.process_frame(nlines, self.part, self.encapsulate_dest_path, filename)
                self.part = self.part + 1;
        print funclist
        self.pool.close()
        self.pool.join()
        result = 0
       # for f in funclist:
       #     result += f.get(timeout=100000) # timeout in 10000 seconds

        LOG.debug("There are {} rows of data in file {}".format(result, filename))
   
 
        
    def process_frame(self, logLines,fpart,storage_pathname='./',filename=None):

        LOG.debug('here')
        print "here"
        n_lines = 0;
        cErr = 0;
        dfdic  = pd.DataFrame(columns=('global_id', 'exchange', 'payload'))
        tempdf = pd.DataFrame(columns=('global_id', 'exchange', 'payload'))
        dfdic['payload']  = dfdic['payload'].astype(str);
        tempdf['payload'] = tempdf['payload'].astype(str);
        LOG.debug('Number of lines  ' + str(len(logLines)) + ' in df ' + str(fpart))
        for line in logLines:
            try :
                start0 = self.pattern_for_exchange.search(line).span()[0] - 2
                splitted = line[0:start0].split('\t')[1].split(':')
                topic = splitted[0]
                if "monitor" in topic:
                    # only consider notifications.info 
                    continue
                exchange = splitted[1]
                log = self.pattern_tokens.sub(lambda m: self.rep_tokens[re.escape(m.group(0))], line[start0:])
                tempdf.loc[0,'global_id'] = 0;
                tempdf.loc[0,'exchange'] = exchange;
                tempdf.loc[0,'payload'] = log;
                dfdic = pd.concat([dfdic, tempdf]);
            except Exception as e:
               cErr +=1
            n_lines = n_lines + 1;
        
        save_file_name =  (storage_pathname  + 'Notifdf_' + os.path.basename(filename) + '_' + 'Part_' + str(fpart)   + '_file.csv')
        dfdic.to_csv(save_file_name)
        LOG.debug('Completed lines ' + str(n_lines) + ' in df ' + str(fpart) + save_file_name)
        return len(dfdic)



