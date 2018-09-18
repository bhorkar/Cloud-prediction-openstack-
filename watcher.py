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
import multiprocessing as mp
from pprint import pprint
from itertools import islice
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from extractNotificationLog import ExtractNotificationLog
from filehandler import Handler 



LOG = logging.getLogger(__name__);

class Watcher(ExtractNotificationLog):
    def __init__(self, zone, config):
            self.observer                 = Observer()
            self.DIRECTORY_TO_WATCH       =  config['rabbitmq_input']['dir'] + '/' + zone;
            print (config.keys())
            self.watchtimer               = int(float(config['watchdog_parameter']['retry_new_notificationfile_interval_minutes'])*1.0*60);
            self.extract_notification_log = ExtractNotificationLog(zone, config);
            LOG.debug("watcher started for zone {}".format(zone))

    def run(self):
        event_handler = Handler(self.extract_notification_log)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(self.watchtimer)
        except:
            self.observer.stop()
            LOG.error("Error in watch")

        self.observer.join()

        

