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

LOG = logging.getLogger(__name__)


class Handler(FileSystemEventHandler):
   
    def __init__(self, extract_notification_log):
        self.extract_notification_log = extract_notification_log;
    
    def on_any_event(self,event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            LOG.debug("Received created event - %s." % event.src_path)
            # Take any action here when a file is first created.
            self.extract_notification_log.load_and_process_new_logs(event.src_path,'./')

        elif event.event_type == 'modified':
            pass
            # Taken any action here when a file is modified.
           # LOG.debug("Received modified event - %s." % event.src_path)



