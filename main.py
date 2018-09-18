#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import os
import sys
import time
import logging 
import logging.config
import datetime
import time
import multiprocessing as mp
from watcher import Watcher 
import yaml
import argparse
import multiprocessing
from extractNotificationLog import ExtractNotificationLog
from Utils import verifyconfig

def zone_worker(zone, config):
    p = multiprocessing.current_process()
    LOG.debug( 'Starting zone:' +  p.name + str( p.pid))
    sys.stdout.flush()
  
    if config['debug']['debug_file_flag'] != 1:

    	if config['rabbitmq_input']['type']  == 'file':
        	w = Watcher(zone, config);
        	w.run()
  	        LOG.debug("watcher started for zone {}".format(zone))

    	else:
        	extract_notification_log = ExtractNotificationLog(zone, config);

    else: 
    	extract_notification_log = ExtractNotificationLog(zone, config);
    	extract_notification_log.load_and_process_new_logs('test/notification.zrdm3mosc03.node1.2017-04-14_09')
    	extract_notification_log.load_and_process_new_logs('test/notification.zrdm3mosc03.node1.2017-04-14_10')
    	extract_notification_log.load_and_process_new_logs('test/notification.zrdm3mosc03.node1.2017-04-14_11')
    	extract_notification_log.load_and_process_new_logs('test/notification.zrdm3mosc03.node1.2017-04-14_12')



def service():
    #daemon service wakes up to check any changes in config file every hour
    ##TODO 
    p = multiprocessing.current_process()
    print('Starting Encaptulate Daemon:', p.name, p.pid)
    sys.stdout.flush()
    time.sleep(3600)

if __name__ == "__main__": 

    parser = argparse.ArgumentParser(description='Process the configueration file')
    parser.add_argument('--config', '-c',dest='config_file', metavar='c',default='config.yml'
            ,help='config file in yaml format')
    args = parser.parse_args()
    config_file = args.config_file;
    config = yaml.safe_load(open(config_file))
    verifyconfig(config)
    if config['log']['enable_file_output'] ==  True:
        logging.basicConfig(filename=config['log']['log_file'], level=config['log']['log_level'])
    else:
        logging.basicConfig(stream=sys.stdout, level=config['log']['log_level'])

    LOG = logging.getLogger(__name__)
    daemon = multiprocessing.Process(name='daemon', target=service)
    daemon.daemon = True
    daemon.start();
   #starting one process for each zone  
    for zone in  config['aic_zone_info']['zone_names']:
        if config['rabbitmq_input']['type']  == 'file':
            zone_dir = os.path.join(config['rabbitmq_input']['file_parameters']['dir'], zone) 
            if os.path.exists(zone_dir): 

                p = multiprocessing.Process(target= zone_worker, args = (zone, config, ))
                p.daemon = False
                p.start()
            else:
                LOG.error('zone directory {}  does not exists'.format(zone_dir))
        else:
                p = multiprocessing.Process(target= zone_worker, args = (zone, config, ))
                p.daemon = False
                p.start()




