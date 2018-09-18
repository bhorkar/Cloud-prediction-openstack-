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
import traceback
from filehandler import Handler
from Utils import mkdir_p
from pathos.pools import _ProcessPool
import threading
from threading import Lock;
import datetime
from sherlocklistener import SherlockListener
LOG = logging.getLogger(__name__);
import traceback

class ExtractNotificationLog():
    """Extracts Notififcation logs.
    This class fetches  logs from hourly stored notifications files. The
    logs are passed through log cleaning functions, converetged to pandas dataframe
    for further analysis. The connection function will be extended to directly
    connect to rabbiMQ for real time processing.

    """
    def __init__(self, zone, config):

        # Initialises the source/destination of the log messages
        self.transaction            = config['analysis']['transaction']
        self.maxtransactionduration = config['analysis']['maxtransactionduration']
        self.data_source            = config['rabbitmq_input']['type']
	self.save_all_logs          = config['analysis']['save_all_logs']
	self.save_capsules          = config['analysis']['save_capsules']
        self.encapsulate_dest_path  =  os.path.join(zone, config['encalupated_out']['dir'])
        self.maxtransactionduration = config['analysis']['maxtransactionduration'] # if there is no end event after this period, the transaction is failed
        self.startevent 			= 'compute.instance.create.start'
        self.endevent 				= 'compute.instance.create.end'
        self.VMidList               = []
        self.ReqidList              = []
        self.TokenidList            = []
        self.GlobalidList           = []
        self.token2reqidDic = collections.defaultdict(dict)
        self.token2insidDic = collections.defaultdict(dict)
        self.token2starttimeDic = collections.defaultdict(dict)
        self.actiondfColList = ['transaction', 'token', 'request_id', 'instance_id', 'global_id', 'project_id', 'tenant_id',
                           'user_id', 'transaction_tstart', 'transaction_tend']
        self.actionstartdf = pd.DataFrame(columns=self.actiondfColList)
        self.cerror = -1 # counter for the number of exception errors happend (can be canceled out)
        self.rowcount = -1 # counter for tracking the number of records/logs/lines (can be canceled out)
        self.transaction_aggregated_df = pd.DataFrame() # dataframe for storing the required infromation of observed VMs that have been launched

        self.debug_file_flag = int(config['debug']['debug_file_flag'])
	self.debug_sherlock_flag = int(config['debug']['debug_sherlock_flag']);
 	if self.debug_sherlock_flag:
		self.debug_sherlock_file = config['debug']['debug_sherlock_file'];

        self.timeinterval = config['analysis']['cache_flush_interval']
        if  self.debug_file_flag == 1:
            self.current_interval_tick = np.datetime64(pd.to_datetime(str(config['debug']['startingdate'])  +'-' +str(config['debug']['startingtime'])))
        else:
            self.current_interval_tick = np.datetime64(pd.datetime.now())
        self.previous_interval_tick = self.current_interval_tick - np.timedelta64(self.timeinterval, 's')
        self.future_interval_tick = self.current_interval_tick + np.timedelta64(self.timeinterval, 's')
        self.interval_tickList = [self.previous_interval_tick, self.current_interval_tick, self.future_interval_tick]
        threading.Timer(1.0*int(self.timeinterval), self.update_current_tick_and_more).start()   # Timer that calls "update_current_tick_and_more" every "self.timeinterval" seconds

        mkdir_p(self.encapsulate_dest_path)
        self.n_mprocessing          = config['processing']['ncores_per_zone']
        self.part = 0; #count number of the file

        rep_tokens      = {'\\': '',  '"{': '{',  '}"': '}', '\'{': '{',   '}\'': '}', '{}':'"NA"'}
        self.rep_tokens = dict((re.escape(k), v) for k, v in rep_tokens.iteritems())
        self.pattern_tokens = re.compile("|".join(self.rep_tokens.keys()))

        rep_for_exchange            = r'"oslo.message"'
        self.pattern_for_exchange   = re.compile(rep_for_exchange)

        if self.data_source == 'file':
            self.files_to_load = None;
            self.data_source_path       = config['rabbitmq_input']['file_parameters']['dir'];
        elif self.data_source == 'stream':
	    self.flush_sherlock_msglist_interval = config['rabbitmq_input']['stream_parameters']['flush_sherlock_msglist_interval']
            LOG.debug("starting the timer " + str(self.flush_sherlock_msglist_interval))
            threading.Timer(self.flush_sherlock_msglist_interval, self.sherlock_retrive_df_process_frame).start();
            if self.debug_sherlock_flag == 0:
		self.sherlock_listener =  SherlockListener(config)
	    	self.sherlock_listener.start_listener()
            	#t1 = threading.Thread(target=self.sherlock_listener.start_listener)
    	    	#t1.start()
	    	#print "joining thread" 
           	# t1.join(0)
	    else:
		self.msg_list = [];
		self.load_and_process_new_logs(filename = './sherlock');
        else:
            raise ValueError("Data source other than File is not implemented");

        if config['debug']['debug_file_flag'] == 1:
            self.current_interval_tick = np.datetime64(pd.to_datetime(str(config['debug']['startingdate'])  +'-' +str(config['debug']['startingtime'])))
        else:
            self.current_interval_tick = np.datetime64(pd.datetime.now())

        LOG.debug("Completed initialization ")
    def sherlock_retrive_df_process_frame(self):
       self.msg_list = self.sherlock_listener.return_msg_list();
       self.load_and_process_new_logs(filename = './sherlock');
       self.sherlock_listener.flush_prevmsg();
       threading.Timer(self.sherlock_listener.flush_sherlock_msglist_interval, self.sherlock_retrive_df_process_frame).start();


    # ******************************************************************************************************************
    # Updates the current tick, removes the infromation of terminated transactions, and dumps datafrems of completed transactions
    def update_current_tick_and_more(self):
        if self.debug_file_flag == 1:
            self.current_interval_tick = self.current_interval_tick + np.timedelta64(self.timeinterval, 's');
        else:
            self.current_interval_tick =  np.datetime64(pd.datetime.now())
        self.previous_interval_tick = self.current_interval_tick - np.timedelta64(self.timeinterval, 's')
        self.future_interval_tick = self.current_interval_tick + np.timedelta64(self.timeinterval, 's')
        self.interval_tickList = [self.previous_interval_tick, self.current_interval_tick, self.future_interval_tick]

        self.keep_not_terminated_transactions()  # removes the infromation of terminated transactions
        self.dump_completed_transactions() # dumps datafrems of completed transactions

        threading.Timer(1.0 * int(self.timeinterval), self.update_current_tick_and_more).start()

    # ******************************************************************************************************************
    # Loads and processes new logs
    def load_and_process_new_logs(self,filename = None):
        if self.data_source == 'file':
	    if self.n_mprocessing > 1 :
		error ("Multiprocessing on files is not supposeted yet");
            	pool = _ProcessPool(self.n_mprocessing);
            funclist = []
            reader = open(filename,'r')
            self.part = 0
            while True:
                nlines = [x.strip() for x in islice(reader, 10000)]
                if not nlines:
                    logging.debug("breaking " + filename + " date " )
                    break
                self.part = self.part + 1;
                
              #multiprocessing not working
		if self.n_mprocessing == 1 :
	         	self.process_frame(nlines, self.part, self.encapsulate_dest_path,filename)  # self.interval_tickList
		else:
              		f = pool.apply_async(self.process_frame,[nlines, self.part, self.encapsulate_dest_path, filename,])   
   			funclist.append(f)

     	    if self.n_mprocessing > 1:
           	 result = 0
           	 for f in funclist:
            	     result += f.get(timeout=100000) # timeout in 100000 seconds

           	 LOG.debug("There are {} rows of data in file {}".format(result, filename))
        elif self.data_source == 'stream':
                self.part = self.part + 1;
                self.process_frame(self.msg_list,self.part, self.encapsulate_dest_path, filename)



    def process_frame(self, logLines,fpart,storage_pathname='./',filename='./temp'):
        previous_interval_tick = self.interval_tickList[0]
        current_interval_tick = self.interval_tickList[1]
        if self.debug_sherlock_flag:
		 reader = open(self.debug_sherlock_file,'r')
                 logLines = [x.strip() for x in islice(reader, 10)]
		 
		 
        
        n_lines = 0;
        cErr = 0;
        dfdic  = pd.DataFrame(columns=('global_id', 'exchange', 'payload'))
        tempdf = pd.DataFrame(columns=('global_id', 'exchange', 'payload'))
        dfdic['payload']  = dfdic['payload'].astype(str);
        tempdf['payload'] = tempdf['payload'].astype(str);
        LOG.debug('Number of lines  ' + str(len(logLines)) + ' in df ' + str(fpart))
        for line in logLines:
            if self.data_source == 'file':
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
                    tempdf.loc[0,'timestamp'] = 0
                    logPL = tempdf.loc[0,'payload']
                   
                    event_globalid_and_timestamp_List = self.perrow_transaction_globalid_assignment(exchange, logPL)
                    if event_globalid_and_timestamp_List is not None:
                        tempdf.loc[0, 'global_id'] = str(event_globalid_and_timestamp_List[0])
                        tempdf.loc[0, 'timestamp'] = str(event_globalid_and_timestamp_List[1])
                        self.transaction_aggregated_df = self.transaction_aggregated_df.append(tempdf.loc[0],ignore_index=True)

                    dfdic = pd.concat([dfdic, tempdf]);
                except Exception as e:
                    cErr +=1
            else:
               # try:
                    tempdf.loc[0,'global_id'] = 0;
                    tempdf.loc[0,'exchange']  = 'none';
                    tempdf.loc[0,'payload']   = line;
                    tempdf.loc[0,'timestamp'] = 0
		    if self.debug_sherlock_flag:
			row_jsonft = pd.read_json(line);
  		    else:	
                    	logPL = tempdf.loc[0,'payload'][0]
       		    event_globalid_and_timestamp_List = self.perrow_transaction_globalid_assignment('none', logPL)


                    if event_globalid_and_timestamp_List is not None:
                    	 tempdf.loc[0, 'global_id'] = str(event_globalid_and_timestamp_List[0])
                    	 tempdf.loc[0, 'timestamp'] = str(event_globalid_and_timestamp_List[1])
                         self.transaction_aggregated_df = self.transaction_aggregated_df.append(tempdf.loc[0],ignore_index=True)


                    dfdic = pd.concat([dfdic, tempdf]);
	        #except Exception as e:
                 #   cErr +=1
                    
            n_lines = n_lines + 1;
        if self.save_all_logs: 
        	save_file_name =  os.path.join(storage_pathname, 'Notifdf_' + os.path.basename(filename) + '_' + 'Part_' + str(fpart)   + '_file.csv')
        	dfdic.to_csv(save_file_name)
        	LOG.debug('Completed lines ' + str(n_lines) + ' in df ' + str(fpart) + save_file_name)
        return len(dfdic)

    # ******************************************************************************************************************
    # Dumps the capsules of transactions every "self.timeinterval" seconds
    def dump_completed_transactions(self):
	tmpdumdf = pd.DataFrame()
        dumpindexes = []
        indexset = set(range(self.transaction_aggregated_df.shape[0]))
        if self.transaction_aggregated_df.shape[0]>0:
            for row in list(set(self.transaction_aggregated_df['global_id'])):
                rowstartendflag = [0, 0]
                rowfailureflag = [0, 0]
                starttime = -1
                tmpdf = self.transaction_aggregated_df[self.transaction_aggregated_df['global_id'] == row]
                tmpindList = list(self.transaction_aggregated_df[self.transaction_aggregated_df['global_id'] == row].index.values)
                for tmprow in tmpindList: # range(tmpdf.shape[0])
		    cur_event_type = '';
		    if self.data_source == 'stream':
			 cur_event_type =  tmpdf['payload'].loc[tmprow][0]['event_type']
                    if (self.startevent in tmpdf['payload'].loc[tmprow]) or (self.startevent in cur_event_type):
                        rowstartendflag[0] = 1
                        rowfailureflag[0] = 1
                        starttime = np.datetime64(pd.to_datetime(tmpdf['timestamp'].loc[tmprow]))
                    elif (self.endevent in tmpdf['payload'].loc[tmprow]) or (self.endevent in cur_event_type) :
                        rowstartendflag[1] = 1
                    elif (starttime!=-1) and (np.datetime64(pd.to_datetime(tmpdf['timestamp'].loc[tmprow])) > (starttime+np.timedelta64(self.maxtransactionduration, 's')) ):
                        rowfailureflag[1] = 1

                if ( ((rowstartendflag[0] == 1) & (rowstartendflag[1] == 1)) | ((rowfailureflag[0] == 1) & (rowfailureflag[1] == 1)) ):
                    dumpindexes = dumpindexes + list(tmpindList)

            keepindexset = list(indexset - set(dumpindexes))
            if len(dumpindexes)>=1:
                save_dumpfile_name = os.path.join(self.encapsulate_dest_path,str(self.transaction) + '-' + str(self.current_interval_tick).replace(':','-') + '_df000.csv')
		LOG.debug("dumping capsule to file" + save_dumpfile_name);
		if self.save_capsules:
			self.transaction_aggregated_df.loc[dumpindexes].to_csv(save_dumpfile_name)
                tmpdf0 = self.transaction_aggregated_df.loc[keepindexset]
                del self.transaction_aggregated_df
                self.transaction_aggregated_df = tmpdf0
                del tmpdf0

    # ******************************************************************************************************************
    # Removes the infromation of terminated transactions and keeps only the infromation of not-terminated transactions
    def keep_not_terminated_transactions(self):
        previous_interval_tick = self.interval_tickList[0]
        current_interval_tick = self.interval_tickList[1]

        tmpdropList = []
        tmpdroptokenList = []
        tmpactionstartdf = pd.DataFrame(columns=self.actiondfColList)
        for droprow in range(self.actionstartdf.shape[0]):
            if (np.datetime64(self.actionstartdf['transaction_tstart'].iloc[droprow]) < previous_interval_tick) | \
                    (np.datetime64(self.actionstartdf['transaction_tend'].iloc[droprow]) < \
                             (np.datetime64(self.actionstartdf['transaction_tstart'].iloc[droprow]) + \
                                      np.timedelta64(self.maxtransactionduration, 's'))):
                tmpdropList = tmpdropList + [droprow]
                tmpdroptokenList = tmpdroptokenList + [self.actionstartdf['token'].iloc[droprow]]

        if len(tmpdropList) > 0:
            for droprow in range(self.actionstartdf.shape[0]):
                if droprow in tmpdropList:
                    droptoken = self.actionstartdf['token'].iloc[droprow]
                    del self.token2reqidDic[droptoken]
                    del self.token2insidDic[droptoken]
                    del self.token2starttimeDic[droptoken]
                else:
                    tmpactionstartdf = tmpactionstartdf.append(self.actionstartdf.loc[droprow], ignore_index=True)
            del self.actionstartdf
            self.actionstartdf = tmpactionstartdf
            del tmpactionstartdf

    # ******************************************************************************************************************
    # Assigns a global_id to each row per each transaction
    def  perrow_transaction_globalid_assignment(self, row_exchange_value, orgrowPL):
        self.rowcount += 1
        try:
	    if self.data_source == 'file':
		# The payload of the original dataframe in json format.
		#Streaming data is already preprocessed with josn
            	row_jsonft = pd.read_json(orgrowPL)['oslo.message'];
	    else:
		row_jsonft = orgrowPL;	
            # **********************************************************************************************************
            # Find the rows having the start event and build the required data structure
            if (('_context_auth_token' in list(row_jsonft.keys())) & (
                        '_context_request_id' in list(row_jsonft.keys())) & ('event_type' in list(row_jsonft.keys()))):
                if (self.transaction == 'vmcreation') & (
                            str(row_jsonft['event_type']) == self.startevent) & \
                        ((str(row_jsonft['_context_auth_token']) not in self.TokenidList) & \
                                 ((str(row_jsonft['_context_request_id']) not in self.ReqidList))):
                    curtoken = str(row_jsonft['_context_auth_token'])
                    curreqid = str(row_jsonft['_context_request_id'])
                    curinsid = str(
                        row_jsonft['payload']['instance_id'])  # Required for VM creation
                    if (len(self.token2reqidDic[curtoken]) < 1):
                        curtime = np.datetime64(pd.to_datetime(row_jsonft['timestamp']))
                        self.token2starttimeDic[curtoken] = list()
                        self.token2starttimeDic[curtoken].append(curtime)

                    self.ReqidList = list(set(list(self.ReqidList + [curreqid])))
                    self.VMidList = list(set(list(self.VMidList + [curinsid])))
                    self.TokenidList = list(set(list(self.TokenidList + [curtoken])))
                    globalid = self.transaction + '_' + curtoken + '_' + curinsid
                    self.GlobalidList = list(set(list(self.GlobalidList + [globalid])))
                    ctoken = self.actionstartdf.shape[0]
                    self.actionstartdf.set_value(ctoken, 'transaction', 'vmcreation')
                    self.actionstartdf.set_value(ctoken, 'token', curtoken)
                    self.actionstartdf.set_value(ctoken, 'request_id', curreqid)
                    self.actionstartdf.set_value(ctoken, 'instance_id', curinsid)
                    self.actionstartdf.set_value(ctoken, 'global_id', globalid)
                    self.actionstartdf.set_value(ctoken, 'project_id',
                                            str(row_jsonft['_context_project_id']))
                    self.actionstartdf.set_value(ctoken, 'tenant_id',
                                            str(row_jsonft['payload']['tenant_id']))
                    self.actionstartdf.set_value(ctoken, 'user_id',
                                            str(row_jsonft['payload']['user_id']))
                    self.actionstartdf.set_value(ctoken, 'transaction_tstart', self.token2starttimeDic[curtoken][0])
                    self.actionstartdf.set_value(ctoken, 'transaction_tend',
                                            self.token2starttimeDic[curtoken][0] + np.timedelta64(self.maxtransactionduration,'s'))
                    self.token2reqidDic[curtoken] = list()
                    self.token2reqidDic[curtoken].append(curreqid)
                    self.token2insidDic[curtoken] = list()
                    self.token2insidDic[curtoken].append(curinsid)
                    return [self.actionstartdf[self.actionstartdf['token'] == curtoken]['global_id'].values[0], self.token2starttimeDic[curtoken][0]]

                else:
                    if (('timestamp' in list(row_jsonft.keys())) & (
                                'event_type' in list(row_jsonft.keys())) & \
                                ('_context_auth_token' in list(row_jsonft.keys())) & (
                                '_context_request_id' in list(row_jsonft.keys()))):
                        curtoken = str(row_jsonft['_context_auth_token'])
                        curreqid = str(row_jsonft['_context_request_id'])
                        curtime = np.datetime64(pd.to_datetime(row_jsonft['timestamp']))
                        if curtoken in list(self.actionstartdf['token'].values):
                            self.token2reqidDic[curtoken].append(curreqid)
                            if curreqid in list(self.token2reqidDic[curtoken]):
                                if self.actionstartdf.shape[0] >= 1:
                                    tend = np.timedelta64(0, 's')
                                    if (str(row_jsonft['event_type']) == self.endevent):
                                        tend = curtime
                                        tmpindex = self.actionstartdf[self.actionstartdf['token'] == curtoken][
                                            self.actionstartdf['request_id'] == curreqid].index[0]
                                        self.actionstartdf.set_value(tmpindex, 'transaction_tend', curtime)
                                    else:
                                        tend = self.actionstartdf[self.actionstartdf['token'] == curtoken][
                                            'transaction_tend'].values[0]
                                    if ((curtime >= self.actionstartdf[self.actionstartdf['token'] == curtoken][
                                        'transaction_tstart'].values[0]) & (curtime <= tend)):
                                        return [self.actionstartdf[self.actionstartdf['token'] == curtoken]['global_id'].values[0], curtime]
                                        
                         # **********************************************************************************************
                        # Include everyrow that has the same instance_id and it is between start and end
                        if (self.actionstartdf.shape[0] >= 1):
                            for curtoken in list(self.actionstartdf['token'].values):
                                curinsid = \
                                    self.actionstartdf[self.actionstartdf['token'] == curtoken]['instance_id'].values[0]
                                if str(curinsid) in str(row_jsonft[
                                                            'payload']):
                                    if (str(row_jsonft['event_type']) == self.endevent):
                                        tend = curtime
                                        tmpindex = self.actionstartdf[self.actionstartdf['token'] == curtoken][
                                            self.actionstartdf['request_id'] == curreqid].index[0]
                                        self.actionstartdf.set_value(tmpindex, 'transaction_tend', curtime)
                                    else:
                                        tend = self.actionstartdf[self.actionstartdf['token'] == curtoken][
                                            'transaction_tend'].values[0]
                                    if ((curtime >= self.actionstartdf[self.actionstartdf['token'] == curtoken][
                                        'transaction_tstart'].values[0]) & (curtime <= tend)):
                                        return [self.actionstartdf[self.actionstartdf['token'] == curtoken][self.actionstartdf['instance_id'] == curinsid]['global_id'].values[0], curtime]

                                        # **********************************************************************************************************************
            # Include all rwos from Openstack with event_type that has "image." and they are between start and end events
            else:
		if self.data_source == 'file': 
                  if self.actionstartdf.shape[0] >= 1:
                    if (str('receiver_user_id') in str(row_jsonft['payload'])) & (
                                str('receiver_tenant_id') in str(row_jsonft['payload'])):
                	curtime = np.datetime64(pd.to_datetime(row_jsonft['timestamp']))
                        # to generalize may need a for loop here
                        if (str('image.') in str(row_jsonft[
                                                     'event_type'])):  # (str(row_jsonft['event_type']) == openstackeventList[0]):  # image.send
                            opsownerid = str(row_jsonft['payload']['owner_id'])
                            opstenantid = str(row_jsonft['payload']['receiver_tenant_id'])
                            opsuserid = str(row_jsonft['payload']['receiver_user_id'])
                            tmptimeVec = self.actionstartdf[self.actionstartdf['tenant_id'] == opstenantid][
                                self.actionstartdf['user_id'] == opsuserid]['transaction_tstart'].values
                            tmpindexVec = self.actionstartdf[self.actionstartdf['tenant_id'] == opstenantid][
                                self.actionstartdf['user_id'] == opsuserid]['transaction_tstart'].index
                            difftimeVec0 = np.array([curtime - item for item in tmptimeVec])
                            difftimeVec = np.array(difftimeVec0[difftimeVec0 > np.timedelta64(0, 's')])
                            tokenind = tmpindexVec[np.where(difftimeVec0 == np.min(difftimeVec))[0]]
                            curtoken = self.actionstartdf['token'].iloc[tokenind].values[0]
                            curglobalid = self.actionstartdf['global_id'].iloc[tokenind].values[0]
                            tend = self.actionstartdf[self.actionstartdf['token'] == curtoken]['transaction_tend'].values[
                                0]
                            if (curtime <= tend):
                                return [curglobalid, curtime]

        except Exception as e:
            self.cerror = self.cerror + 1
            #print '***** Exception Error Occured *****: Row Number=', self.rowcount, 'Error Count=', self.cerror, ' , Error Message:', str(e)
