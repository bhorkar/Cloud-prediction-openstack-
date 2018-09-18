#!/usr/bin/python
# useful: http://pika.readthedocs.io/en/0.11.0b1/modules/channel.html

# vim: ts=4 sw=4 noet:


import pika
import json
import string
import random
import logging
LOG = logging.getLogger(__name__);


#
#	for other channel calls.  You'd think declare would return the name, or an object that
#	could be passed to bind/consume. Sigh.
#
class SherlockListener():
	def __init__ (self, config):
		self.pw                 =  config['rabbitmq_input']['stream_parameters']['pw']; 
		self.host               =  config['rabbitmq_input']['stream_parameters']['collector_source']; 
		self.exchange           =  config['rabbitmq_input']['stream_parameters']['exchange'];
		self.uname              =  config['rabbitmq_input']['stream_parameters']['uname']; 
		self.random_string_length = 10;
		self.msglist = [];
		self.flush_sherlock_msglist_interval = config['rabbitmq_input']['stream_parameters']['flush_sherlock_msglist_interval']
	def start_listener(self):
		LOG.debug( "connecting to: %s@%s" % (self.uname, self.host) )
                try: 
			creds = pika.PlainCredentials( self.uname, self.pw )
			target = pika.ConnectionParameters( self.host, 5672, "/", creds )
			qname = "some-random-name" + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(self.random_string_length))
                        LOG.debug("used qname for sherlock rabbitmq " + qname);
                      	connection = pika.BlockingConnection( target )

			channel = connection.channel()
			channel.exchange_declare( exchange= self.exchange, type='fanout', durable=False, auto_delete=True )		# change to ix to select instance 0-6
			channel.queue_declare( queue=qname, exclusive=True, durable=False, auto_delete=True )		
			channel.queue_bind( exchange= self.exchange, queue=qname )
			channel.basic_consume(self.callback, queue=qname, no_ack=True )
			channel.start_consuming()
			LOG.debug( "connected to: %s@%s" % (uname, host) )
		except:
			LOG.error("Could not connect to sherlock server")


# Called for each bundle received on the exchange.  This illustrates how to loop through
# the messages, looking for the guest UUID that could be hidden in any of the possible
# spots identified by the mtags data for "vm_uuid".
#
	def callback(self, ch, method, properties, body ):
		data = json.loads( str( body ) )
		msgs = data.get( "msgs" )
                for msg in msgs:
			self.msglist.append([msg])
     
	def return_msg_list(self):
		return  self.msglist;
	def flush_prevmsg(self):
		self.msglist = [];



# all of the necessary things to start listening on a rabbit exchange
# TODO: add random name generation (queue declare happily accepts an empty name, generates
#	one behind the scenes, but I can't find any way to dig it out and the name is neede
