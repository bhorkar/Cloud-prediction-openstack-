import pika

def callback(ch, method, properties, body):
     print("Received: %s  bytes" % (body) )

# -----------------------------------------------------------
creds = pika.PlainCredentials( "scott", "BunnyHop" )
target = pika.ConnectionParameters( "135.197.226.53", 5672, "/", creds )

connection = pika.BlockingConnection( target )
channel = connection.channel()
channel.exchange_declare(exchange="re_pusher_i1", type="fanout", durable=False, auto_delete=True)       # change to i1 to select instance 1-4
q = channel.queue_declare( queue="scottq" )
channel.queue_bind( exchange='re_pusher_i1', queue="scottq" )

channel.basic_consume( callback, queue="scottq", no_ack=True )

print( "waiting for messages..." )
channel.start_consuming()


