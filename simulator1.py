import pika, json, sys
import random as rand
from config import *

nodes_amount = int(sys.argv[1])

def SendUpdates():
    global channel, update_batch
    for i in range(nodes_amount):
        send_seq = rand.sample(range(MAX_CACHE_SIZE), MAX_CACHE_SIZE)
        for update_num in send_seq:
            channel.basic_publish(body=json.dumps(update_batch[update_num]),
                                  exchange="replika_requests",
                                  routing_key=str(i))

def HandleMessage(ch, method, properties, body):
    global update_batch
    update = json.loads(body)
    # print("simulator update: ", update)
    update_batch.append(update)
    if len(update_batch) == MAX_CACHE_SIZE:
        SendUpdates()
        update_batch = []

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

'''
Holds list of:
{
    'i':{
        "var": ... ,
        "op": ... ,
        "value": ... 
    }
}
'''
update_batch = []

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.basic_consume(queue="simulator",
                      on_message_callback=HandleMessage,
                      auto_ack=False)

try:
    channel.start_consuming()
except:
    print("Simulator wrapped up")