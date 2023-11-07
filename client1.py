import pika, json
from protocol import *

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

while True:
    request = dict()
    query = input().split()
    if len(query) != 3:
        continue
    if query[0] not in VARIABLES:
        print("Not valid variable name!\nAvailable variables are: ", *VARIABLES)
        continue
    elif query[1] not in OPS:
        print("Not valid operation!\nAvailable operations are: ", *OPS)
        continue
    
    variable, operation, value = query[0], query[1], float(query[2])
    request['var'] = variable
    request['op'] = operation
    request['value'] = value
    
    channel.basic_publish(body=json.dumps(request),
                          exchange="master",
                          routing_key="client_request")