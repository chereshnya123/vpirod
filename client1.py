import pika, json
from config import *

def TerminateMaster(master_id):
    global channel
    terminate_body = dict({"msg":"terminate", "terminate_id": master_id})
    channel.basic_publish(exchange="master",
                          routing_key="terminate",
                          body=json.dumps(terminate_body))

    channel.basic_publish(exchange="master",
                          routing_key="master",
                          body=json.dumps(terminate_body))
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

while True:
    request = dict()
    query = input().split()
    if (len(query) == 0):
        continue
    print("query[0] = ", query[0])
    print("query = ", query)
    if (query[0] == "terminate"):
        TerminateMaster(int(query[1]))
        continue
        
    if len(query) != 3:
        print(f"Expected 3 parameters, got {len(query)}")
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