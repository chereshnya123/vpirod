import pika, json
from protocol import *

vars = dict()
vars['x'] = 0
vars['y'] = 0
vars['z'] = 0
update = dict()
update_num = 0

# Update current update_struct
def AddUpdateBatch(body: dict):
    global update, update_num
    
    update[str(update_num)] = body
    update_num += 1

# Update local values
def UpdateLocalVariables(body):
    global vars
    
    if body["op"] == "mul":
        vars[body["var"]] *= body["value"]
    else:
        vars[body["var"]] += body["value"]
    print(f"Master variables: {vars}")

def HandleMessage(ch, method, properties, body):
    global vars, updates, update_num
    key = method.routing_key
    if key == "client_request":
        update = json.loads(body.decode())
        AddUpdateBatch(update)
        UpdateLocalVariables(update)
        '''
        sends: {
            "i": {var, op, value}
        }
        '''
        update_json = dict()
        update_json[update_num] = update
        channel.basic_publish(exchange="simulator",
                              routing_key="simulator",
                              body=json.dumps(update_json))
          
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.basic_consume(queue="master",
                      on_message_callback=HandleMessage,
                      auto_ack=False)

# try:
channel.start_consuming()
# except:
#     print("Master terminated")