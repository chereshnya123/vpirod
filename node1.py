import pika, json, sys, signal

id = sys.argv[1]

file = open(f"nodes/log{id}.txt", 'w')

def HandleSignal(a, b):
    global file
    file.close()
    sys.exit(0)
    
signal.signal(signal.SIGINT,
              HandleSignal)
last_update = 0


vars = {'x': 0, 'y': 0, 'z': 0}
update_cache = dict()

def UpdateFromCache():
    global update_cache, last_update
    # print("trying add from cache")
    if len(update_cache.keys()) == 0: 
        # print("empty cache!")
        return False
    if last_update+1 in list(update_cache.keys()):
        print(f"NODE[{id}]: Update from cache: ", update_cache[(last_update+1)])
        # print("Update: ", update_cache)
        UpdateData(update_cache[(last_update+1)])
        del update_cache[(last_update)]
        return True
    else:
        # print("Cache: ", update_cache)
        # print(f'{(last_update+1)} != {(list(update_cache.keys())[0])}')
        return False
        
        
def UpdateData(update):
    global f, last_update
    var = update['var']
    update_str = f"Update {var}:{vars[update['var']]} -> "
    if update['op'] == 'mul':
        vars[update['var']] *= (update["value"])
    else:
        vars[update['var']] += (update["value"])
    update_str += f"{var}:{vars[update['var']]}\n"
    last_update += 1
    # file.write(f"Node {id} updated: {update_str}\n")
    file.write(f"Node {id} variables: x = {vars['x']}, y = {vars['y']}, z = {vars['z']}\n")
    
    file.flush()

'''
Receives:
{
    'i': {var, op, value}
}
'''

def HandleUpdate(ch, method, properties, body):
    global last_update
    update = json.loads(body)
    # print("Node got: ", update)
    update_nums = int(list(update.keys())[0])
    if update_nums == last_update + 1:
        print(f"NODE[{id}]: Update immediately with:", update[str(update_nums)])
        UpdateData(update[str(update_nums)])
        update = None
    else:
        update_cache[update_nums] = update[str(update_nums)]
    while UpdateFromCache(): 
        pass

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue=f"node{id}",
                      auto_delete=True)

channel.queue_bind(queue=f"node{id}",
                   exchange="replika_requests",
                   routing_key=id)

channel.basic_consume(f"node{id}",
                      on_message_callback=HandleUpdate,
                      auto_ack=True)

# try: 
channel.start_consuming()
# except:
    # print(f"Node {id} terminated")
    # file.close()