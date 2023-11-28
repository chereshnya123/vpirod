import pika, json, sys
import time as t
from config import *
def GetContext():
    global vars, update_num, update
    f = open(DUMPFILE, "r")
    vars['x'] = float(f.readline().split()[0])
    vars['y'] = float(f.readline().split()[0])
    vars['z'] = float(f.readline().split()[0])
    update_num = int(f.readline().split()[0])
    update = json.loads(f.readline())
    f.close()

def DumpData():
    global vars, update_num, update
    f = open(DUMPFILE, 'w')
    f.write(f"{vars['x']}\n{vars['y']}\n{vars['z']}\n{update_num}\n{json.dumps(update)}")
    f.close()

def Init():
    for i in range(LEADERS_AMOUNT):
        channel.queue_declare(queue=f"master{i}", auto_delete=False)
        channel.queue_bind(exchange="master", queue=f"master{i}", routing_key=str(i))
    channel.queue_declare(queue="master", auto_delete=False)
    channel.queue_bind(exchange="master", queue="master", routing_key="client_request")
    channel.queue_bind(exchange="master", queue="master", routing_key="ping")
    channel.queue_bind(exchange="master", queue="master", routing_key="terminate")
    
def TraceLeader():
    global channel, local_id, next_id, leader_id, local_state
    print(f"[{local_id}]: Watchdog : tracing = {next_id}")
    while (1):
        ping_body = dict({"id": local_id, "msg":"ping"})
        channel.basic_publish(exchange="master", routing_key="ping", body=json.dumps(ping_body))
        t.sleep(WAIT_TIMEOUT_SEC)
        received = channel.basic_get(queue=f"master{local_id}",
                        auto_ack=True)
        if received == (None, None, None):
            print(f"[{local_id}]: Leader is dead!")
            next_id = (next_id + 1) % LEADERS_AMOUNT
            if next_id == local_id:
                leader_id = local_id
                local_state = LEADER
                break
            vote_body = dict({"propose_id": local_id, "msg": "voting"})
            print(f"[{local_id}]: Master-watchdog start voting: Propose id = {local_id}, sent to = {next_id}")
            channel.basic_publish(exchange="master",
                                  routing_key=str(next_id),
                                  body=json.dumps(vote_body))
            local_state = NULL_STATE
            break
        else:
            continue

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
    DumpData()
    print(f"[{local_id}]: Leader variables: {vars}")

def HandleMessage(ch, method, properties, body):
    global vars, updates, update_num, leader_id, local_state
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
        return
    elif key == "ping":
        receiver_id = int(json.loads(body.decode())["id"])
        ack = dict({"msg":"OK"})
        channel.basic_publish(exchange="master",
                              routing_key=str(receiver_id),
                              body=json.dumps(ack))
        return
        
    msg = str(json.loads(body.decode())["msg"])
    if msg == "OK":
        pass
    elif msg == "voting":
        print(f"[{local_id}]: Vote")
        propose_id = int(json.loads(body.decode())["propose_id"])
        if propose_id == local_id:
            print(f"[{local_id}]: New leader is {local_id}")
            vote_body = dict({"leader_id":propose_id, "msg":"voting_end"})
            leader_id = local_id
            local_state = LEADER
            channel.basic_publish(exchange="master",
                                  routing_key=str(next_id),
                                  body=json.dumps(vote_body))
        elif propose_id > local_id:
            print(f"[{local_id}]: Voting: propose_id = {propose_id}, send to = {next_id}")
            vote_body = dict({"propose_id" : propose_id, "msg":"voting"})
            channel.basic_publish(exchange="master",
                                  routing_key=str(next_id),
                                  body=json.dumps(vote_body))
        elif propose_id < local_id:
            print(f"[{local_id}]: Voting: propose_id = {local_id}, send to = {next_id}")
            vote_body = dict({"propose_id":local_id, "msg": "voting"})
            channel.basic_publish(exchange="master",
                                  routing_key=str(next_id),
                                  body=json.dumps(vote_body))
            
    elif msg == "voting_end":
        leader_id = int(json.loads(body.decode())["leader_id"])
        print(f"[{local_id}]: Chose leader = {leader_id}")
        if leader_id == next_id:
            local_state = TRACING_LEADER
            vote_end_body = dict({"leader_id": leader_id, "msg":"voting_end"})
            channel.basic_publish(exchange="master",
                                  routing_key=str(next_id),
                                  body=json.dumps(vote_end_body))
            print(f"[{local_id}]: I'm tracer after voting: leader = {leader_id}")
            channel.stop_consuming()
        elif leader_id == local_id:
            local_state = LEADER
            print(f"[{local_id}]: I'm new leader after voting: leader = {leader_id}")
            channel.stop_consuming()
        else:
            local_state = NULL_STATE
            vote_end_body = dict({"leader_id":leader_id, "msg":"voting_end"})
            channel.basic_publish(exchange="master",
                                  routing_key=str(next_id),
                                  body=json.dumps(vote_end_body))
            print(f"[{local_id}]: I'm null state after voting: leader = {leader_id}")
            channel.stop_consuming()
    elif msg == "terminate":
        print(f"[{local_id}]: Leader is terminated by user")
        raise KeyboardInterrupt()
def HandleSlaveMessage(ch, method, properties, body):
    pass
    
vars = dict()
vars['x'] = 0
vars['y'] = 0
vars['z'] = 0

update = dict()
update_num = 0

local_id = int(sys.argv[1])
leader_id = LEADER_ID
next_id = (local_id + 1) % LEADERS_AMOUNT

local_state = NULL_STATE
if next_id == leader_id:
    local_state = TRACING_LEADER
    print(f"[{local_id}]: Start as tracing leader!")
elif local_id == leader_id:
    print(f"[{local_id}]: Start as leader!")
    local_state = LEADER
else:
    print(f"[{local_id}]: Start as NULL")
    local_state = NULL_STATE

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue=f"master{local_id}",
                      auto_delete=False)

channel.queue_bind(queue=f"master{local_id}",
                   exchange="master",
                   routing_key=str(local_id))
try:
    while (1):
        if local_state == LEADER:
            channel.basic_consume(queue="master",
                                on_message_callback=HandleMessage,
                                auto_ack=True)
            print(f"[{local_id}]: Enter LEADER state")
            channel.start_consuming()
            GetContext()
            Init()
            print(f"[{local_id}]: Stop consuming as LEADER, new ROLE = {local_state}")
        elif local_state == TRACING_LEADER:
            print(f"[{local_id}]: Enter TRACER state")
            TraceLeader()
            GetContext()
        elif local_state == NULL_STATE:
            print(f"[{local_id}]: Enter NULL_STATE")
            channel.basic_consume(queue=f"master{local_id}",
                                on_message_callback=HandleMessage,
                                auto_ack=True)
            channel.start_consuming()
            Init()
            GetContext()
            print(f"[{local_id}]: Stop consuming as NULL_STATE, new ROLE = {local_state}")
except KeyboardInterrupt:
    print(f"Master {local_id} was terminated by user")
except:
    print(f"Master {local_id} was terminated.")
    raise