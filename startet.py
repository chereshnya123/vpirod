import pika, subprocess
from protocol import *
import sys

processes = []

def StartMasterProcess():
    global processes, replikas_amount
    processes.append(subprocess.Popen(
    f"python3 master1.py {replikas_amount}".split(), stdin=None, stdout=None,
    stderr=None, close_fds=True
    ))

def StartNodes(number_of_workers):
    global processes, replikas_amount
    for i in range(number_of_workers):
        processes.append(subprocess.Popen(
        f"python3 node1.py {i}".split(), stdin=None, stdout=None,
        stderr=None, close_fds=True
        ))

def StartSimulatorProcess():
    global processes, replikas_amount
    processes.append(subprocess.Popen(
    f"python3 simulator1.py {replikas_amount}".split(), stdin=None, stdout=None,
    stderr=None, close_fds=True
    ))

def StopProcesses():
    global processes
    print("Wrap up system...")
    for p in processes:
        p.terminate()

replikas_amount = int(sys.argv[1])

conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = conn.channel()

# Master's queue & exchange for getting client requests
channel.queue_declare(queue="master", auto_delete=True)
channel.exchange_declare(exchange="master", exchange_type="direct")
channel.queue_bind(queue="master", exchange="master",
                   routing_key="client_request")
# Simulator queue & exchange
channel.queue_declare(queue="simulator", auto_delete=True)
channel.exchange_declare(exchange="simulator", exchange_type="direct")
channel.queue_bind(exchange='simulator', queue='simulator')
# Replika's queue & exchange
channel.exchange_declare(exchange="replika_requests", exchange_type="direct")

StartMasterProcess()
StartSimulatorProcess()
StartNodes(replikas_amount)

channel.close()
conn.close()
try:
    import client1
except:
    print("Client terminated")
    StopProcesses()