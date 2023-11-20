#!usr/bin/env python3

import threading
from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import time
from tokensGen import generate_random_token, generate_reportID
import statistics
import sys

KAFKA_IP = sys.argv[1]
ORCH_IP = sys.argv[2]

kp = KafkaProducer(bootstrap_servers=KAFKA_IP)

threadLock = threading.Lock()
NODE_ID = generate_random_token()
res = []  # Initialize the res list
full = []
mean = 0
n = 0
m2 = float("inf")
m1 = 0
trigger_event = threading.Event()  # Event to trigger the metric function

TEST_CONFIG = None

def heartBeat():
    data = {
        "node_id": NODE_ID,
        "heartbeat": "YES",
    }
    while True:
        send_data("heartbeat", data)
        time.sleep(8)

def metric():
    while True:
        if TEST_CONFIG is None:
            exit()
        # Wait for the trigger event to be set before running the metric function
        global mean, m1, m2, n
        trigger_event.wait()
        while len(res) == 0:
            time.sleep(0.2)
            pass
        threadLock.acquire()
        res_len = len(res)
        tot_len = n + res_len
        mean = (sum(res) + mean * n) / (tot_len)
        b = tot_len
        m1 = max(m1, max(res))
        m2 = min(m2, min(res))
        full.extend(res)
        median = statistics.median(full)

        send_data("metrics", {"node_id": NODE_ID, "test_id": TEST_CONFIG["test_id"],
                              "report_id": generate_reportID(), "l": res_len, "val": res,
                              "metrics": {"mean_latency": mean, "median_latency": median,
                                          "max_latency": m1, "min_latency": m2}})
        res.clear()
        threadLock.release()
        
        # Clear the event to ensure it runs only once
        trigger_event.clear()
        time.sleep(1)

metric_thread = threading.Thread(target=metric)

def send_data(channel, data_dict):
    kp.send(channel, json.dumps(data_dict).encode("utf-8"))
    kp.flush()

def get_request():
    try:
        t1 = time.time()
        resp = requests.get(SERVER_URL)
        t2 = time.time()
        threadLock.acquire()
        res.append(t2 - t1)
        threadLock.release()
    except Exception as e:
        print(e)
        print("Error while trying to GET server @ " + str(time.localtime(t1)))

FREQ = None

def receive_data(msg):
    return json.loads(msg.value.decode("utf-8"))

def request_thread():
    global reqCount, intervalsStart, start
    start = time.time()
    reqCount = 0
    intervalsStart = start
    while time.time() - start < 25:
        get_request()
        reqCount += 1
        if reqCount == TEST_CONFIG["throughput"]:
            while time.time() - intervalsStart <= 1:
                pass
            intervalsStart = time.time()
            reqCount = 0
        # Set the trigger event to run the metric function
        trigger_event.set()
    kp.flush()
    kp.close()
    requestThread.join(0)
    metric_thread.join(0)
    sys.exit()

requestThread = threading.Thread(target=request_thread)

if __name__ == "_main_":
    SERVER_URL = "http://127.0.0.1:5000"
    kc = KafkaConsumer(bootstrap_servers=KAFKA_IP)
    kc.subscribe(["test_config", "register", "trigger"])
    send_data("register", {
        "node_id": NODE_ID,
        "node_IP": NODE_ID,
        "message_type": "DRIVER_NODE_REGISTER",
    })

    hb_anchor = threading.Thread(target=heartBeat)
    hb_anchor.start()

    for msg in kc:
        if msg.topic == "test_config":
            TEST_CONFIG = receive_data(msg)

        elif msg.topic == "trigger":
            data = receive_data(msg)
            if data["trigger"] == "NO":
                kp.flush()
                kp.close()
                requestThread.join(0)
                metric_thread.join(0)
                sys.exit()

            requestThread.start()
            metric_thread.start()
            res.clear()  # Clear the res list

    # Close Kafka session
kp.flush()
kp.close()
