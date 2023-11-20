#!/usr/bin/env python3

from kafka import KafkaProducer, KafkaConsumer
import json
import sys
from tokensGen import generate_testID
import statistics
import threading

def send_message(producer, channel, data):
    producer.send(channel, json.dumps(data).encode("utf-8"))
    producer.flush()

def handle_triggers(producer, test_id, trigger_value):
    print("Handle_triggers called with", trigger_value)
    send_message(producer, "trigger", {"test_id": test_id, "trigger": trigger_value})

def receive_message(message):
    return json.loads(message.value.decode("utf-8"))

def listen(kafka_consumer, global_metrics):
    for msg in kafka_consumer:
        print("msg received", msg.topic, msg.value)
        
        if msg.topic == "heartbeat":
            temp = receive_message(msg)
            print("heartbeat received!" + temp["node_id"])
        
        elif msg.topic == "metrics":
            temp = receive_message(msg)
            print("metrics received!")
            # TODO process to global metrics
            if temp["node_id"] not in global_metrics:
                global_metrics[temp["node_id"]] = []
            global_metrics[temp["node_id"]].extend(temp["val"])
            for k in global_metrics:
                print(k, len(global_metrics[k]))

def main():
    ip_kafka = sys.argv[1]
    orch_ip = sys.argv[2]
    kp = KafkaProducer(bootstrap_servers=ip_kafka)

    global_metrics = {}
    kc = KafkaConsumer(bootstrap_servers=ip_kafka)
    kc.subscribe(["metrics", "heartbeat", "register"])

    listen_thread = threading.Thread(target=listen, args=(kc, global_metrics))
    listen_thread.start()

    test_type = input().upper()
    test_id = generate_testID()

    if test_type == "TSUNAMI":
        print("not yet supported!")
        exit()
    elif test_type == "AVALANCHE":
        print("enter desired max throughput requests per second per driver node")
        throughput = int(input())
        send_message(kp, "test_config", {
            "test_id": test_id,
            "test_type": test_type,
            "throughput": throughput,
            "test_message_delay": 0,
        })

        handle_triggers(kp, test_id, "YES")
    else:
        print("invalid type!!")
        exit()

    while True:
        user_input = input()
        if user_input.upper() == "EXIT":
            handle_triggers(kp, test_id, "NO")
            break

    listen_thread.join()

    kp.flush()
    kp.close()

if __name__ == "_main_":
    main()