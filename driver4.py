import json
import sys
import uuid
import requests
import time
import datetime as dt
import threading
import statistics
from kafka import KafkaProducer

KAFKA_REGISTER_TOPIC = 'register'
KAFKA_METRICS_TOPIC = 'metrics'
KAFKA_HEARTBEAT_TOPIC = 'heartbeat'
KAFKA_TEST_CONFIG_TOPIC = 'test_config'
KAFKA_TRIGGER_TOPIC = 'trigger'
KAFKA_TSUNAMI_TOPIC = 'tsunami'
KAFKA_AVALANCHE_TOPIC = 'avalanche'

node_id = 'D1'
test_id = None
latencies = []

orchestrator_ip = sys.argv[1]
kafka_broker = sys.argv[2]


def send_to_kafka(topic, message):
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.close()

def register_node():
    register_message = {
        'node_id': node_id,
        'test_id': test_id,
        'node_ip': f'{node_id}:8080',
        'message_type': 'DRIVER_NODE_REGISTER'
    }
    send_to_kafka(KAFKA_REGISTER_TOPIC, register_message)

def start_load_test(target_url, target_throughput, total_requests):
    global test_id, latencies

    register_node()

    threading.Thread(target=send_requests, args=(target_url, total_requests)).start()

    time.sleep(2)

    test_id = str(uuid.uuid4())

    threading.Thread(target=send_heartbeats).start()

    test_config = {
        'test_id': test_id,
        'target_url': target_url,
        'target_throughput': target_throughput,
        'total_requests': total_requests,
    }
    send_to_kafka(KAFKA_TEST_CONFIG_TOPIC, test_config)

    trigger_message = {'test_id': test_id, 'trigger': 'YES', 'test_type': 'tsunami'}
    send_to_kafka(KAFKA_TRIGGER_TOPIC, trigger_message)

    latencies = []

def send_requests(target_url, total_requests):
    global latencies, test_id

    for _ in range(total_requests):
        start_time = dt.datetime.now().timestamp()
        response = requests.get(target_url)
        end_time = dt.datetime.now().timestamp()

        latency = (end_time - start_time)*1000
        #print(f"Latency: {latency}")

        latencies.append(latency)
        time.sleep(1)

    mean_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    metrics_message = {
        'node_id': node_id,
        'test_id': test_id,
        'report_id': str(uuid.uuid4()),
        'metrics': {
            'mean_latency': mean_latency,
            'median_latency': median_latency,
            'min_latency': min_latency,
            'max_latency': max_latency
        }
    }

    send_to_kafka(KAFKA_METRICS_TOPIC, metrics_message)

def send_heartbeats():
    while True:
        heartbeat_message = {
            'node_id': node_id,
            'heartbeat': 'YES',
            'timestamp': int(time.time())
        }
        send_to_kafka(KAFKA_HEARTBEAT_TOPIC, heartbeat_message)
        time.sleep(1)
        
def start_avalanche_test(target_url, target_throughput, max_total_requests):
    global test_id, latencies

    register_node()

    threading.Thread(target=send_requests_avalanche, args=(target_url, target_throughput, max_total_requests)).start()

    time.sleep(2)

    test_id = str(uuid.uuid4())

    threading.Thread(target=send_heartbeats).start()

    test_config = {
        'test_id': test_id,
        'target_url': target_url,
        'target_throughput': target_throughput,
        'total_requests': max_total_requests
    }
    send_to_kafka(KAFKA_TEST_CONFIG_TOPIC, test_config)

    trigger_message = {'test_id': test_id, 'trigger': 'YES'}
    send_to_kafka(KAFKA_TRIGGER_TOPIC, trigger_message)

    latencies = []

def send_requests_avalanche(target_url, target_throughput, max_total_requests):
    global latencies, test_id

    request_counter = 0

    while request_counter < max_total_requests:
        start_time = dt.datetime.now().timestamp()
        response = requests.get(target_url)
        end_time = dt.datetime.now().timestamp()

        latency = (end_time - start_time) * 1000

        latencies.append(latency)
        time.sleep(1 / target_throughput)

        request_counter += 1
        
    trigger_message = {'test_id': test_id, 'trigger': 'NO', 'test_type': 'avalanche'}
    send_to_kafka(KAFKA_TRIGGER_TOPIC, trigger_message)

if __name__ == '__main__':
    target_url = 'http://localhost:5000/ping'
    target_throughput = 5
    total_requests = 50
    start_load_test(target_url, target_throughput, total_requests)
