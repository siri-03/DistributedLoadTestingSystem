
from flask import Flask, request, jsonify, current_app, g
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
import threading
import time
import statistics

app = Flask(__name__)

KAFKA_BROKER = 'localhost:9092'
KAFKA_REGISTER_TOPIC = 'register'
KAFKA_TEST_CONFIG_TOPIC = 'test_config'
KAFKA_TRIGGER_TOPIC = 'trigger'
KAFKA_METRICS_TOPIC = 'metrics'

registered_nodes = {}
current_test_id = None
metrics_store = {}
dashboard_data = {'min_latency': 0, 'max_latency': 0, 'mean_latency': 0, 'median_latency': 0}

def send_to_kafka(topic, message):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.close()

def get_current_test_id():
    with current_app.app_context():
        return g.get('current_test_id', None)

metrics_lock = threading.Lock()

def update_dashboard():
    global dashboard_data
    while True:
        time.sleep(1)
        with app.app_context():
            metrics_store_data = get_metrics_store()
            #print("Metrics Store Data:", metrics_store_data)
            
            with metrics_lock:
                calculate_aggregated_metrics(metrics_store_data)
            
            #print("Dashboard Data:", get_dashboard_data())


def calculate_aggregated_metrics(metrics_store):
    global dashboard_data
    aggregated_data = {'min_latency': [], 'max_latency': [], 'mean_latency': [], 'median_latency': []}
    
    for test_id, metrics_list in metrics_store.items():
        for metric in metrics_list:
            for key, value in metric['metrics'].items():
                aggregated_data[key].append(value)
    
    for key, values in aggregated_data.items():
        if values:
            dashboard_data[key] = statistics.mean(values)
        else:
            dashboard_data[key] = 0.0


    if all(value == 0.0 for value in dashboard_data.values()):
        dashboard_data['min_latency'] = min(aggregated_data['min_latency'], default=0.0)
        dashboard_data['max_latency'] = max(aggregated_data['max_latency'], default=0.0)
        if aggregated_data['mean_latency']:
            dashboard_data['mean_latency'] = statistics.mean(aggregated_data['mean_latency'])
        else:
            dashboard_data['mean_latency'] = 0.0
            
        if aggregated_data['median_latency']:
            dashboard_data['median_latency'] = statistics.median(aggregated_data['median_latency'])
        else:
            dashboard_data['median_latency'] = 0.0


def get_metrics_store():
    with current_app.app_context():
        if 'metrics_store' not in g:
            g.metrics_store = {}
        return g.metrics_store

def get_dashboard_data():
    global dashboard_data
    if 'dashboard_data' not in g:
        g.dashboard_data = {'min_latency': 0, 'max_latency': 0, 'mean_latency': 0, 'median_latency': 0}
    return g.dashboard_data

@app.route('/register', methods=['POST'])
def register_node():
    data = request.get_json()

    node_id = data['node_id']
    test_id = data['test_id']
    node_ip = data['node_ip']
    registered_nodes[node_id] = {'node_ip': node_ip, 'last_heartbeat': time.time()}

    return jsonify({'message': 'Node registered successfully'}), 201

@app.route('/start-test', methods=['POST'])
def start_test():
    global current_test_id, metrics_store

    test_config = request.get_json()

    current_test_id = str(uuid.uuid4())
    test_config['test_id'] = current_test_id

    send_to_kafka(KAFKA_TEST_CONFIG_TOPIC, test_config)

    trigger_message = {'test_id': current_test_id, 'trigger': 'YES'}
    send_to_kafka(KAFKA_TRIGGER_TOPIC, trigger_message)

    metrics_store = {}

    return jsonify({'test_id': current_test_id}), 200

@app.route('/stop-test', methods=['POST'])
def stop_test():
    global current_test_id, dashboard_data

    current_test_id = None
    dashboard_data = {'min_latency': 0, 'max_latency': 0, 'mean_latency': 0, 'median_latency': 0}
    
    return jsonify({'message': 'Test stopped successfully'}), 200

@app.route('/get-statistics', methods=['GET'])
def get_statistics():
    global metrics_store
    if current_test_id:
        return jsonify({'statistics': metrics_store})
    else:
        return jsonify({'message': 'No active test'}), 200

def metrics_consumer():
    consumer = KafkaConsumer(KAFKA_METRICS_TOPIC, bootstrap_servers=KAFKA_BROKER, group_id='orchestrator_group')
    with app.app_context():
        for message in consumer:
            handle_metrics(json.loads(message.value.decode('utf-8')))

def handle_metrics(metrics_message):
    global metrics_store
    required_keys = ['node_id', 'test_id', 'report_id', 'metrics']

    if all(key in metrics_message for key in required_keys):
        node_id = metrics_message['node_id']
        test_id = metrics_message['test_id']
        report_id = metrics_message['report_id']
        metrics_data = metrics_message['metrics']

        with metrics_lock:
            metrics_store = get_metrics_store()

            if test_id not in metrics_store:
                metrics_store[test_id] = []
            
            output_format = {'node_id': node_id, 'test_id': test_id, 'report_id': report_id, 'metrics': metrics_data}

            metrics_store[test_id].append(output_format)
            print(output_format)

if __name__ == '__main__':
    threading.Thread(target=update_dashboard).start()
    threading.Thread(target=metrics_consumer).start()
    app.run(host='0.0.0.0', port=8080, threaded=True)

