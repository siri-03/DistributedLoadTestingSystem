from flask import Flask, jsonify

app = Flask(__name__)

request_count = 0
response_count = 0

@app.route('/ping')
def ping():
    global response_count
    response_count += 1
    return 'Pong!'

@app.route('/metrics')
def metrics():
    global request_count, response_count
    metrics_data = {
        'requests_received': request_count,
        'responses_sent': response_count
    }
    return jsonify(metrics_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
