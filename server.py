#!usr/bin/env python3

from flask import Flask, send_file, request, jsonify
from flask_cors import CORS
import threading

app = Flask(__name__)
CORS(app)  # Enable CORS for the entire app

data_metrics = {}

@app.route('/')
def home():
    global data_metrics
    metrics_str = "\n".join([f"{n}: {metrics}" for n, metrics in data_metrics.items()])
    
    # Display the metrics
    return f"Welcome to the load testing dashboard!<br><br>Metrics:<br>{metrics_str}"

@app.route('/metrics', methods=['POST'])
def receive_metrics():
    global data_metrics
    received_data = request.json
    node_id = received_data.get('node_id')
    test_id = received_data.get('test_id')
    node_metrics = received_data.get('metrics', {})
    
    # Store the received metrics
    data_metrics[(node_id, test_id)] = node_metrics
    
    return jsonify({"status": "Success! Metrics recieved!"})

if __name__ == '_main_':
    app.run(debug=True)