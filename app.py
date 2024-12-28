from flask import Flask, request, jsonify
from confluent_kafka import Producer, Consumer
import json
import uuid
import threading
from threading import Lock
import time


app = Flask(__name__)

# Kafka Configuration
KAFKA_BROKER = 'test-kafka-1:9092'
TOPICS = {
    'en2ar': 'translationEnToArabicRequest',
    'ar2en': 'translationArToEnRequest',
    'summary': 'summarization-request',
}
RESPONSE_TOPICS = {
    'en2ar': 'translationEnToArabicResponse',
    'ar2en': 'translationArToEnResponse',
    'summary': 'summarization-response',
}

# Kafka Producer Configuration
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'flask_app',
    'auto.offset.reset': 'latest',  # Start consuming only new messages
}
consumer = Consumer(consumer_conf)
consumer.subscribe(list(RESPONSE_TOPICS.values()))

# Shared dictionary to store responses and a lock for thread safety
responses = {}
responses_lock = Lock()

# Kafka Producer
def produce_to_kafka(topic, payload):
    """Send a message to the appropriate Kafka topic."""
    producer.produce(topic, json.dumps(payload).encode('utf-8'))
    producer.flush()
    print(f"[Kafka Producer] Sent message to topic '{topic}': {payload}")

# Kafka Consumer Thread
def consume_kafka_responses():
    """Continuously listen for responses from Kafka topics."""
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            message = json.loads(msg.value().decode('utf-8'))
            task_id = message.get("id") or message.get("task_id")  # Handle different key names
            if task_id:
                with responses_lock:
                    responses[task_id] = message
            print(f"[Kafka Consumer] Received message: {message}")
        except json.JSONDecodeError as e:
            print(f"Failed to decode Kafka message: {e}")

# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_kafka_responses, daemon=True)
consumer_thread.start()

# API Endpoints
@app.route('/translate/en2ar', methods=['POST'])
def handle_en2ar_request():
    """Handle English to Arabic translation requests."""
    data = request.get_json()
    if not data or 'text' not in data:
        return jsonify({"error": "Invalid input. 'text' is required."}), 400

    task_id = str(uuid.uuid4())
    text = data['text']
    message = {
        "id": task_id,
        "text": text,
    }
    produce_to_kafka(TOPICS['en2ar'], message)
    return jsonify({"task_id": task_id, "status": "sent to Kafka"}), 202

@app.route('/translate/ar2en', methods=['POST'])
def handle_ar2en_request():
    """Handle Arabic to English translation requests."""
    data = request.get_json()
    if not data or 'text' not in data:
        return jsonify({"error": "Invalid input. 'text' is required."}), 400

    task_id = str(uuid.uuid4())
    text = data['text']
    message = {
        "id": task_id,
        "text": text,
    }
    produce_to_kafka(TOPICS['ar2en'], message)
    return jsonify({"task_id": task_id, "status": "sent to Kafka"}), 202

@app.route('/summary', methods=['POST'])
def handle_summary_request():
    """Handle text summarization requests."""
    data = request.get_json()
    if not data or 'text' not in data:
        return jsonify({"error": "Invalid input. 'text' is required."}), 400

    task_id = str(uuid.uuid4())
    text = data['text']
    style = data.get('style', 'formal')  # Default style if not provided

    message = {
        "task_id": task_id,
        "text": text,
        "style": style
    }
    produce_to_kafka(TOPICS['summary'], message)
    return jsonify({"task_id": task_id, "status": "sent to Kafka"}), 202

@app.route('/response/<task_id>', methods=['GET'])
def get_response(task_id):
    """Retrieve the response for a given task ID."""
    timeout = 10  # Timeout in seconds
    start_time = time.time()

    while time.time() - start_time < timeout:
        with responses_lock:
            response = responses.pop(task_id, None)  # Fetch and remove the response
        if response:
            return jsonify(response), 200
        time.sleep(0.5)  # Poll every 0.5 seconds

    return jsonify({"error": "Result not ready or task ID not found."}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8200)
