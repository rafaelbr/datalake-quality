import json
import random
import time
from datetime import datetime

import boto3

QUEUE_NAME = 'geekfoxlab-sb-ups-queue'
boto3.setup_default_session(region_name='us-east-1', profile_name='geekfoxlab-sb-admin')

def generate_message():
    return [{
        'last_input_vac': random.uniform(0, 227.0),
        'input_vac': random.uniform(117.0, 227.0),
        'output_vac': random.uniform(0, 127.0),
        'output_power': random.uniform(0, 100.0),
        'power_now': random.uniform(0, 100.0),
        'output_hz': random.uniform(0, 90.0),
        'battery_level': random.uniform(0, 100.0),
        'temperature': random.uniform(0, 100.0),
        'beep_on': True,
        'shutdown_active': False,
        'test_active': False,
        'ups_ok': True,
        'boost': (random.randint(0, 1) == 1),
        'bypass': False,
        'low_battery': (random.randint(0, 1) == 1),
        'battery_in_use': (random.randint(0, 1) == 1),
        'publish_time': datetime.today().strftime('%Y %m %d %H-%M-%S'),
        'info': "UPS Senoidal",
        'name': 'UPS Server',
        'no_data': False
    }]

def send_to_sqs(message):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    response = queue.send_message(MessageBody=json.dumps(message))
    print(response.get('MessageId'))

while(True):
    message = generate_message()
    send_to_sqs(message)
    print(message)
    time.sleep(5)
