from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import uuid
import json
import requests

from confluent_kafka import Producer
import time
import logging



def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res
    

def format_data(res):
    data = {}
    location = res['location']
    # data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    # res = get_data()
    # res = format_data(res)
    
    conf = {
        'bootstrap.servers': 'broker:29092' 
        #'localhost:9092'  for Local Test; 
        #'broker:29092'    for Docker Running
    }
    
    producer = Producer(conf) # KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
    
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.produce(
                'users_created'
                , json.dumps(res).encode('utf-8')
                , callback=delivery_report)
            # producer.flush()

        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


default_args = {
    'owner':'Gary Hsu'
    , 'start_date': datetime(2024, 6, 21, 22, 00)
}

with DAG(
    'user_automation'
    , default_args = default_args
    , schedule_interval='*/1 * * * *'
    , catchup=False ) as dag:

    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api'
        , python_callable=stream_data
    )

streaming_task