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
    data['id'] = str(uuid.uuid4())
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


if __name__ == "__main__":
    conf = {
        'bootstrap.servers': 'localhost:9092' 
        #'localhost:9092'  for Local Test; 
        #'broker:29092'    for Docker Running
    }
    producerTopic = 'users_created'
    producer = Producer(conf) 

    

    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    while True:
        print('Sending--------')
        res = get_data()
        res = format_data(res)

        producer.produce(
            producerTopic
            , json.dumps(res).encode('utf-8')
            , callback=delivery_report)
        
        print(f'{res} Send--------')
        # Wait for 5 seconds before producing the next message
        time.sleep(5)

        # Poll the producer to handle delivery reports
        producer.poll(0)