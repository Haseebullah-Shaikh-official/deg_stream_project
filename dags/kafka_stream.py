# Databricks notebook source
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'Haseebullah',
    'start_date': datetime(2023, 12, 4, 10, 24)
}

def get_data():
    import requests

    response = requests.get("https://randomuser.me/api/")
    result = response.json()['results'][0]
    
    return result

def format_data(result):
    data = {}
    print(result)
    location = result['location']
    # data['id'] = uuid.uuid4()
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['gender'] = result['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = result['email']
    data['username'] = result['login']['username']
    data['dob'] = result['dob']['date']
    data['registered_date'] = result['registered']['date']
    data['phone'] = result['phone']
    data['picture'] = result['picture']['medium']

    return data



def stream_data():
    import json 
    from kafka import KafkaProducer
    import time
    
    result = get_data()
    result = format_data(result)
    # print(json.dumps(result, indent=3))
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send("users_created", json.dumps(result).encode('utf-8'))


with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
    
stream_data();