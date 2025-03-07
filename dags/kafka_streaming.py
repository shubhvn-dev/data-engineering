from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'shubhan',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 5),
}

def format_data(res):
    import json
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} - {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registration Date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['cell'] = res['cell']
    data['picture'] = res['picture']['large']

    return data

def get_data():
    import json
    import requests
    import time

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]

    return res

def stream_data():
    import json
    from kafka import KafkaProducer
    import time

    res = get_data()
    res = format_data(res)
    # print(json.dumps(res, indent=4))

    producer = KafkaProducer(bootstrap_servers='localhost:9092', max_block_ms=5000, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(('user-created'), value=res)
# with DAG('user-automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
#     streaming_data = PythonOperator(
#         task_id='streaming_data_from_api',
#         python_callable=stream_data
#     )

stream_data();