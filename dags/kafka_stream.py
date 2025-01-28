from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_data():
    import requests
    url = "https://randomuser.me/api/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        response = response.json()
        response = response['results'][0]
        return response
    except requests.exceptions.RequestException as error:
        print(f"An error occurred: {str(error)} ")
        return


def format_data(response):
    data = {}
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}"\
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['age'] = response['dob']['age']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()

    while True:
        if time.time() > current_time + 60:
            break
        try:
            response = get_data()
            data = format_data(response)
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            continue


# DAG Definition
dag = DAG(
    dag_id='user_automation',
    default_args={
        "owner": "Ertan",
        "start_date": datetime(2025, 1, 25, 10, 00)
    },
    schedule_interval="@daily",
    catchup=False
)

# Task definitions
streaming_task = PythonOperator(
    task_id="stream_data_from_api",
    python_callable=stream_data,
    provide_context=True,
    dag=dag
)


# stream_data()




