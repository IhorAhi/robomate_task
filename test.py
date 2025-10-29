import requests
import psycopg2
import pytest
import time

def insert_data():
    url = "http://localhost:8000/events"

    with open("events_sample.csv", "rb") as f:
        file= {"file": f}
        response = requests.post(url, files=file)
        print(response.text)
        print(response)
    return response.text, response

def insert_data_200k():
    url = "http://localhost:8000/clear"
    response = requests.get(url)
    print(response.text)
    print(response)

    start = time.time()

    url = "http://localhost:8000/events"

    with open("events_sample_200k.csv", "rb") as f:
        file= {"file": f}
        response = requests.post(url, files=file)

    print(response.text)
    print(response)
    print('{} sec passed'.format(time.time() - start))
    return response.text, response

def test_idemp():
    url = "http://localhost:8000/clear"
    response = requests.get(url)
    print(response.text)
    print(response)

    insert_data()

    x = insert_data()[0]
    assert x == 'Inserted 0 amount of rows'

def test_integr():
    url = "http://localhost:8000/clear"
    response = requests.get(url)
    print(response.text)
    print(response)

    insert_data()

    url = "http://localhost:8000/stats/retention?start_date=2025-08-21&windows=1"
    response = requests.get(url)
    assert response.json()[0][0] == 0.4074074

insert_data_200k()