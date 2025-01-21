from kafka import KafkaProducer
import requests
import json
import time
import sys
import pandas as pd

server = sys.argv[1]
key = sys.argv[2]
topic = sys.argv[3]

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))
dt = pd.to_datetime('today') - pd.Timedelta(days=365)
while dt < pd.to_datetime('today'):
  dt = dt + pd.Timedelta(days=1)
  dt_string = dt.strftime('%Y-%m-%d')
  address = server + '?key=' + key + '&q=Donetsk' + '&dt=' + dt_string + '&end_dt=' + dt_string
  data = requests.get(address)
  if data.status_code == 200:
    data = data.json()
    print(data[forecast])
