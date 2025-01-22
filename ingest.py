from kafka import KafkaProducer
import requests
import json
import time
import sys
import pandas as pd

server = sys.argv[1]
key = sys.argv[2]
topic = sys.argv[3]
oblasts = json.load(sys.argv[4])

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))
ob_id = len(oblasts) - 1
while True:
  if ob_id == len(oblasts) - 1:
    ob_id = 0
  else:
    ob_id += 1
  oblast = oblasts[ob_id]
  dt = pd.to_datetime('today') - pd.Timedelta(days=365)
  while dt < pd.to_datetime('today'):
    dt = dt + pd.Timedelta(days=1)
    dt_string = dt.strftime('%Y-%m-%d')
    address = server + '?key=' + key + '&q=' + oblast + '&dt=' + dt_string + '&end_dt=' + dt_string
    data = requests.get(address)
    if data.status_code == 200:
      data = data.json()
      data['forecast']['oblast'] = oblast
      print(data['forecast'])
