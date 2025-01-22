from kafka import KafkaProducer
import requests
import json
import time
import sys
import pandas as pd

key = sys.argv[1]
topic = sys.argv[2]

server = 'https://api.weatherapi.com/v1/history.json'
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))
oblasts = ['Donetsk',
            'Luhansk',
            'Dnipropetrovsk',
            'Zaporizhzhya',
            'Kharkiv',
            'Kherson',
            'Sumy',]
ob_id = len(oblasts) - 1
while True:
  if ob_id == len(oblasts) - 1:
    ob_id = 0
  else:
    ob_id += 1
  oblast = oblasts[ob_id]
  dt = pd.to_datetime('today') - pd.Timedelta(days=365)
  while dt < pd.to_datetime('2025-01-22'):
    dt = dt + pd.Timedelta(days=1)
    dt_string = dt.strftime('%Y-%m-%d')
    address = server + '?key=' + key + '&q=' + oblast + '&dt=' + dt_string + '&end_dt=' + dt_string
    data = requests.get(address)
    if data.status_code == 200:
      data = data.json()
      data['forecast']['oblast'] = oblast
      print(data['forecast'])
