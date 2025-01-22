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
            #'Dnipropetrovsk',
            'Zaporizhzhya',
            'Kharkiv',
            'Kherson',
            #'Sumy',
          ]
df = pd.read_csv('2024-01-01-2025-01-22-Russia-Ukraine.csv', on_bad_lines='warn')
df.loc[df['admin1']=='Zaporizhia', 'admin1'] = 'Zaporizhzhya'
df['datetime'] = pd.to_datetime(df['event_date'])
df = df.loc[df['event_type'].isin(['Battles'])]
df = df.loc[df['country'].isin(['Ukraine'])]
df = df.loc[df['admin1'].isin(oblasts)]
df = df[['datetime', 'admin1', 'event_type']]
totals = df.groupby('admin1').count()['event_type']
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
      total_battles = int(totals.loc[oblast])
      daily_battles = int(df.loc[(df['datetime'] == dt) & (df['admin1'] == oblast), 'event_type'].count())
      target = daily_battles / total_battles
      data['forecast']['target'] = target
      print(data['forecast'])
      print(f'{target:.8f}')
