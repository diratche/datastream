from kafka import KafkaProducer
import requests
import json
import time
import sys
import pandas as pd
from datetime import datetime

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
  dt = pd.Timestamp.today().normalize() - pd.Timedelta(days=365)
  while dt < pd.to_datetime('2025-01-22'):
    dt += pd.Timedelta(days=1)
    dt_string = dt.strftime('%Y-%m-%d')
    address = server + '?key=' + key + '&q=' + oblast + ',Ukraine' + '&dt=' + dt_string + '&end_dt=' + dt_string
    data = requests.get(address)
    if data.status_code == 200:
      data = data.json()
      output = {}
                
      output['oblast'] = oblast
                
      total_battles = int(totals.loc[oblast])
      daily_battles = int(df.loc[(df['datetime'] == dt) & (df['admin1'] == oblast), 'event_type'].count())
      output['target'] = daily_battles / total_battles

      output['features'] = {}
      output['features']['temperature'] = data['forecast']['forecastday'][0]['day']['avgtemp_c']
      output['features']['wind'] = data['forecast']['forecastday'][0]['day']['maxwind_kph']
      output['features']['precipitation'] = data['forecast']['forecastday'][0]['day']['totalprecip_mm']
      output['features']['snow'] = data['forecast']['forecastday'][0]['day']['totalsnow_cm']
      output['features']['visibility'] = data['forecast']['forecastday'][0]['day']['avgvis_km']
      output['features']['moon'] = data['forecast']['forecastday'][0]['astro']['moon_illumination']
      sunrise_str = data['forecast']['forecastday'][0]['astro']['sunrise']
      sunset_str = data['forecast']['forecastday'][0]['astro']['sunset']
      sunrise = datetime.strptime(sunrise_str, "%I:%M %p")
      sunset = datetime.strptime(sunset_str, "%I:%M %p")
      sunrise = sunrise.hour + sunrise.minute / 60
      sunset = sunset.hour + sunset.minute / 60
      output['features']['daylength'] = sunset - sunrise

      producer.send(topic, value=output)
      print(output)
