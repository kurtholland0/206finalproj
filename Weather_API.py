import requests
import sqlite3
import os
from datetime import datetime

API_KEY = '9c5e56635b172ff8a5e4881052968d14'

def get_weather_data(lat, lon, dt):
    url = f'https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={dt}&appid={API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data

def store_weather_data(cur, conn, track_id, lat, lon, race_date):
    cur.execute("CREATE TABLE IF NOT EXISTS F1_Weather (track_id INTEGER UNIQUE PRIMARY KEY, race_date TEXT, temp FLOAT, humidity INTEGER, wind_speed FLOAT, FOREIGN KEY(track_id) REFERENCES F1_Track_Names(id))")
    
    unix_timestamp = int(datetime.strptime(race_date, '%Y-%m-%d').strftime('%s'))
    weather_data = get_weather_data(lat, lon, unix_timestamp)
    print(f"Weather data for track_id {track_id}, race_date {race_date}: {weather_data}")

    data_list = weather_data.get('data', [])
    if data_list:
        temp = data_list[0].get('temp')
        if temp is not None:
            temp = round((temp - 273.15) * 9/5 + 32)
        humidity = data_list[0].get('humidity')
        wind_speed = data_list[0].get('wind_speed')

        if temp is not None and humidity is not None and wind_speed is not None:
            print(f"Inserting data for track_id {track_id}, race_date {race_date}, temp {temp}, humidity {humidity}, wind_speed {wind_speed}")
            cur.execute("INSERT OR IGNORE INTO F1_Weather (track_id, race_date, temp, humidity, wind_speed) VALUES (?, ?, ?, ?, ?)", (track_id, race_date, temp, humidity, wind_speed))
            conn.commit()
        else:
            print(f"Data not inserted for track_id {track_id}, race_date {race_date}. Temp: {temp}, Humidity: {humidity}, Wind speed: {wind_speed}")
    else:
        print(f"No data available for track_id {track_id}, race_date {race_date}")

def collect_weather_data(cur, conn):
    cur.execute("SELECT id, lat, long FROM F1_Track_Names")
    tracks = cur.fetchall()

    cur.execute("SELECT track_id, date FROM F1_Times")
    race_dates = cur.fetchall()
    track_race_dates = {track_id: race_date for track_id, race_date in race_dates}

    for track_id, lat, lon in tracks:
        if track_id in track_race_dates:
            race_date = track_race_dates[track_id]
            store_weather_data(cur, conn, track_id, lat, lon, race_date)

def main():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/F1_Data.db')
    cur = conn.cursor()

    collect_weather_data(cur, conn)
    conn.close()
    
if __name__ == '__main__':
    main()