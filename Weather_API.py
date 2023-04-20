import requests
import sqlite3
import os
from datetime import datetime
import apikey

def get_weather_data(lat, lon, dt):
    """
    Get historical weather data for a specific latitude, longitude, and date.

    This function retrieves historical weather data from the OpenWeatherMap API
    using the provided latitude, longitude, and Unix timestamp (date). The API key
    from the 'apikey' module is used to authenticate the API request.

    Args:
    lat (float): The latitude coordinate of the location for which to fetch the weather data.
    lon (float): The longitude coordinate of the location for which to fetch the weather data.
    dt (int): The Unix timestamp (date) for which to fetch the weather data.

    Returns:
    dict: The weather data in JSON format, as returned by the OpenWeatherMap API.
    """
    url = f'https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={dt}&appid={apikey.API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data

def store_weather_data(cur, conn, race_id, track_id, lat, lon, race_date):
    """
    Store weather data for a specific track and race date in the F1_Weather table.

    This function first creates the F1_Weather table if it doesn't already exist. Then, it converts the given
    race date to a Unix timestamp and retrieves the weather data using the get_weather_data function.
    If valid temperature, humidity, and wind speed values are available, the function inserts the data into
    the F1_Weather table. If any of the values are missing, the function prints an appropriate message.

    Args:
    cur (sqlite3.Cursor): An SQLite cursor object for executing SQL commands.
    conn (sqlite3.Connection): An SQLite connection object for committing the transaction.
    track_id (int): The unique identifier of the F1 track.
    lat (float): The latitude coordinate of the location for which to fetch the weather data.
    lon (float): The longitude coordinate of the location for which to fetch the weather data.
    race_date (str): The race date in the format 'YYYY-MM-DD'.

    Side Effects:
    Inserts the weather data into the F1_Weather table if valid data is available. Prints messages
    regarding the insertion status and any missing data.
    """
    cur.execute("CREATE TABLE IF NOT EXISTS F1_Weather (id INTEGER PRIMARY KEY, track_id INTEGER, race_date TEXT, temp FLOAT, humidity INTEGER, wind_speed FLOAT, FOREIGN KEY(track_id) REFERENCES F1_Track_Names(id))")
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
            cur.execute("INSERT OR IGNORE INTO F1_Weather (id, track_id, race_date, temp, humidity, wind_speed) VALUES (?, ?, ?, ?, ?, ?)", (race_id, track_id, race_date, temp, humidity, wind_speed))
            conn.commit()
        else:
            print(f"Data not inserted for track_id {track_id}, race_date {race_date}. Temp: {temp}, Humidity: {humidity}, Wind speed: {wind_speed}")
    else:
        print(f"No data available for track_id {track_id}, race_date {race_date}")

def get_last_processed_race_index(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            index = f.read().strip()
            if index:
                return int(index)
    return 0

def update_last_processed_race_index(file_path, index):
    with open(file_path, "w") as f:
        f.write(str(index))

def collect_weather_data(cur, conn):
    """
    Collect and store weather data for each F1 track on their corresponding race dates.

    This function retrieves the track information (ID, latitude, and longitude) from the F1_Track_Names table
    and the race dates from the F1_Times table. It then calls the store_weather_data function for each track
    and its corresponding race date to fetch and store the historical weather data in the F1_Weather table.

    Args:
    cur (sqlite3.Cursor): An SQLite cursor object for executing SQL commands.
    conn (sqlite3.Connection): An SQLite connection object for committing the transaction.

    Side Effects:
    Calls the store_weather_data function for each track and corresponding race date, which in turn
    inserts the weather data into the F1_Weather table if valid data is available.
    """
    cur.execute("SELECT id, lat, long FROM F1_Track_Names")
    tracks = {track_id: (lat, lon) for track_id, lat, lon in cur.fetchall()}

    cur.execute("SELECT id, track_id, date FROM F1_Times")
    race_dates = cur.fetchall()

    index_file = "last_processed_race_index.txt"
    last_processed_index = get_last_processed_race_index(index_file)
    races_to_process = 25
    end_index = min(last_processed_index + races_to_process, len(race_dates))

    for i in range(last_processed_index, end_index):
        race_id, track_id, race_date = race_dates[i]
        lat, lon = tracks[track_id]
        store_weather_data(cur, conn, race_id, track_id, lat, lon, race_date)

    update_last_processed_race_index(index_file, end_index)

def main():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/F1_Data.db')
    cur = conn.cursor()

    collect_weather_data(cur, conn)
    conn.close()
    
if __name__ == '__main__':
    main()