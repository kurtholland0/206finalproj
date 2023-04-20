import requests
import sqlite3
import json
import os
import random

def make_f1_DB(name):
    '''Accepts a name(string) and creates a database with that name. Connects to a database passed in by parameter
    and returns cursor and connection to be used in main'''


    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+name)
    cur = conn.cursor()
    return cur, conn


def get_f1_tracks(cur,conn, year):
    """Retrieves information about F1 circuits in a given year from the ergast API, 
    extracts the lat, long, and name of the circuit, and stores it in a SQLite table.

    Args:
    cur (cursor object): Cursor object of a SQLite connection
    conn (connection object): Connection object to a SQLite database
    year (int): Year for which the F1 circuits information is to be retrieved
    
    Returns:
    None"""


    cur.execute("CREATE TABLE IF NOT EXISTS F1_Track_Names (id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, lat FLOAT, long FLOAT)")
    url = 'http://ergast.com/api/f1/{}/circuits.json'
    requests_url = url.format(year)
    r = requests.get(requests_url)
    loaded_data = json.loads(r.text)
    circuits = loaded_data["MRData"]["CircuitTable"]["Circuits"]
    for index in range(len(circuits)):
        circuit_name = loaded_data["MRData"]["CircuitTable"]["Circuits"][index]["circuitName"]
        circuit_lat = float(loaded_data["MRData"]["CircuitTable"]["Circuits"][index]["Location"]["lat"])
        circuit_long = float(loaded_data["MRData"]["CircuitTable"]["Circuits"][index]["Location"]["lat"])
        cur.execute("INSERT OR IGNORE INTO F1_Track_Names (name,lat,long) VALUES (?,?,?)", (circuit_name,circuit_lat,circuit_long,))
    conn.commit()


def get_f1_data(cur, conn, year):
    """Retrieves F1 race information, including the fastest lap times for each race, 
    for a given year from the ergast API and stores it in a SQLite table with the track name 
    as the track id from the table created by get_f1_tracks.
    
    Args:
    cur (cursor object): Cursor object of a SQLite connection
    conn (connection object): Connection object to a SQLite database
    year (int): Year for which the F1 race information is to be retrieved
    
    Returns:
    None"""


    cur.execute("CREATE TABLE IF NOT EXISTS F1_Times (id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT, track_id INTEGER, fastest_time INTEGER UNIQUE, date TEXT)")
    url = 'https://ergast.com/api/f1/{}/results/1.json'
    requests_url = url.format(year,)
    r = requests.get(requests_url)
    loaded_data = json.loads(r.text)
    races = loaded_data['MRData']['RaceTable']['Races']
    for index in range(len(races)):
        time = loaded_data['MRData']['RaceTable']['Races'][index]['Results'][0]['Time']['millis']
        track = loaded_data['MRData']['RaceTable']['Races'][index]['Circuit']['circuitName']
        date = loaded_data["MRData"]["RaceTable"]["Races"][index]["date"]
        cur.execute("INSERT OR IGNORE INTO F1_Times (track_id, fastest_time, date) VALUES ((SELECT id FROM F1_Track_Names WHERE name = ?), ?, ?)", (track, time, date))
    conn.commit()


def main():
    year = random.randint(1980, 2021)
    cur, conn = make_f1_DB("F1_Data.db")
    get_f1_tracks(cur, conn, year)
    get_f1_data(cur, conn, year)

main()

