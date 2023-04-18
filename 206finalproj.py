import requests
import sqlite3
import json
import os
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET

def make_f1_DB(name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+name)
    cur = conn.cursor()
    return cur, conn

def get_f1_tracks(cur,conn):
    year_counter = 2010
    cur.execute("CREATE TABLE IF NOT EXISTS F1_Track_Names (id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, lat FLOAT, long FLOAT)")
    for year in range(2010,2020):
        url = 'http://ergast.com/api/f1/{}/circuits.json'
        requests_url = url.format(year_counter)
        r = requests.get(requests_url)
        loaded_data = json.loads(r.text)
        circuits = loaded_data["MRData"]["CircuitTable"]["Circuits"]
        for index in range(len(circuits)):
            circuit_name = loaded_data["MRData"]["CircuitTable"]["Circuits"][index]["circuitName"]
            circuit_lat = float(loaded_data["MRData"]["CircuitTable"]["Circuits"][index]["Location"]["lat"])
            circuit_long = float(loaded_data["MRData"]["CircuitTable"]["Circuits"][index]["Location"]["lat"])
            cur.execute("INSERT OR IGNORE INTO F1_Track_Names (name,lat,long) VALUES (?,?,?)", (circuit_name,circuit_lat,circuit_long,))
        year_counter += 1
    conn.commit()


def get_f1_data(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS F1_Times (id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT, track_id INTEGER, fastest_time INTEGER UNIQUE)")
    year_counter = 2010
    for year in range(2010,2020):
        url = 'https://ergast.com/api/f1/{}/results/1.json'
        requests_url = url.format(year_counter,)
        r = requests.get(requests_url)
        loaded_data = json.loads(r.text)
        races = loaded_data['MRData']['RaceTable']['Races']
        for index in range(len(races)):
            time = loaded_data['MRData']['RaceTable']['Races'][index]['Results'][0]['Time']['millis']
            track = loaded_data['MRData']['RaceTable']['Races'][index]['Circuit']['circuitName']
            cur.execute("INSERT OR IGNORE INTO F1_Times (track_id, fastest_time) SELECT F1_Track_Names.id, ? FROM F1_Track_Names WHERE F1_Track_Names.name = ?", (time, track,))
        year_counter += 1
    conn.commit()

def main():
    cur, conn = make_f1_DB("F1DATABASE.db")
    get_f1_tracks(cur, conn)
    get_f1_data(cur, conn)

main()

