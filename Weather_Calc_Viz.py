import sqlite3
import csv
import matplotlib.pyplot as plt

def calculations(cur):
    """
    Calculate the average temperature for each F1 track using data from the SQLite database.

    This function calculates the average temperature for each track by querying the F1_Track_Names and
    F1_Weather tables in the database. It first retrieves a list of track IDs and then, for each track ID,
    fetches the track name and temperature values from the database. The average temperature is then
    computed for each track, and the results are stored in a dictionary where the keys are track names
    and the values are the corresponding average temperatures.

    Args:
    cur (sqlite3.Cursor): An SQLite cursor object for executing SQL commands.

    Returns:
    dict: A dictionary containing the average temperature for each track, where the keys are track
    names and the values are the corresponding average temperatures.
    """
    avg_temp_dic = {}
    cur.execute("SELECT id FROM F1_Track_Names")
    rows = cur.fetchall()
    track_ids = [row[0] for row in rows]
    for id in track_ids:
        cur.execute("SELECT F1_Track_Names.name, F1_Weather.temp FROM F1_Weather JOIN F1_Track_Names ON F1_Weather.track_id = F1_Track_Names.id WHERE F1_Weather.track_id = ?", (id,))
        track_data = cur.fetchall()
        total_temp = 0
        for tpl in track_data:
            total_temp += tpl[1]
        if len(track_data) > 0:
            avg_temp_dic[track_data[0][0]] = total_temp/len(track_data)
    return avg_temp_dic

def write_file(data):
    """
    Write the average temperature data for each F1 track to a CSV file.

    This function takes a dictionary containing the average temperature for each F1 track and writes the
    data to a CSV file named 'avg_temp_at_track.csv'. The first row of the CSV file contains the column
    headers 'Track Name' and 'Average Temperature'. Subsequent rows contain the track name and the
    corresponding average temperature.

    Args:
    data (dict): A dictionary containing the average temperature for each track, where the keys are
    track names and the values are the corresponding average temperatures.

    Side Effects:
    Creates a new CSV file named 'avg_temp_at_track.csv' with the average temperature data for each
    F1 track.
    """
    with open('avg_temp_at_track.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Track Name', 'Average Temperature'])
        for track_name, avg_temp in data.items():
            writer.writerow([track_name, avg_temp])

def make_visualization(data):
    """
    Create a horizontal bar chart to visualize the average temperature data for each F1 track.

    This function takes a dictionary containing the average temperature for each F1 track and creates a
    horizontal bar chart using Matplotlib to visualize the data. The chart has a dim grey background, and
    the bars are colored forest green. The chart's title, axis labels, and tick labels are formatted using
    the Georgia font.

    Args:
    data (dict): A dictionary containing the average temperature for each track, where the keys are
    track names and the values are the corresponding average temperatures.

    Side Effects:
    Displays the created horizontal bar chart of the average temperature data for each F1 track.
    """
    track_names = list(data.keys())
    avg_temps = list(data.values())

    fig, ax = plt.subplots()
    fig.set_facecolor('dimgrey')

    ax.barh(track_names, avg_temps, color = 'forestgreen')
    ax.set_title("Average Temperature at Each Track", fontname = "Georgia", size = 16, fontweight = "bold") 
    ax.set_xlabel('Temperature (Farenheit)', fontname = "Georgia", size = 12)
    ax.set_ylabel('Track Name', fontname = "Georgia", size = 8)
    ax.set_yticklabels(track_names, fontname="Georgia", fontsize=5)

    plt.show()

def main():
    conn = sqlite3.connect('F1_Data.db')
    cur = conn.cursor()
    data = calculations(cur)
    write_file(data)
    make_visualization(data)

if __name__ == '__main__':
    main()
