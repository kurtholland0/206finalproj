import sqlite3
import csv
import matplotlib.pyplot as plt

def calculations(cur):
    avg_time_dic = {}
    cur.execute("SELECT id FROM F1_Track_Names")
    rows = cur.fetchall()
    track_ids = [row[0] for row in rows]
    for id in track_ids:
        cur.execute("SELECT F1_Track_Names.name, F1_Times.fastest_time FROM F1_Times JOIN F1_Track_Names ON F1_Times.track_id = F1_Track_Names.id WHERE F1_Times.track_id = ?", (id,))
        track_data = cur.fetchall()
        total_time = 0
        for tpl in track_data:
            total_time += tpl[1]
        avg_time_dic[track_data[0][0]] = total_time/len(track_data)
    return avg_time_dic


def write_file(data):
    with open('avg_fastest_time_at_track.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Track Name', 'Average Fastest Time'])
        for track_name, fastest_time in data.items():
            writer.writerow([track_name, fastest_time])


def make_visualization(data):
    track_names = list(data.keys())
    fastest_times = list(data.values())

    fig, ax = plt.subplots()
    fig.set_facecolor('dimgrey')

    ax.barh(track_names, fastest_times, color = 'forestgreen')
    ax.set_title("Average Fastest Time at Each Track", fontname = "Georgia", size = 16, fontweight = "bold") 
    ax.set_xlabel('Fastest Time (in millions of milliseconds)', fontname = "Georgia", size = 12)
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