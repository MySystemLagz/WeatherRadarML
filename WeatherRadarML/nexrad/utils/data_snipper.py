from WeatherRadarML.readWarnings import read_warnings
from datetime import datetime
import pickle

def snip(file, start, end, output):
    records = read_warnings(file, start, end)

    with open(output, 'wb') as f:
        records = [r._record for r in records]
        pickle.dump(records, f)

if __name__ == "__main__":
    snip('/home/allen/Downloads/2017_all.zip', start=datetime(2017, 8, 28, 0), end=datetime(2017, 8, 28, 3), output='WeatherRadarML/data/pickle1.pic')