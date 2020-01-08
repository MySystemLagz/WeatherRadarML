from fiona.io import ZipMemoryFile
from zipfile import ZipFile
from WeatherRadarML.wwaVTEC import wwaVTEC
from datetime import datetime
import pickle

def read_warnings(zipfile, start_date = None, end_date = None):
    if zipfile.endswith('.pic'):
        with open(zipfile, 'rb') as f:
            records = pickle.load(f)
        records = [wwaVTEC(r) for r in records]
        return records

    with open(zipfile, 'rb') as fid:
        file = ZipFile(fid).namelist()
        fid.seek(0)
        data = fid.read()
    
    # 
    for item in file:
        if item.endswith('.shp'):
            shapefile = item
    
    # 
    with ZipMemoryFile(data) as zip:
        with zip.open(shapefile) as collection:
            records = []
            for record in collection:
                try:
                    wwa = wwaVTEC(record)
                except:
                    continue
                if start_date is None and end_date is None:
                    records.append(wwa)
                elif start_date and end_date is None:
                    if wwa.EXPIRED >= start_date:
                        records.append(wwa)
                elif start_date is None and end_date:
                    if wwa.ISSUED <= end_date:
                        records.append(wwa)
                else:
                    if (wwa.EXPIRED >= start_date and wwa.ISSUED <= end_date) or (wwa.ISSUED <= end_date and wwa.EXPIRED >= start_date):
                        records.append(wwa)
    
    return records
    
if __name__ == "__main__":
    # print('Clamped: ' + str(len(read_warnings('/home/allen/Downloads/1986_all.zip', start_date=datetime(1986, 1, 1), end_date=datetime(1986, 3, 1)))))
    # print('Full: ' + str(len(read_warnings('/home/allen/Downloads/1986_all.zip'))))
    # print('Clamped: ' + str(len(read_warnings('/home/allen/Downloads/2017_all.zip', start_date=datetime(2017, 8, 25), end_date=datetime(2017, 8, 29)))))
    read_warnings('WeatherRadarML/data/pickle.pic')