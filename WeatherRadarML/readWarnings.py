from fiona.io import ZipMemoryFile
from zipfile import ZipFile
from WeatherRadarML.wwaVTEC import wwaVTEC
from datetime import datetime

def read_warnings(zipfile, start_date = None, end_date = None):
    # 
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
                wwa = wwaVTEC(record)
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
    print('Clamped: ' + str(len(read_warnings('/home/allen/Downloads/1986_all.zip', start_date=datetime(1986, 1, 1), end_date=datetime(1986, 3, 1)))))
    print('Full: ' + str(len(read_warnings('/home/allen/Downloads/1986_all.zip'))))