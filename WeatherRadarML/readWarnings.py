from fiona.io import ZipMemoryFile
from zipfile import ZipFile
from WeatherRadarML.wwaVTEC import wwaVTEC

def read_warnings(zipfile):
    with open(zipfile, 'rb') as fid:
        file = ZipFile(fid).namelist()
        fid.seek(0)
        data = fid.read()
        
    for item in file:
        if item.endswith('.shp'):
            shapefile = item
    
    with ZipMemoryFile(data) as zip:
        with zip.open(shapefile) as collection:
            records = []
            for record in collection:
                records.append(wwaVTEC(record))
    
    return records
    
if __name__ == "__main__":
    print(read_warnings('/home/allen/Downloads/1986_all.zip'))