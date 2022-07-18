from scripts import api_to_raw,harmonized_to_staged,raw_to_harmonized,visualizations
import os, json
from shutil import rmtree
import subprocess
from multiprocessing.dummy import Pool as ThreadPool 

ROOT = os.path.dirname(os.path.realpath(__file__))
RAW_DIR = os.path.join(ROOT, 'data/raw')
HAR_DIR = os.path.join(ROOT, 'data/harmonized')
VIS_DIR = os.path.join(ROOT, 'visualizations')
CONCAT_DATA_DIR = os.path.join(ROOT, 'data/concat')
CONCAT_DATA_FILE = os.path.join(CONCAT_DATA_DIR, 'all_cities.json')
SWE_CITIES_FILE = os.path.join(ROOT, 'res/se.json')
ALL_DIRS = [RAW_DIR, HAR_DIR, VIS_DIR, CONCAT_DATA_DIR]

class Etl:

    def __init__(self):
        self.silent = True
        
    def get_all_coordinates(self):
        with open(SWE_CITIES_FILE, 'r') as f:
            dict = json.load(f)
        return { i['city'] : (i['lat'], i['lng']) for i in dict }

    def get_default_coordinates(self):
        return {
            'Stockholm' : (59.32, 18.06),
            'Göteborg'  : (57.70, 11.97),
            'Malmö'     : (55.60, 13.00)
        }

    def get_url(self,x,y):
        return f'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/{y}/lat/{x}/data.json'

    def start_data_thread(self, data):
        url, pathout = data[0], data[1]
        if not self.silent : print(f'extracting data from {pathout.split("/")[-1][:-5]}')
        api_to_raw.get_data(url, pathout)

    def extract(self, all=True):
        coordinates = self.get_all_coordinates() if all else self.get_default_coordinates()
        request_data = [(self.get_url(*coords), os.path.join(RAW_DIR, f'{city}.json')) for city, coords in coordinates.items()]
        pool = ThreadPool(150)
        pool.map(self.start_data_thread, request_data)
        pool.close()
        pool.join() 

    def transform(self):
        for file in os.listdir(RAW_DIR):
            if not self.silent : print(f'transforming {file}')
            pathin = os.path.join(RAW_DIR, file)
            pathout = os.path.join(HAR_DIR, file)

            if not raw_to_harmonized.harmonized_data(pathin, pathout):
                raise Exception('Failed to harmonize data.', raw_to_harmonized.error_msg)

        files = [os.path.join(HAR_DIR, file) for file in os.listdir(HAR_DIR)]
        raw_to_harmonized.concat_data(files, CONCAT_DATA_FILE)

    def visualize(self):
        if not self.silent : print(f'Visualizing')
        to_visualize = [(os.path.join(HAR_DIR, file), os.path.join(VIS_DIR, file.split('.')[0])) for file in os.listdir(HAR_DIR)]
        to_visualize.append((CONCAT_DATA_FILE, os.path.join(VIS_DIR, 'all')))
        [os.mkdir(path[1]) for path in to_visualize]
        visualizations.forecast_vis_line(to_visualize)

    def load(self):
        port = self.setup_psql()
        for file in os.listdir(HAR_DIR):
            if not self.silent : print(f'loading {file}')
            pathin = os.path.join(HAR_DIR, file)
            city = file.split(".")[0]
            if not harmonized_to_staged.load_db(pathin, city, port):
                raise Exception('Cant load db')

    def clean(self):
        if not self.silent : print(f'cleaning {ALL_DIRS}')
        for dir in ALL_DIRS:
            if os.path.isdir(dir): rmtree(dir)

    def setup(self):
        if not self.silent : print(f'setting up {ALL_DIRS}')
        for dir in ALL_DIRS:
            os.makedirs(dir, exist_ok=True)

    def setup_psql(self):
        res = subprocess.run(r'psql -U postgres -d weather_db -c "\conninfo"', shell=True, capture_output=True).stdout
        if res == b'':
            print('Starting psql...')
            subprocess.run('sudo service postgresql start', shell=True)
            return self.setup_psql() # tur vi har ctrl+c
        return str(res).split(' ')[-1][1:-5]

if __name__=='__main__':
    etl = Etl()
    etl.silent = True
    etl.clean()
    etl.setup()
    etl.extract(all=True)
    etl.transform()
    etl.visualize()
    etl.load()