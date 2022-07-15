from scripts import api_to_raw,harmonized_to_staged,raw_to_harmonized,visualizations
import os, json
from shutil import rmtree
import subprocess

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
        self.silent = False
        self.COORDINATES = {
            'Stockholm' : (59.32, 18.06),
            'Göteborg'  : (57.70, 11.97),
            'Malmö'     : (55.60, 13.00)
        }

    def use_all_coordinates(self):
        if not self.silent : print(f'Adding cities')
        self.COORDINATES
        with open(SWE_CITIES_FILE, 'r') as f:
            dict = json.load(f)
        self.COORDINATES = { i['city'] : (i['lat'], i['lng']) for i in dict }

    def get_url(self,x,y):
        return f'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/{y}/lat/{x}/data.json'

    def extract(self):
        for city, coords in self.COORDINATES.items():
            if not self.silent : print(f'extracting data from {city}')
            url = self.get_url(*coords)
            pathout = os.path.join(RAW_DIR, f'{city}.json')
            if not api_to_raw.get_data(url, pathout):
                raise Exception('Failed to extract data.', api_to_raw.error_msg)

    def transform(self):
        for file in os.listdir(RAW_DIR):
            if not self.silent : print(f'transforming {file}')
            pathin = os.path.join(RAW_DIR, file)
            pathout = os.path.join(HAR_DIR, file)

            if not raw_to_harmonized.harmonized_data(pathin, pathout):
                raise Exception('Failed to harmonize data.', raw_to_harmonized.error_msg)

        files = [os.path.join(HAR_DIR, file) for file in os.listdir(HAR_DIR)]
        raw_to_harmonized.concat_data(files, CONCAT_DATA_FILE)

    def try_plot(self, func, pathin, pathout):
        try:
            func(pathin, pathout)
        except:
            print(f'{pathout} could not be plotted.')

    def visualize(self):
        for file in os.listdir(HAR_DIR):
            if not self.silent : print(f'visualizing {file}')
            pathin = os.path.join(HAR_DIR, file)

            city = file.split('.')[0]
            outdir = os.path.join(VIS_DIR, city)
            os.mkdir(outdir)

            for parameter in ('temperature', 'mean_precipitation', 'air_pressure'):
                pathout = os.path.join(outdir, f'{parameter}.png')
                self.try_plot(visualizations.forecast_vis_line, pathin, pathout)

            self.try_plot(visualizations.temperature_plot, pathin, os.path.join(outdir, 'temp_plot.png'))
            self.try_plot(visualizations.precipitation_pressure_plot, pathin, os.path.join(outdir, 'prec_pres_plot.png'))

        if not self.silent : print('visualizing all')
        outdir = os.path.join(VIS_DIR, 'all')
        os.mkdir(outdir)
        self.try_plot(visualizations.precipitation_pressure_plot, CONCAT_DATA_FILE, os.path.join(outdir, 'prec_pres_plot.png'))

        for parameter in ('temperature', 'mean_precipitation', 'air_pressure'):
            pathout = os.path.join(outdir, f'{parameter}.png')
            self.try_plot(visualizations.forecast_vis_line, CONCAT_DATA_FILE, pathout)
        

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
    etl.silent = False
    etl.use_all_coordinates()
    etl.clean()
    etl.setup()
    etl.extract()
    etl.transform()
    etl.visualize()
    etl.load()