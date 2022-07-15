from scripts import api_to_raw,harmonized_to_staged,raw_to_harmonized,visualizations
import os, json, sys
from shutil import rmtree
import subprocess

class Etl:

    def __init__(self):
        self.SILENT = False
        self.ROOT = os.path.dirname(os.path.realpath(__file__))
        self.RAW_DIR = os.path.join(self.ROOT, 'data/raw')
        self.HAR_DIR = os.path.join(self.ROOT, 'data/harmonized')
        self.VIS_DIR = os.path.join(self.ROOT, 'visualizations')
        self.CONCAT_DATA_DIR = os.path.join(self.ROOT, 'data/concat')
        self.CONCAT_DATA_FILE = os.path.join(self.CONCAT_DATA_DIR, 'all_cities.json')
        self.SWE_CITIES_FILE = os.path.join(self.ROOT, 'res/se.json')
        self.ALL_DIRS = [self.RAW_DIR, self.HAR_DIR, self.VIS_DIR, self.CONCAT_DATA_DIR]

        self.COORDINATES = {
            # 'Stockholm' : (59.32, 18.06),
            # 'Göteborg'  : (57.70, 11.97),
            'Malmö'     : (55.60, 13.00)
        }

    def use_all_coordinates(self):
        if not self.SILENT : print(f'Adding cities')
        self.COORDINATES
        with open(self.SWE_CITIES_FILE, 'r') as f:
            dict = json.load(f)
        self.COORDINATES = { i['city'] : (i['lat'], i['lng']) for i in dict }


    def get_url(self,x,y):
        return f'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/{y}/lat/{x}/data.json'

    def extract(self):
        for city, coords in self.COORDINATES.items():
            if not self.SILENT : print(f'extracting data from {city}')
            url = self.get_url(*coords)
            pathout = os.path.join(self.RAW_DIR, f'{city}.json')
            if not api_to_raw.get_data(url, pathout):
                raise Exception('Failed to extract data.', api_to_raw.error_msg)

    def transform(self):
        for file in os.listdir(self.RAW_DIR):
            if not self.SILENT : print(f'transforming {file}')
            pathin = os.path.join(self.RAW_DIR, file)
            pathout = os.path.join(self.HAR_DIR, file)

            if not raw_to_harmonized.harmonized_data(pathin, pathout):
                raise Exception('Failed to harmonize data.', raw_to_harmonized.error_msg)


        files = [os.path.join(self.HAR_DIR, file) for file in os.listdir(self.HAR_DIR)]
        raw_to_harmonized.concat_data(files, self.CONCAT_DATA_FILE)

    def visualize(self):
        for file in os.listdir(self.HAR_DIR):
            if not self.SILENT : print(f'visualizing {file}')
            pathin = os.path.join(self.HAR_DIR, file)

            city = file.split('.')[0]
            outdir = os.path.join(self.VIS_DIR, city)
            os.mkdir(outdir)

            for parameter in ('temperature', 'mean_precipitation', 'air_pressure'):
                pathout = os.path.join(outdir, f'{parameter}.png')
                visualizations.forecast_vis_line(pathin, pathout, city, parameter)

            visualizations.temperature_plot(pathin, os.path.join(outdir, 'temp_plot.png'))
            visualizations.precipitation_pressure_plot(pathin, os.path.join(outdir, 'prec_pres_plot.png'))

        if not self.SILENT : print('visualizing all')
        outdir = os.path.join(self.VIS_DIR, 'all')
        os.mkdir(outdir)
        visualizations.precipitation_pressure_plot(self.CONCAT_DATA_FILE, os.path.join(outdir, 'prec_pres_plot.png'))

    def load(self):
        port = self.setup_psql()
        for file in os.listdir(self.HAR_DIR):
            if not self.SILENT : print(f'loading {file}')
            pathin = os.path.join(self.HAR_DIR, file)
            city = file.split(".")[0]
            if not harmonized_to_staged.load_db(pathin, city, port):
                raise Exception('Cant load db')

    def clean(self):
        if not self.SILENT : print(f'cleaning {self.ALL_DIRS}')
        for dir in self.ALL_DIRS:
            if os.path.isdir(dir): rmtree(dir)

    def setup(self):
        if not self.SILENT : print(f'setting up {self.ALL_DIRS}')
        for dir in self.ALL_DIRS:
            os.makedirs(dir, exist_ok=True)

    def setup_psql(self):
        res = subprocess.run(r'psql -U postgres -d weather_db -c "\conninfo"', shell=True, capture_output=True).stdout
        if res == b'':
            print('Starting psql...')
            subprocess.run('sudo service postgresql start', shell=True)
            return self.setup_psql() # tur vi har ctrl+c
        return str(res).split(' ')[-1][1:-5]

    
# RUN = {
#         'c' : clean,
#         's' : setup,
#         'e' : extract,
#         't' : transform,
#         'v' : visualize,
#         'l' : load}
if __name__=='__main__':
    # SILENT = True # False for status messages
    #use_all_coordinates() # uncomment to make api call for 363 cities
    # schedule = sys.argv[1] if len(sys.argv) > 1 else 'csetvl'
    # [RUN[task]() for task in schedule.lower()]
    etl = Etl()
    # etl.use_all_coordinates()
    etl.clean()
    etl.setup()
    etl.extract()
    etl.transform()
    etl.visualize()
    etl.load()