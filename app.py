from scripts import api_to_raw,harmonized_to_staged,raw_to_harmonized,visualizations
import os
from shutil import rmtree

ROOT = os.path.dirname(os.path.realpath(__file__))
RAW_DIR = os.path.join(ROOT, 'data/raw')
HAR_DIR = os.path.join(ROOT, 'data/harmonized')
VIS_DIR = os.path.join(ROOT, 'visualizations')
CONCAT_DATA_DIR = os.path.join(ROOT, 'data/concat')
CONCAT_DATA_FILE = os.path.join(CONCAT_DATA_DIR, 'all_cities.json')
ALL_DIRS = [RAW_DIR, HAR_DIR, VIS_DIR, CONCAT_DATA_DIR]

COORDINATES = {
    'Stockholm' : (59.32, 18.06),
    'Göteborg'  : (57.70, 11.97),
    'Malmö'     : (55.60, 13.00)
}

def get_url(x,y):
    return f'https://opendata-download-metfcst.smhi.se/api\
/category/pmp3g/version/2/geotype/point\
/lon/{y}/lat/{x}/data.json'

def extract():
    for city, coords in COORDINATES.items():
        url = get_url(*coords)
        pathout = os.path.join(RAW_DIR, f'{city}.json')
        if not api_to_raw.get_data(url, pathout):
            raise Exception('Failed to extract data.', api_to_raw.error_msg)

def transform():
    for file in os.listdir(RAW_DIR):
        pathin = os.path.join(RAW_DIR, file)
        pathout = os.path.join(HAR_DIR, file)

        if not raw_to_harmonized.harmonized_data(pathin, pathout):
            raise Exception('Failed to harmonize data.', raw_to_harmonized.error_msg)
    

    files = [os.path.join(HAR_DIR, file) for file in os.listdir(HAR_DIR)]
    raw_to_harmonized.concat_data(files, CONCAT_DATA_FILE)

def visualize():
    for file in os.listdir(HAR_DIR):
        pathin = os.path.join(HAR_DIR, file)

        city = file.split('.')[0]
        outdir = os.path.join(VIS_DIR, city)
        os.mkdir(outdir)

        for parameter in ('temperature', 'mean_precipitation', 'air_pressure'):
            pathout = os.path.join(outdir, f'{parameter}.png')
            visualizations.forecast_vis_line(pathin, pathout, city, parameter)
        
        visualizations.temperature_plot(pathin, os.path.join(outdir, 'temp_plot.png'))
        visualizations.precipitation_pressure_plot(pathin, os.path.join(outdir, 'prec_pres_plot.png'))
    
    outdir = os.path.join(VIS_DIR, 'all')
    os.mkdir(outdir)
    visualizations.precipitation_pressure_plot(CONCAT_DATA_FILE, os.path.join(outdir, 'prec_pres_plot.png'))
    
def load():
    for file in os.listdir(HAR_DIR):
        pathin = os.path.join(HAR_DIR, file)
        city = file.split(".")[0]
        if not harmonized_to_staged.load_db(pathin, city):
            raise Exception('Cant load db')

def clean():
    for dir in ALL_DIRS:
        if os.path.isdir(dir): rmtree(dir)

def setup():
    for dir in ALL_DIRS:
        os.makedirs(dir, exist_ok=True)

if __name__=='__main__':
    clean()
    setup()

    extract()
    transform()
    visualize()
    # load()