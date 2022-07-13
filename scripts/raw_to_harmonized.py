import json
import pandas as pd


# 2. skapa funktion som omvandlar raw data till harmoniserad data i raw_to_harmonized.py
# 	ARGS: path.in, path.out
# 	OUTPUT: ladda json data i path.out
# 	RETURN Boolean

 
with open('data/raw/data.json', 'r') as f:
    json_data = json.load(f)
    time_series = json_data['timeSeries'][0]
    normalized_data = pd.json_normalize (
    time_series,
    record_path=['parameters'],
    meta=['validTime'],
    meta_prefix = 'config_params_',
    record_prefix= 'parameter_'
    )
    df = pd.DataFrame(normalized_data)

print (df)