from itertools import groupby
import json
from logging.config import valid_ident
from numpy import float32
import pandas as pd


# 2. skapa funktion som omvandlar raw data till harmoniserad data i raw_to_harmonized.py
# 	ARGS: path.in, path.out
# 	OUTPUT: ladda json data i path.out
# 	RETURN Boolean


with open('data/raw/data.json', 'r') as f:
    json_data = json.load(f)
time_series = json_data['timeSeries']
normalized_data = pd.json_normalize (
time_series,
record_path=['parameters'],
meta=['validTime'])

parameters = {
        't' : 'temperature', 
        'pmean' : 'mean_precipitation',
        'msl' : 'air_pressure',
        'Wsymb2' : 'weather_symbol'}

df = pd.DataFrame(normalized_data)
times = df['validTime']
df = df[df['name'].isin(parameters.keys())]
df = df.pivot(values='values', columns='name')
df['date'] = times

agg_func = {p:'first' for p in parameters}
df = df.groupby('date', as_index=False).aggregate(agg_func)
df.rename(columns = parameters, inplace = True)
df = df.rename_axis(None, axis=1)

for par in parameters.values():
    df[par] = df[par].map(lambda x : x[0])

print(df)
