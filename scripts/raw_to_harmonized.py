import json, os
import pandas as pd


# 2. skapa funktion som omvandlar raw data till harmoniserad data i raw_to_harmonized.py
# 	ARGS: path.in, path.out
# 	OUTPUT: ladda json data i path.out
# 	RETURN Boolean

error_msg = ''

def harmonized_data(pathin, pathout):
    global error_msg
    #try to open json data
    try:
        with open(pathin, 'r') as f:
            json_data = json.load(f)
    except:
        error_msg = f'failed to open files from {pathin}'
    
    #select data from file
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

    #aggregate by date
    agg_func = {p:'first' for p in parameters}
    df = df.groupby('date', as_index=False).aggregate(agg_func)
    df.rename(columns = parameters, inplace = True)
    df = df.rename_axis(None, axis=1)
    
    #transform object values into data types
    for par in parameters.values():
        df[par] = df[par].map(lambda x : x[0])

    #write json to file
    if not os.path.isdir('/'.join(pathout.split('/')[:-1])):
        error_msg = f'invalid path: {pathout}'
        return False
    df.to_json(pathout, indent=4)

    #everything worked
    error_msg = None
    return True


