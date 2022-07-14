import pandas as pd
import matplotlib.pyplot as plt

city = 'data'
#for future iteration 
#for city in geo_locations.keys()

def group_data_mean(city):
    df = pd.read_json(f'data/harmonized/{city}.json')
    df_mean = df.groupby(pd.Grouper(key='date', axis=0, freq='D', sort=True)).mean()
    return df_mean

def group_data_max(city):
    df = pd.read_json(f'data/harmonized/{city}.json') 
    df_max = df.groupby(pd.Grouper(key='date', axis=0, freq='D', sort=True)).max()
    return df_max

def group_data_min(city):
    df = pd.read_json(f'data/harmonized/{city}.json')
    df_min = df.groupby(pd.Grouper(key='date', axis=0, freq='D', sort=True)).min()
    return df_min

def forecast_vis_line(city, parameter):
    mean = group_data_mean(city)[parameter]
    max = group_data_max(city)[parameter]
    min = group_data_min(city)[parameter]
    plt.plot(mean.index, mean, marker = 'o')
    plt.plot(max.index, max, marker = 'o')
    plt.plot(max.index, min, marker = 'o')
    plt.title(f"Daily {parameter} forecast for {city} - mean, max and min")
    plt.savefig(f'visualizations/{city}_{parameter}.png')

