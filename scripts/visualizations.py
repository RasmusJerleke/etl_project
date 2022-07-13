from datetime import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt

ROOT = os.path.abspath(os.path.join(__file__ ,"../.."))
IN_FILE = os.path.join(ROOT, 'data/harmonized/data.json')
OUT_DIR = os.path.join(ROOT, 'visualizations')

def temperature_plot():
    midnight = datetime.strptime('00:00:00', '%H:%M:%S').time()
    noon = datetime.strptime('12:00:00', '%H:%M:%S').time()

    df = pd.read_json(IN_FILE)
    df_noon = df[df['date'].dt.time == noon]
    df_mid = df[df['date'].dt.time == midnight]

    temps_mid = df_mid['temperature']
    dates_mid = df_mid['date'].dt.day

    temps_noon = df_noon['temperature']
    dates_noon = df_noon['date'].dt.day


    fig, ax = plt.subplots()

    ax.plot(dates_mid, temps_mid, linewidth=2.0, color='red')
    ax.plot(dates_noon, temps_noon, linewidth=2.0, color='blue')

    t_range = range(int(df['temperature'].min()) - 1, round(df['temperature'].max() + 0.5) + 1)
    d_range = list(set(dates_mid.tolist()).union(set(dates_noon.tolist())))

    ax.set(xlim=(d_range[0], d_range[-1]), xticks=sorted(d_range),
           ylim=(t_range[0], t_range[-1]), yticks=t_range)

    ax.set_xlabel(f'Date ({df["date"].dt.date.min()} - {df["date"].dt.date.max()})')
    ax.set_ylabel('Celsius')

    ax.set_title('red = 00:00:00, blue = 12:00:00')

    plt.savefig(f'{OUT_DIR}/temperature.png')


def precipitation_pressure_plot():

    df = pd.read_json(IN_FILE)

    air_pressure = df['air_pressure']
    precipitation = df['mean_precipitation']

    fig, ax = plt.subplots()

    ax.scatter(air_pressure, precipitation)
    pressure_range = range(int(air_pressure.min()), round(air_pressure.max()+ 0.5) + 1)

    ax.set(xlim=(air_pressure.min() - 1, air_pressure.max() + 1), xticks=pressure_range,
       ylim=(-0.1, 1.1), yticks=[i/10 for i in range(0, 11)])

    ax.set_xlabel('Air Pressure (hPa)')
    ax.set_ylabel('Mean Precipitation (mm/h)')

    plt.savefig(f'{OUT_DIR}/precipitation_pressure.png')