import pandas as pd
import matplotlib.pyplot as plt
import os

def forecast_vis_line(to_plot):
    ax = plt.subplot()

    for paths in to_plot:
        df = pd.read_json(paths[0])
        df = df.groupby(pd.Grouper(key='date', axis=0, freq='D', sort=True))
        city = paths[0].split('/')[-1][:-5]
        for parameter in ('temperature', 'mean_precipitation', 'air_pressure'):
            min = df.min()[parameter]
            max = df.max()[parameter]
            mean = df.mean()[parameter]

            plt.plot(mean.index, mean, marker = 'o')
            plt.plot(max.index, max, 'o:')
            plt.plot(min.index, min, 'o:')
            plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
            plt.title(f"Daily {parameter} forecast for {city} - mean, max and min")
            plt.savefig(os.path.join(paths[1], f'{parameter}.png'))
            plt.cla()
    plt.close()


