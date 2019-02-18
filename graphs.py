from plotly.offline import init_notebook_mode, plot, iplot
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from matplotlib import pyplot as plt
import matplotlib.dates  as mdates
import plotly.graph_objs as go
import pandas as pd
import os
import time
import numpy as np
from plotly import plotly
from plotly.offline import iplot, init_notebook_mode
import plotly.graph_objs as go
import plotly.io as pio
try:
    from IPython.display import SVG, display, Image
except Exception:
    pass

import os
import numpy as np

try:
    init_notebook_mode(connected=True)
except Exception as e:
    print(e.__str__())
    pass


def graph_time(*dfs, descr=None, dropna=True, markers=False, for_print=False):
    traces = []
    for df in dfs:
        if for_print:
            df = prepare_df(df)
        for col in df.columns:
            series = df[col]
            if dropna:
                series.dropna(inplace=True)
            traces.append(go.Scatter(x=series.index  # .to_pydatetime()
                                     , y=series, name=col, mode="markers" if markers else None))
    return save_plotly_traces(traces, descr, for_print)


def prepare_df(df, max_point_count=20000):
    point_count = df.shape[0] * df.shape[1]
    if point_count > max_point_count:
        df = df[::point_count // max_point_count]
    return df


def save_plotly_traces(traces, descr=None, for_print=False, width=1000, height=500):
    if isinstance(traces, go.Figure):
        fig = traces
    else:
        layout = go.Layout(width=width, height=height)
        fig = go.Figure(data=traces, layout=layout)
    if not os.path.exists("./out"):
        os.makedirs("./out")
    if for_print:
        image_filename = f'{f"./out/time_{descr}.jpg" if descr else "./out/tmp_graph.jpg"}'
        try:
            pio.write_image(fig, image_filename)
        except Exception as e:
            print(f"While saving image in plotly offline mode Exception has occured: {e.__str__()}")
            plotly.plotly.sign_in('EgorKorovin', 'dCuI77pcQp6bmSspU8P3')
            plotly.plotly.image.save_as(fig, filename=image_filename)
    if descr:
        if not for_print:
            path = f"./out/time_{descr}.html"
            plot(traces, filename=path, auto_open=False)
            print(f"graph saved at {path}")
    else:
        if for_print:
            display( Image(image_filename))
        else:
            try:
                iplot(traces)
            except ImportError as e:
                print(e.__str__())


def box_plot(*dfs, descr=None, for_print=False):
    traces = []
    for df in dfs:
        for col in df.columns:
            traces.append(go.Box(y=df[col], name=col))
    return save_plotly_traces(traces, descr, for_print)


def graph_corr(res, ref, descr=None):
    graph_count = res.shape[1]
    fig, ax = plt.subplots(nrows=1, ncols=res.shape[1], figsize=(5 * graph_count, 4))

    if graph_count == 1:
        fig, ax = plt.subplots(nrows=graph_count, ncols=1, figsize=(5, 5), dpi=100)
        x = ref.iloc[:, 0]
        y = res.iloc[:, 0]
        ax.scatter(x, y)
        ax.plot(range(int(x.min()), int(x.max())), range(int(x.min()), int(x.max())), color='black')

        max_ = int(x.max())
        ax.plot(range(max_), range(max_), c='k')

        mse = mean_squared_error(x, y)
        mae = mean_absolute_error(x, y)
        r2 = r2_score(x, y)
        text = f'mse = {mse:.2f} \n mae = {mae:.2f} \n r2 = {r2:.2f}'
        ax.annotate(text, xy=(0.05, 0.8), xycoords='axes fraction')

        ax.set_title(x.name)
        ax.set_xlabel(x.name)
        ax.set_ylabel(y.name)
        ax.grid()
    else:
        for i in range(graph_count):
            tmp_df = pd.DataFrame()
            tmp_df[ref.columns[i]] = ref.iloc[:, i]
            tmp_df[res.columns[i]] = res.iloc[:, i]
            tmp_df.dropna(inplace=True)
            x = tmp_df.iloc[:, 0]
            y = tmp_df.iloc[:, 1]

            ax[i].scatter(x, y)
            ax[i].plot(range(int(x.min()), int(x.max())), range(int(x.min()), int(x.max())), color='black')

            max_ = y.max()
            ax[i].plot(np.linspace(0, max_, 5), np.linspace(0, max_, 5), c='k')
            print(max_)

            mse = mean_squared_error(x, y)
            mae = mean_absolute_error(x, y)
            r2 = r2_score(x, y)
            text = f'mse = {mse:.2f} \n mae = {mae:.2f} \n r2 = {r2:.2f}'
            ax[i].annotate(text, xy=(0.05, 0.8), xycoords='axes fraction')

            ax[i].set_title(res.columns[i])
            ax[i].set_xlabel(x.name)
            ax[i].set_ylabel(y.name)
            ax[i].grid()
    if descr:
        plt.savefig(f'./out/corr_{descr}.png')
    else:
        plt.show()


def graph_time_matplotlib(df, serial_number="", save_file=True, serial_numer_for_path=None):
    params_to_show = {
        'PM2.5': 'grey',
        'RH': 'royalblue',
        'T': 'tomato',
        'CO2': 'C0',
        'O3': 'blue',
        'SO2': 'red',
        'NO2': 'red',
        'CO': 'black',
        'H2S': 'yellow'
    }
    if len(df.index) != 0:
        df = df.resample('10T').mean()
    params = (list(filter(lambda param: param in list(params_to_show.keys()), df.columns)))
    params.sort()
    graph_count = len(params)

    if graph_count == 0 or len(df.index) == 0:
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 5 * 1), dpi=100)
        ax.plot([0], [0])
        ax.annotate(f"{serial_number} has no data", xy=(0.03, 0.92), xycoords='axes fraction', fontweight='bold',
                    backgroundcolor="w")
        ax.grid()
    elif graph_count == 1:
        param = params[0]
        fig, ax = plt.subplots(nrows=graph_count, ncols=1, figsize=(10, 5 * graph_count), dpi=100)
        ax.plot([mdates.date2num(d) for d in df.index], df[param], color=params_to_show[param])
        ax.annotate(serial_number, xy=(0.03, 0.92), xycoords='axes fraction', fontweight='bold',
                    backgroundcolor="w")
        ax.set_ylabel(param)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
        ax.grid()
    else:
        # df = df.resample('10T').mean()
        x = [mdates.date2num(d) for d in df.index]
        fig, ax = plt.subplots(nrows=graph_count, ncols=1, figsize=(10, 5 * graph_count), dpi=100)
        for i, param in enumerate(params):
            ax[i].plot(x, df[param], color=params_to_show[param])
            ax[i].annotate(serial_number, xy=(0.03, 0.92), xycoords='axes fraction', fontweight='bold',
                           backgroundcolor="w")
            ax[i].set_ylabel(param)
            ax[i].xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
            ax[i].grid()
    if save_file:
        path = f'{serial_numer_for_path if serial_numer_for_path else serial_number}_graph.png'
        plt.savefig(path)
        return (path)
    else:
        plt.show()








