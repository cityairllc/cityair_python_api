import pandas as pd
import os
from math import ceil
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from plotly.offline import init_notebook_mode, plot, iplot
from plotly import plotly
import plotly.offline
from plotly import tools
from plotly.offline import iplot, init_notebook_mode
import plotly.graph_objs as go
import plotly.io as pio

from utils import *

try:
    from IPython.display import SVG, display, Image
except Exception:
    pass

try:
    init_notebook_mode(connected=True)
except Exception as e:
    print(e.__str__())
    pass


def plot(*figs, descr=None, for_print=False, cols=3, width=1000):
    fig_count = len(figs)
    if fig_count > 1:
        cols = min(fig_count, cols)
        rows = ceil(fig_count / cols)
        aspect_ratio = figs[0].layout['width'] / figs[0].layout['height']
        fig = tools.make_subplots(rows=rows, cols=cols, subplot_titles=[_.layout['title']['text'] for _ in figs],
                                  horizontal_spacing=0.3 / cols, vertical_spacing=0.6 / rows)
        for row in range(rows):
            for col in range(cols):
                i = col + cols * row
                if i >= fig_count:
                    break
                for trace in figs[i].data:
                    fig.append_trace(trace, row + 1, col + 1)
        for i in range(fig_count):
            fig['layout'][f'xaxis{i + 1}'].update(figs[i].layout['xaxis'])
            fig['layout'][f'yaxis{i + 1}'].update(figs[i].layout['yaxis'])
            fig['layout'][f'yaxis{i + 1}'].update(automargin=False)
            fig['layout'][f'xaxis{i + 1}'].update(automargin=False)
        fig['layout']['showlegend'] = False
        fig['layout']['width'], fig['layout']['height'] = width, (width / aspect_ratio) * rows / cols + (
                rows - 1) * width * 0.1
    else:
        fig = figs[0]
    fig['layout']["plot_bgcolor"] = "rgba(0, 0, 0, 0)"
    fig['layout']["paper_bgcolor"] = "rgba(0, 0, 0, 0)"

    if not os.path.exists("./out"):
        os.makedirs("./out")
    path = f'{f"./out/graph_{descr}" if descr else "./out/tmp"}'

    if for_print:
        image_filename = f"{path}.png"
        try:
            pio.write_image(fig, image_filename, scale=5)
        except Exception as e:
            print(f"While saving image in plotly offline mode Exception has occured: {e.__str__()}")
            plotly.plotly.sign_in('EgorKorovin', 'dCuI77pcQp6bmSspU8P3')
            plotly.plotly.image.save_as(fig, filename=image_filename)
        display(Image(image_filename))
    else:
        iplot(fig)
        plotly.offline.plot(fig, filename=f"{path}.html", auto_open=False)
    print(f"graph saved at {path}")
    return fig


def corr_(x, y=None, temperature=None, metrics=[], line_func=lambda x: x):
    if isinstance(x, pd.Series):
        df = pd.concat([series.resample('1T').mean() for series in [x, y]], axis=1)
    else:
        df = x
    df.dropna(inplace=True)
    x, y = df.iloc[:, 0], df.iloc[:, 1]

    traces = []
    traces.append(go.Scatter(x=x, y=y, name=y.name, mode="markers",
                             marker=dict(color=temperature,
                                         # colorbar=dict(title='Colorbar'),
                                         colorscale='Rainbow') if temperature is not None else None))
    if line_func:
        line_x = np.linspace(0, x.max(), 300)
        line_y = [line_func(x) for x in line_x]
        traces.append(go.Scatter(x=line_x, y=line_y, line=dict(color='black'), mode="lines", showlegend=False))

    layout = go.Layout(
        width=500,
        height=500,
        title=f"<b>{y.name} vs {x.name}</b><br>{'<br>'.join([f'{metric}={metrics[metric]}' for metric in metrics])}",
        showlegend=False,
        xaxis=dict(title=x.name, range=[0, x.max()], nticks=6),
        yaxis=dict(title=y.name, range=[0, line_func(x.max())] if line_func else None, nticks=6)
    )
    return go.Figure(data=traces, layout=layout)


def time_(*dfs, descr=None, markers=False, for_print=False, dropna=True):
    traces = []
    for df in dfs:
        if for_print:
            df = prepare_df(df)
        if isinstance(df, pd.Series):
            series = df
            if dropna:
                series.dropna(inplace=True)
            traces.append(go.Scatter(x=series.index.strftime("%Y-%m-%d %H:%M:%S")
                                     , y=series, name=series.name, mode="markers" if markers else None))
        else:
            for col in df.columns:
                series = df[col]
                if dropna:
                    series.dropna(inplace=True)
                traces.append(go.Scatter(x=series.index.strftime("%Y-%m-%d %H:%M:%S")
                                         , y=series, name=series.name, mode="markers" if markers else None))

        layout = go.Layout(width=1000, height=500, title=descr)
        return go.Figure(data=traces, layout=layout)


def box_plot(*dfs, descr=None, for_print=False):
    traces = []
    for df in dfs:
        for col in df.columns:
            traces.append(go.Box(y=df[col], name=col))

    layout = go.Layout(width=1000, height=500, title=descr)
    return go.Figure(data=traces, layout=layout)


def prepare_df(df, max_point_count=20000):
    point_count = df.shape[0] * df.shape[1]
    if point_count > max_point_count:
        df = df[::point_count // max_point_count]
    return df
