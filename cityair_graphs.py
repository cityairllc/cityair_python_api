from plotly.offline import init_notebook_mode, plot, iplot
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from matplotlib import pyplot as plt
import plotly.graph_objs as go
import pandas as pd
import os

try:
    init_notebook_mode(connected=True)
except Exception as e:
    print(e.__str__())
    pass


def graph_time(*dfs, descr=None, dropna=True, markers=False):
    traces = []
    for df in dfs:
        for col in df.columns:
            series = df[col]
            if dropna:
                series.dropna(inplace=True)
            traces.append(go.Scatter(x=series.index#.to_pydatetime()
             , y=series, name=col, mode="markers" if markers else None))
    if descr:
        if not os.path.exists("./out"):
            os.makedirs("./out")
        path = f"./out/time_{descr}.html"
        plot(traces, filename=path, auto_open=False)
        print(f"graph saved at {path}")
    else:
        iplot(traces)


def graph_corr(res, ref, descr=None):
    graph_count = res.shape[1]
    fig, ax = plt.subplots(nrows=1, ncols=res.shape[1], figsize=(5 * graph_count, 4))

    for i in range(graph_count):
        tmp_df = pd.DataFrame()
        tmp_df[ref.columns[i]] = ref.iloc[:, i]
        tmp_df[res.columns[i]] = res.iloc[:, i]
        tmp_df.dropna(inplace=True)
        x = tmp_df.iloc[:, 0]
        y = tmp_df.iloc[:, 1]

        ax[i].scatter(x, y)
        ax[i].plot(range(int(x.min()), int(x.max())), range(int(x.min()), int(x.max())), color = 'black')

        max_ = int(ref.iloc[:, i].max())
        ax[i].plot(range(max_), range(max_), c='k')

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
