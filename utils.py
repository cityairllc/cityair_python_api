import pandas as pd


def filter_df(df_or_series, window=5, tol=0.03, shift=-1):
    if isinstance(df_or_series, pd.Series):
        series = df_or_series
        series = series[abs(series - series.rolling(window).mean().shift(-shift)) / series < tol]
        return
    elif isinstance(df_or_series, pd.DataFrame):
        res = pd.DataFrame()
        for col in df_or_series:
            series = df_or_series[col]
            res[col] = series[abs(series - series.rolling(window).mean().shift(-shift)) / series < tol]
        return res
    else:
        raise TypeError('should be pd.Series or pd.DataFrame')


def dropnas(series):
    if not isinstance(series, pd.Series):
        raise TypeError('should be pd.Series')
    df = pd.DataFrame(series)
    df['index'] = series.index
    df.dropna(inplace=True)
    return pd.Series(data=df.iloc[:, 0], index=df['index'])


class Sensor:
    def __init__(self, ref_data=None, sensor_data=None, param_name='Parameter'):
        self.ref = ref_data
        self.sensor_data = sensor_data
        self.param_name = param_name
        self.result_data = None

    def time_graph(self):
        pass

    def params_corr(self):
        pass

    def corr_graph(self):
        pass

    def fit(self, train_start = 0, train_finish = 1):
        pass

    def get_model_params(self):
        pass

    def get_model_metrics(self):
        pass

    def to_csv(self):
        pass

    def read_csv(self):
        pass
