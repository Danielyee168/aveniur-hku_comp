import pandas as pd
import numpy as np
import os

path = './data/'
path_raw = path + 'raw'


def basic_washing(path_=path_raw):
    """
    :param path_:
    :return:
    """
    data_all = pd.DataFrame()

    # single coin time series processing
    # multi-processes
    for filename in os.listdir(path_):
        filepath = os.path.join(path_, filename)
        coin_df = pd.read_parquet(filepath)

        # complete the timestamp


        coin_df['timestamp'] = pd.to_datetime(coin_df['timestamp'], unit='ms')  # transform timestamp to datetime

        # transform dtypes
        c_col = coin_df.columns
        c_col.remove('timestamp')
        c_col.remove('count')
        for c in c_col:
            coin_df[c] = coin_df[c].astype('float')
        coin_df = coin_df.sort_values('timestamp')
        coin_df.drop_duplicates('timestamp', keep='last')

    # cross-section processing: filtering out extremes; standardization

    # dump data into a pickle file

    return


def data_graph(date_range, path_=path):
    """
    :param date_range:
    :param path_:
    :return:
    """
    file_directory = path_ + 'washed'
    if not os.listdir(file_directory):
        df = basic_washing
    else:
        file_path = [os.path.join(path_, filename) for filename in os.listdir(file_directory)]
        df = pd.read_pickle(file_path[0])

    # build graph

    return df
