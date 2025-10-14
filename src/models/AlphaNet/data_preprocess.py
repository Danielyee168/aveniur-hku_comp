import numpy as np
import pandas as pd
from typing import (
    List,
    Optional,
    Tuple
)

from tqdm import tqdm
from sklearn.preprocessing import QuantileTransformer
from sklearn.preprocessing import StandardScaler
from multiprocessing import Pool

from DataLoader import Kline


def data_standardize(df:pd.DataFrame,feature:str,method:str='quantile')->np.array:  # Quantile normalization or z-score

    scaler = StandardScaler()
    qt = QuantileTransformer(output_distribution='normal', random_state=42)
    pivot_df = df.pivot_table(index='trade_date', columns='st_code', values=feature)

    if method == 'quantile':
        standardized_array = np.stack(pivot_df.apply(lambda x:qt.fit_transform(x.values.reshape(-1,1)).reshape(-1), axis=1).values)
        standardized_df =pd.DataFrame(standardized_array, columns=pivot_df.columns,index=pivot_df.index)
    else:

        standardized_array =  np.stack(pivot_df.apply(lambda x: scaler.fit_transform(x.values.reshape(-1,1)).reshape(-1),axis=1).values)
        standardized_df =pd.DataFrame(standardized_array, columns=pivot_df.columns,index=pivot_df.index)

    return standardized_df.unstack().dropna()

def data_transform(origin_data: pd.DataFrame, lookback:int=30) -> Tuple[np.array,np.array,np.array]:
    # Pre-compute the set of unique tickers
    unique_codes = origin_data.index.map(lambda x:x[0]).unique()
    
    # Pre-allocate containers
    X_list = []
    Y_list = []
    indices_list = []
    empty = []
    
    # Group by symbol once to avoid repeated loc operations
    grouped_data = origin_data.groupby(level=0)
    
    for code in tqdm(unique_codes):
        try:
            # Fetch the timeseries for the current ticker
            df = grouped_data.get_group(code)
            
            # Slide a window across the feature matrix
            data = df.iloc[:, :-1].values  # Feature matrix
            returns = df['rtn'].values     # Target returns
            dates = df.index.get_level_values(1).values  # Timestamp index
            
            # Determine how many samples can be generated
            n_samples = len(df) - lookback + 1
            if n_samples <= 0:
                print(f"Code {code}: Not enough data (need {lookback}, got {len(df)})")
                empty.append(code)
                continue
                
            # Create sliding windows with NumPy
            X = np.lib.stride_tricks.sliding_window_view(data, (lookback, data.shape[1]))

            X = X[::5]
            X = X.reshape([X.shape[0],X.shape[2],X.shape[3]])  # Down-sample every 5 trading days
            # Gather labels and timestamps aligned with the subsampling
            Y = returns[lookback-1::5]
            dates = dates[lookback-1::5]
            
            if len(X) == 0:
                empty.append(code)
                continue
                
            # Transpose to match the (batch, feature, time) convention
            X = np.transpose(X, (0, 2, 1))
            
            # Build indices; convert timestamps to strings to avoid dtype issues
            dates_str = pd.to_datetime(dates).strftime('%Y-%m-%d %H:%M:%S').astype(str)
            indices = np.column_stack((np.full(len(X), code), dates_str))
            
            X_list.append(X)
            Y_list.append(Y)
            indices_list.append(indices)
            
        except Exception as e:
            print(f"Error processing code {code}: {e}")
            empty.append(code)
            continue
    
    # Bail out if no valid samples were produced
    if not X_list:
        print("No valid data found. All stocks were filtered out.")
        return np.array([]), np.array([]), np.array([])
    
    # Concatenate data from every symbol
    X = np.concatenate(X_list, axis=0)
    Y = np.concatenate(Y_list, axis=0)
    indices = np.concatenate(indices_list, axis=0)
    
    print('Shape of X: ', X.shape)
    print('Shape of Y: ', Y.shape)
    print('Shape of indices: ', indices.shape)
    print('Stocks with not enough data: ', empty)
    
    return X, Y, indices

if __name__ == '__main__':
    from config import PARQUET_PATH
    my_data = Kline(parquet_path=PARQUET_PATH)
    data = my_data.Data
    data = data.sort_values(by=['st_code', 'trade_date'])
    # Compute forward returns per symbol and realign indices to match the original DataFrame
    data['rtn'] = data.groupby('st_code')['close_adj'].apply(lambda x: x.pct_change(5).shift(-5)).reset_index(level=0, drop=True)  # Predict the next 5-day return
    data.loc[:, 'trade_state'], data.loc[:, 'close_adj'] = data.loc[:, 'close_adj'], data.loc[:, 'trade_state']
    data = data.rename(columns={'close_adj': 'trade_state', 'trade_state': 'close_adj'})
    data = data.dropna(axis=0)
    standard_sr_list = []
    for col in tqdm(data.columns[3:-1]):
        standard_feature = data_standardize(data,feature=col,method='zscore')
        standard_sr_list.append(standard_feature)
    standard_sr_list.append(data.set_index(['st_code','trade_date'])['rtn'])
    standard_data = pd.concat(standard_sr_list,axis=1)
    standard_data.columns = data.columns[3:]
    print(f'standard_data_column: {standard_data.columns}')

    a,b,c = data_transform(standard_data, lookback=30)
    np.save('Features.npy', a)
    np.save('Labels.npy', b)
    np.save('indices.npy', c)
