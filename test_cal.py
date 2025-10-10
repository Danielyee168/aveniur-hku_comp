import pandas as pd
import numpy as np
import time
from config import TRADE_LIST
from src.utils.CalFactorFramework import FactorCalculator, FactorRegistry

from src.utils.factors.rv_daily import compute as rv_daily

calc = FactorCalculator()
registry = FactorRegistry(calc, "./register/factor_registry.json")

register_config = {'name': 'rv_daily', 'factor_func': rv_daily, 'frequency': 'min',
                   'fields': ["symbol", "timestamp", "Close"],
                   'type_': 'alpha', 'category': 'volatility', 'description': 'test'}

registry.register(**register_config) # 使用默认数据注册

if __name__ == '__main__':
    # start_single_process = time.time()
    # registry.calculate(
    #         'rv_daily',
    #         is_batch=False,
    #         is_parallel=False,
    #         symbols=TRADE_LIST,
    #         price_col='Close'  # 这里的是动态参数，根据因子计算函数要求给
    #     )
    # end_single_process = time.time()
    # print(f"single-process takes {end_single_process - start_single_process} seconds")
    # res = registry.get_factor_data('rv_daily')
    # print(res.head())
    # print("===============================")
    # print(res.shape)

    # test multi-process
    start_multi_process = time.time()
    registry.calculate(
            'rv_daily',
            is_batch=True,
            is_parallel=True,
            symbols=TRADE_LIST,
            batch_size=160,
            parallel_batch_size=40,
            n_jobs=4
        )
    end_multi_process = time.time()
    print(f"multi-process takes {end_multi_process - start_multi_process} seconds")
    res1 = registry.get_factor_data('rv_daily')
    print(res1.head())
    print("===============================")
    print(res1.shape)