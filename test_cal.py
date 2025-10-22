import pandas as pd
from src.utils.CalFactorFramework import FactorCalculator, FactorRegistry
from src.utils.BacktestTools import SingleAlphaFactorBacktester, BacktestContext
from src.models.factors import rsk
from config import TRADE_LIST


if __name__ == "__main__":
    calc = FactorCalculator()
    registry = FactorRegistry(calc, "./register/factor_registry.json")
    # registry.register(**rsk.DEFAULT_CONFIG)
    # registry.update_factor(corwin_schultz.DEFAULT_CONFIG['name'], "logic_update", **{'type': "alpha"})
    # registry.calculate(
    #         rsk.DEFAULT_CONFIG['name'],
    #         is_batch=True,
    #         is_parallel=True,
    #         batch_size=180,
    #         parallel_batch_size=60,
    #         n_jobs=3,
    #         symbols=TRADE_LIST,
    #         col_name=rsk.DEFAULT_CONFIG['name'],
    #         window=48 * 60,
    #         # price_col='Close'  # 这里的是动态参数，根据因子计算函数要求给
    #     )

    test = registry.get_factor_data(rsk.DEFAULT_CONFIG['name'])

    close = pd.read_parquet('./data/hour_data/all_data.parquet')[['symbol', 'timestamp', 'Close']]

    test_contest = BacktestContext(factor_df=test.set_index(['symbol', 'timestamp'], drop=False), factor_name=rsk.DEFAULT_CONFIG['name'],
                                   price_data=close.set_index(['symbol', 'timestamp'], drop=False), rebalance_freq='1H', price_col='Close')

    test_backtest = SingleAlphaFactorBacktester(10)
    result = test_backtest.run(test_contest)
    test_backtest.plot_summary(result, cost_Unilateral=0.0004, save_path='test.jpg')
    print(result)
