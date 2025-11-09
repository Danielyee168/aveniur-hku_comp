#!/usr/bin/env python3

"""Run loop for collecting and storing Roostoo market data snapshots."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

import requests

from market_data_collector import collect_once, next_minute_boundary, wait_until, delete_old_data
from calc_util import load_data
from order_executor import rebalance_portfolio
from rv import compute

SIGNAL_HOURS = {6, 18}  # local time hours when we run signal generation
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:

    # configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / "collector.log"
    today = datetime.now()
    if log_file.exists():
        last_modified = datetime.fromtimestamp(log_file.stat().st_mtime)
        if last_modified.date() < today.date():
            archived_name = log_file.with_name(f"{log_file.name}.{last_modified.strftime('%Y-%m-%d')}")
            counter = 1
            while archived_name.exists():
                archived_name = log_file.with_name(
                    f"{log_file.name}.{last_modified.strftime('%Y-%m-%d')}.{counter}"
                )
                counter += 1
            log_file.replace(archived_name)

    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        delay=True,
    )
    file_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.getLogger().addHandler(file_handler)

    # main loop starts from here
    session = requests.Session()

    try:
        # loop execution interval: 1 min
        next_run = next_minute_boundary(datetime.now(timezone.utc))
        while True:
            # remove expired market data files: default retention date is 3 days
            delete_old_data()

            # market data retrieval
            wait_until(next_run)
            cycle_time = next_run
            collect_once(session)
            
            local_time = cycle_time.astimezone()
            time_to_rebalance = local_time.hour in SIGNAL_HOURS and local_time.minute == 0
            if time_to_rebalance:
                df = load_data(frequency='hour')
                df_factor = compute(df, price_col='close')
                if "factor" not in df_factor.columns:
                    logging.error("Factor column missing for %s", cycle_time)
                else:
                    latest_hour = df_factor["timestamp"].max()
                    df_factor = df_factor[df_factor["timestamp"] == latest_hour]

                    # final check data is enough
                    null_ratio = df_factor["factor"].isna().mean()
                    if null_ratio >= 0.5:
                        logging.error(
                            "Factor column is %.0f%% null for %s",
                            null_ratio * 100,
                            cycle_time,
                        )
                    logging.info("Computed factor for %s", latest_hour)

                    # rank the top k symbols in factor value
                    df_factor = df_factor.sort_values("factor", ascending=False)
                    topk = df_factor.head(3)["symbol"].tolist()

                    # rebalance
                    rebalance_portfolio(topk)

            next_run = cycle_time + timedelta(minutes=1)
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
