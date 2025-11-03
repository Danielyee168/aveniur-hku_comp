#!/usr/bin/env python3

"""Run loop for collecting and storing Roostoo market data snapshots."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import requests
from market_data_collector import collect_once, next_minute_boundary, wait_until

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def main() -> None:

    session = requests.Session()

    try:
        # TODO: remove expired market data files

        # loop execution interval: 1 min
        next_run = next_minute_boundary(datetime.now(timezone.utc))
        while True:
            # market data retrieval
            wait_until(next_run)
            collect_once(session)

            # TODO: signal calculation
            # TODO: order placement
            next_run += timedelta(minutes=1)
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
