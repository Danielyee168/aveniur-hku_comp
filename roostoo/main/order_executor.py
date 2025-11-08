import json
import logging
import time
from pathlib import Path
from typing import Dict, Optional, Sequence

import requests
import hashlib
import hmac
from decimal import Decimal, ROUND_DOWN

CREDENTIALS_PATH = Path(__file__).resolve().parent / "credentials.json"

with CREDENTIALS_PATH.open("r", encoding="utf-8") as fh:
    creds = json.load(fh)
    API_KEY = creds["key"]
    SECRET_KEY = creds["secret"]

BASE_URL = "https://mock-api.roostoo.com"
_EXCHANGE_INFO_CACHE: Optional[dict] = None
API_MAX_RETRIES = 5
API_RETRY_DELAY = 1


# ------------------------------
# Utility Functions
# ------------------------------

def _get_timestamp():
    """Return a 13-digit millisecond timestamp as string."""
    return str(int(time.time() * 1000))


def _get_signed_headers(payload: dict = {}):
    """
    Generate signed headers and totalParams for RCL_TopLevelCheck endpoints.
    """
    payload['timestamp'] = _get_timestamp()
    sorted_keys = sorted(payload.keys())
    total_params = "&".join(f"{k}={payload[k]}" for k in sorted_keys)

    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        total_params.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    headers = {
        'RST-API-KEY': API_KEY,
        'MSG-SIGNATURE': signature
    }

    return headers, payload, total_params


def _request_with_retries(method: str, url: str, **kwargs):
    """Send an HTTP request with automatic retries."""
    last_error = None
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            last_error = exc
            print(f"[ERROR] {method.upper()} {url} failed (attempt {attempt}/{API_MAX_RETRIES}): {exc}")
            if attempt < API_MAX_RETRIES:
                time.sleep(API_RETRY_DELAY)
    if last_error:
        raise last_error
    raise RuntimeError("Request failed without exception.")


# ------------------------------
# Public Endpoints
# ------------------------------

def get_ticker(pair=None):
    """Get ticker for one or all pairs."""
    url = f"{BASE_URL}/v3/ticker"
    params = {'timestamp': _get_timestamp()}
    if pair:
        params['pair'] = pair
    try:
        res = _request_with_retries("GET", url, params=params)
        return res.json()
    except requests.exceptions.RequestException as e:
        logging.error("Error getting ticker: %s", e)
        return None


# ------------------------------
# Signed Endpoints
# ------------------------------

def get_balance():
    """Get wallet balances (RCL_TopLevelCheck)."""
    url = f"{BASE_URL}/v3/balance"
    headers, payload, _ = _get_signed_headers({})
    try:
        res = _request_with_retries("GET", url, headers=headers, params=payload)
        return res.json()
    except requests.exceptions.RequestException as e:
        logging.error("Error getting balance: %s", e)
        logging.error("Response text: %s", e.response.text if e.response else 'N/A')
        return None


def get_pending_count():
    """Get total pending order count."""
    url = f"{BASE_URL}/v3/pending_count"
    headers, payload, _ = _get_signed_headers({})
    try:
        res = _request_with_retries("GET", url, headers=headers, params=payload)
        return res.json()
    except requests.exceptions.RequestException as e:
        logging.error("Error getting pending count: %s", e)
        logging.error("Response text: %s", e.response.text if e.response else 'N/A')
        return None


def place_order(pair_or_coin, side, quantity, price=None, order_type=None):
    """
    Place a LIMIT or MARKET order.
    """
    url = f"{BASE_URL}/v3/place_order"
    pair = f"{pair_or_coin}/USD" if "/" not in pair_or_coin else pair_or_coin

    if order_type is None:
        order_type = "LIMIT" if price is not None else "MARKET"

    if order_type == 'LIMIT' and price is None:
        logging.error("LIMIT orders require 'price'.")
        return None

    payload = {
        'pair': pair,
        'side': side.upper(),
        'type': order_type.upper(),
        'quantity': str(quantity)
    }
    if order_type == 'LIMIT':
        payload['price'] = str(price)

    headers, _, total_params = _get_signed_headers(payload)
    headers['Content-Type'] = 'application/x-www-form-urlencoded'

    try:
        res = _request_with_retries("POST", url, headers=headers, data=total_params)
        return res.json()
    except requests.exceptions.RequestException as e:
        logging.error("Error placing order: %s", e)
        logging.error("Response text: %s", e.response.text if e.response else 'N/A')
        return None


def query_order(order_id=None, pair=None, pending_only=None):
    """Query order history or pending orders."""
    url = f"{BASE_URL}/v3/query_order"
    payload = {}
    if order_id:
        payload['order_id'] = str(order_id)
    elif pair:
        payload['pair'] = pair
        if pending_only is not None:
            payload['pending_only'] = 'TRUE' if pending_only else 'FALSE'

    headers, _, total_params = _get_signed_headers(payload)
    headers['Content-Type'] = 'application/x-www-form-urlencoded'

    try:
        res = _request_with_retries("POST", url, headers=headers, data=total_params)
        return res.json()
    except requests.exceptions.RequestException as e:
        logging.error("Error querying order: %s", e)
        logging.error("Response text: %s", e.response.text if e.response else 'N/A')
        return None


def cancel_order(order_id=None, pair=None):
    """Cancel specific or all pending orders."""
    url = f"{BASE_URL}/v3/cancel_order"
    payload = {}
    if order_id:
        payload['order_id'] = str(order_id)
    elif pair:
        payload['pair'] = pair

    headers, _, total_params = _get_signed_headers(payload)
    headers['Content-Type'] = 'application/x-www-form-urlencoded'

    try:
        res = _request_with_retries("POST", url, headers=headers, data=total_params)
        return res.json()
    except requests.exceptions.RequestException as e:
        logging.error("Error canceling order: %s", e)
        logging.error("Response text: %s", e.response.text if e.response else 'N/A')
        return None



# ------------------------------
# Self-defined
# ------------------------------

def _normalize_pair(symbol: str) -> str:
    return symbol if "/" in symbol else f"{symbol}/USD"


def _extract_asset(pair: str) -> str:
    return pair.split("/")[0]


def _fetch_last_price(pair: str, max_retries: int = 10, retry_delay: int = 1) -> float | None:
    for attempt in range(1, max_retries + 1):
        ticker = get_ticker(pair)
        if ticker and ticker.get("Success"):
            data = ticker.get("Data", {})
            details = data.get(pair)
            if not details:
                logging.error("Ticker payload missing data for %s", pair)
                return None

            price = details.get("LastPrice")
            if price is None:
                logging.error("Ticker payload missing LastPrice for %s", pair)
                return None

            return float(price)

        logging.warning(
            "Failed to fetch ticker for %s (attempt %s/%s). Retrying in %ss.",
            pair,
            attempt,
            max_retries,
            retry_delay,
        )
        if attempt < max_retries:
            time.sleep(retry_delay)

    logging.error("Exceeded retries fetching ticker for %s", pair)
    return None


def _get_pair_rules(pair: str) -> Dict[str, float]:
    info = get_exchange_info()
    trade_pairs = (info or {}).get("TradePairs", {})
    return trade_pairs.get(pair, {})


def _quantize_quantity(quantity: float, precision: Optional[int]) -> float:
    if precision is None:
        return quantity
    if precision < 0:
        precision = 0
    fmt = "1." + ("0" * precision)
    quantized = Decimal(str(quantity)).quantize(Decimal(fmt), rounding=ROUND_DOWN)
    return float(quantized)


def rebalance_portfolio(top_symbols: Sequence[str], allocation_ratio: float = 0.9, usd_symbol: str = "USD"):
    """
    Rebalance holdings into the provided top symbols using equal-weighted allocation.
    """
    logging.info("Starting rebalance with symbols: %s", top_symbols)
    logging.info("Allocation ratio: %.2f, USD symbol: %s", allocation_ratio, usd_symbol)

    if not top_symbols:
        logging.warning("No symbols provided for rebalance.")
        return

    desired_assets = []
    seen_assets = set()

    for symbol in top_symbols:
        pair = _normalize_pair(symbol)
        asset = _extract_asset(pair)
        if asset in seen_assets:
            logging.info("Skipping duplicate asset %s from symbol %s", asset, symbol)
            continue
        seen_assets.add(asset)
        desired_assets.append(asset)
        logging.info("Target asset added: %s (pair %s)", asset, pair)

    if not desired_assets:
        logging.warning("No valid symbols provided for rebalance.")
        return

    balance = get_balance()
    if not balance or not balance.get("Success"):
        logging.error("Unable to fetch account balance.")
        return

    spot_wallet = balance.get("SpotWallet", {})
    logging.info("Current spot wallet: %s", spot_wallet)
    holdings = {
        asset: float(details.get("Free", 0) or 0)
        for asset, details in spot_wallet.items()
        if asset != usd_symbol
    }
    usd_free = float(spot_wallet.get(usd_symbol, {}).get("Free", 0) or 0)
    logging.info("Parsed holdings (non-USD): %s", holdings)
    logging.info("Free USD balance: %.2f", usd_free)

    # Sell assets not in the desired list
    for asset, quantity in holdings.items():
        if quantity <= 0:
            continue
        if asset not in seen_assets:
            pair = _normalize_pair(asset)
            logging.info("Selling %s %s to exit non-target position.", quantity, asset)
            result = place_order(pair, "SELL", quantity)
            logging.info("Sell order result for %s: %s", pair, result)

    # Refresh balance after potential sales
    balance = get_balance()
    if balance and balance.get("Success"):
        spot_wallet = balance.get("SpotWallet", spot_wallet)
        usd_free = float(spot_wallet.get(usd_symbol, {}).get("Free", usd_free) or usd_free)
        holdings = {
            asset: float(details.get("Free", 0) or 0)
            for asset, details in spot_wallet.items()
            if asset != usd_symbol
        }
        logging.info("Updated spot wallet after sales: %s", spot_wallet)
        logging.info("Updated holdings: %s", holdings)
        logging.info("Updated USD balance: %.2f", usd_free)
    else:
        logging.error("Failed to refresh balance after sales, continuing with previous values.")

    budget = usd_free * allocation_ratio
    logging.info("Total budget for purchases (%.0f%% of USD): %.2f", allocation_ratio * 100, budget)
    if budget <= 0:
        logging.info("No USD available for purchases.")
        return

    per_symbol_budget = budget / len(desired_assets)
    logging.info("Per-symbol budget: %.2f", per_symbol_budget)
    assets_to_buy = [asset for asset in desired_assets if holdings.get(asset, 0) <= 0]
    logging.info("Assets to buy (no current holdings): %s", assets_to_buy)

    for asset in assets_to_buy:
        pair = _normalize_pair(asset)
        rules = _get_pair_rules(pair)
        amount_precision = rules.get("AmountPrecision")
        if amount_precision is not None:
            try:
                amount_precision = int(amount_precision)
            except (TypeError, ValueError):
                amount_precision = None
        mini_order = float(rules.get("MiniOrder", 0) or 0)
        last_price = _fetch_last_price(pair)
        if not last_price or last_price <= 0:
            logging.error("Skipping %s due to invalid price.", pair)
            continue

        quantity = per_symbol_budget / last_price
        if quantity <= 0:
            logging.error("Calculated non-positive quantity for %s.", pair)
            continue

        min_qty = mini_order / last_price if mini_order > 0 else 0
        quantity = max(quantity, min_qty)
        quantity = _quantize_quantity(quantity, amount_precision)

        if mini_order > 0 and quantity * last_price < mini_order:
            if amount_precision is not None and amount_precision >= 0:
                step = 10 ** (-amount_precision) if amount_precision > 0 else 1
                quantity = _quantize_quantity(quantity + step, amount_precision)

        if mini_order > 0 and quantity * last_price < mini_order:
            logging.error(
                "Unable to satisfy mini order for %s. Required notional %.4f, got %.4f",
                pair,
                mini_order,
                quantity * last_price,
            )
            continue

        if quantity <= 0:
            logging.error("Quantity rounded down to zero for %s. Skipping.", pair)
            continue

        logging.info(
            "Buying %.6f %s (~$%.2f at price %.4f; precision=%s, mini_order=%.4f)",
            quantity,
            asset,
            quantity * last_price,
            last_price,
            amount_precision,
            mini_order,
        )
        result = place_order(pair, "BUY", quantity)
        logging.info("Buy order result for %s: %s", pair, result)
def get_exchange_info(force_refresh: bool = False):
    """Fetch and cache exchange information."""
    global _EXCHANGE_INFO_CACHE
    if _EXCHANGE_INFO_CACHE is not None and not force_refresh:
        return _EXCHANGE_INFO_CACHE

    url = f"{BASE_URL}/v3/exchangeInfo"
    try:
        res = _request_with_retries("GET", url)
        _EXCHANGE_INFO_CACHE = res.json()
        return _EXCHANGE_INFO_CACHE
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Error getting exchange info: {e}")
        return None
