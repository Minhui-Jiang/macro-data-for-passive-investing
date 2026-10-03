#!/usr/bin/env python3
"""Append one snapshot of the latest published observations to the existing CSV.

Run: uv run update_data.py
Preview: uv run update_data.py --dry-run
All source URLs are embedded here; no Numbers file is required at runtime.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from filelock import FileLock

DEFAULT_CSV = Path(__file__).resolve().with_name("macro_data.csv")
COLUMNS = (
    "date",
    "U.S.",
    "exUS-dev",
    "EM",
    "Europe-dev",
    "China",
    "EFFR",
    "SOFR",
    "10y-US",
    "30y-US",
    "Baa premium",
    "SHIBOR-overnight",
    "10y-China",
    "10y-Germany",
    "10y-France",
    "10y-Italy",
    "10y-Japan",
    "USD index",
    "USD/CNY",
    "WTI($/barrel)",
    "Natural Gas($/MMBtu)",
    "HRC($/20short tons )",
    "gold($/toz)",
    "silver($/toz)",
    "gold-silver-ratio",
    "copper($/metric tonne)",
    "wheat(¢/bu)",
    "BTC($)",
    "Swiss Stocks",
    "10y-swiss",
    "SARON-overnight",
    "CHF/USD",
)

# Original URLs transcribed from data_source.numbers, including incomplete links.
# Resolved product links and any fallback URLs appear in the scrapers below.
ORIGINAL_SOURCES = {
    "U.S.": "https://www.ishares.com/ch/individual/en/products/251900/ishares-sp-500-ucits-etf-inc-fund",
    "exUS-dev": "https://www.ishares.com/us/products/286762/ishares-core-msci-international-developed-markets-etf#/",
    "EM": "https://www.ishares.com/us/products/239637/ishares-msci-emerging-markets-etf",
    "Europe-dev": "https://www.ishares.com/ch/individual/en/products/251860/ishares-msci-europe-ucits-etf-inc-fund",
    "Swiss Stocks": "https://www.ishares.com/ch/professionals/en/products",
    "China": "https://www.ishares.com/us/products/239619/ishares-msci-china-etf",
    "EFFR": "https://www.newyorkfed.org/markets/reference-rates/effr",
    "SOFR": "https://www.newyorkfed.org/markets/reference-rates/sofr",
    "10y-US": "https://fred.stlouisfed.org/series/DGS10",
    "30y-US": "https://fred.stlouisfed.org/series/DGS30",
    "Baa premium": "https://fred.stlouisfed.org/series/BAA10Y",
    "10y-China": "https://www.wsj.com/market-data/quotes/bond/BX/AMBMKRM-10Y",
    "10y-Germany": "https://www.marketwatch.com/investing/bond/tmbmkde-10y?countrycode=bx",
    "10y-France": "https://www.marketwatch.com/investing/bond/tmbmkfr-10y?countrycode=bx",
    "10y-Italy": "https://www.marketwatch.com/investing/bond/tmbmkit-10y?countrycode=bx",
    "10y-Japan": None,  # No URL was supplied; resolved below.
    "SHIBOR-overnight": "https://data.eastmoney.com/shibor/default.html",
    "10y-swiss": "https://www.snb.ch/en/the-snb/mandates-goals/statistics/statistics-pub/current_interest_exchange_rates#t00",
    "SARON-overnight": "https://www.snb.ch/en/the-snb/mandates-goals/statistics/statistics-pub/current_interest_exchange_rates#t00",
    "USD/CNY": "https://wise.com/gb/currency-converter/usd-to-cny-rate",
    "CHF/USD": "https://wise.com/gb/currency-converter/chf-to-usd-rate",
    "USD index": "https://www.marketwatch.com/investing/index/dxy",
    "WTI($/barrel)": "https://www.marketwatch.com/investing/future/cl.1",
    "HRC($/20short tons )": "https://www.marketwatch.com/investing/future/hrn00?utm_source=chatgpt.com",
    "gold($/toz)": "https://goldprice.org/de/live-gold-price.html",
    "silver($/toz)": "https://www.marketwatch.com/investing/future/SI.1?mod=MW_story_quote",
    "copper($/metric tonne)": "https://www.lme.com/en/metals/non-ferrous/lme-copper#Summary",
    "Natural Gas($/MMBtu)": "https://www.marketwatch.com/investing/future/ng.1",
    "wheat(¢/bu)": "https://www.marketwatch.com/investing/future/w.1",
    "BTC($)": "https://www.kraken.com/prices/bitcoin",
}


class SourceError(ValueError):
    """The source did not provide an identifiable, usable observation."""


@dataclass(frozen=True)
class Observation:
    value: Decimal
    as_of: str
    source_url: str
    note: str = ""


def number(value: Any) -> Decimal:
    text = str(value).strip().replace("−", "-").replace(",", "")
    try:
        result = Decimal(text)
    except InvalidOperation as exc:
        raise SourceError(f"Invalid number: {value!r}") from exc
    if not result.is_finite():
        raise SourceError(f"Non-finite number: {value!r}")
    return result


def epoch_iso(value: Any, *, milliseconds: bool = False) -> str:
    return datetime.fromtimestamp(float(value) / (1000 if milliseconds else 1), UTC).isoformat()


class HttpClient:
    """Shared thread-safe HTTP connection pool, bounded retries and timeouts."""

    def __init__(self, timeout: float = 25):
        self.client = httpx.Client(
            follow_redirects=True,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"},
        )

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        for attempt in range(3):
            try:
                response = self.client.get(url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                # A login requirement or bot challenge is not a transient error.
                if exc.response.status_code not in (429, 500, 502, 503, 504) or attempt == 2:
                    raise
            except httpx.TransportError:
                if attempt == 2:
                    raise
            time.sleep(0.5 * (2**attempt))
        raise AssertionError("unreachable")

    def close(self) -> None:
        self.client.close()


def parse_wise(html: str, source: str, target: str, url: str) -> Observation:
    tag = BeautifulSoup(html, "html.parser").select_one("script#__NEXT_DATA__")
    if tag is None:
        raise SourceError("Wise page is missing its currency-converter data")
    rate = json.loads(tag.get_text())["props"]["pageProps"]["model"]["rate"]
    if (rate["sourceCurrency"]["code"], rate["targetCurrency"]["code"]) != (source, target):
        raise SourceError("Wise returned a different currency pair")
    return Observation(
        number(rate["value"]),
        epoch_iso(rate["providerTimestamp"], milliseconds=True),
        url,
        f"Mid-market {target} per 1 {source}; provider timestamp",
    )


def fetch_wise(client: HttpClient, column: str) -> Observation:
    source, target = column.split("/")
    url = ORIGINAL_SOURCES[column]
    return parse_wise(client.get(url).text, source, target, url)


def parse_kraken(payload: dict, url: str) -> Observation:
    if payload.get("error"):
        raise SourceError(f"Kraken: {payload['error']}")
    trades = payload["result"]["XXBTZUSD"]
    if not trades:
        raise SourceError("Kraken returned no BTC/USD trades")
    latest = max(trades, key=lambda trade: float(trade[2]))
    return Observation(
        number(latest[0]), epoch_iso(latest[2]), url, "Latest executed BTC/USD trade on Kraken"
    )


def fetch_kraken(client: HttpClient) -> Observation:
    # Official public feed behind the exchange website. Trades includes an exact timestamp.
    url = "https://api.kraken.com/0/public/Trades?pair=XBTUSD&count=1"
    return parse_kraken(client.get(url).json(), url)


def parse_goldprice(payload: dict, url: str) -> Observation:
    items = [item for item in payload["items"] if item.get("curr") == "USD"]
    if len(items) != 1:
        raise SourceError("GoldPrice did not identify one USD quote")
    return Observation(
        number(items[0]["xauPrice"]),
        epoch_iso(payload["ts"], milliseconds=True),
        url,
        "Gold spot price in USD per troy ounce",
    )


def fetch_gold(client: HttpClient) -> Observation:
    url = "https://data-asg.goldprice.org/dbXRates/USD"
    try:
        return parse_goldprice(client.get(url).json(), url)
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
        reason = str(exc)
    # GoldPrice blocks some networks. This public quote is XAU spot, not GC futures.
    url = "https://api.gold-api.com/price/XAU"
    payload = client.get(url).json()
    if payload["symbol"] != "XAU" or payload.get("currency", "USD") != "USD":
        raise SourceError("Gold-API did not return XAU in USD")
    return Observation(
        number(payload["price"]),
        payload["updatedAt"],
        url,
        f"Fallback gold spot USD/troy ounce; GoldPrice unavailable: {reason}",
    )


def parse_westmetall(html: str, url: str) -> Observation:
    soup = BeautifulSoup(html, "html.parser")
    table = next(
        (
            table
            for table in soup.select("table")
            if "LME Copper Cash-Settlement" in table.get_text(" ", strip=True)
        ),
        None,
    )
    if table is None:
        raise SourceError("Missing Westmetall LME copper cash-settlement table")
    headers = [cell.get_text(" ", strip=True) for cell in table.select("tr")[0].select("th")]
    index = headers.index("LME Copper Cash-Settlement")
    values = []
    for row in table.select("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.select("td")]
        if len(cells) != len(headers):
            continue
        as_of = datetime.strptime(cells[0], "%d. %B %Y").date()
        values.append((as_of, number(cells[index])))
    if not values:
        raise SourceError("No dated LME copper cash observations")
    as_of, value = max(values)
    return Observation(
        value,
        as_of.isoformat(),
        url,
        "LME cash settlement via Westmetall, USD/tonne; 25 tonnes is lot size",
    )


def fetch_copper(client: HttpClient) -> Observation:
    # LME's public page renders price tables in JavaScript and may block HTTP clients.
    # Westmetall republishes the same LME cash-settlement series as dated HTML rows.
    url = "https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash"
    return parse_westmetall(client.get(url).text, url)


ISHARES_URLS = {
    "U.S.": "https://www.ishares.com/ch/individual/en/products/251900/ishares-sp-500-ucits-etf-inc-fund",
    "exUS-dev": "https://www.ishares.com/us/products/286762/ishares-core-msci-international-developed-markets-etf",
    "EM": "https://www.ishares.com/us/products/239637/ishares-msci-emerging-markets-etf",
    "Europe-dev": "https://www.ishares.com/ch/individual/en/products/251860/ishares-msci-europe-ucits-etf-inc-fund",
    "China": "https://www.ishares.com/us/products/239619/ishares-msci-china-etf",
    "Swiss Stocks": "https://www.ishares.com/ch/professionals/en/products/264107/ishares-spi-ch-fund",
}


def _ishares_date(value: Any) -> date:
    value = re.sub(r"^as of\s+", "", str(value).strip(), flags=re.I)
    value = value.replace("Sept", "Sep")
    for fmt in ("%Y%m%d", "%d/%b/%Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unrecognised iShares observation date: {value!r}")


def _ishares_objects(value: Any) -> Iterator[dict]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _ishares_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _ishares_objects(child)


def parse_ishares_pe(html: str) -> tuple[Decimal, date]:
    """Extract ONLY the labeled portfolio P/E and its own observation date.

    CH pages use legacy product-data-item markup; US pages embed public React
    component data. Prefer the displayed formattedValue to match manual rows.
    Do not use NAV dates or generic regex matches for unrelated numeric fields.
    The ETF provider's P/E methodology differs between US and CH sites; these
    are the ratios of the exact products originally used in the workbook.
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates: set[tuple[Decimal, date]] = set()

    def add(value: Any, as_of: Any) -> None:
        try:
            number = Decimal(str(value).strip().replace(",", ""))
        except InvalidOperation as exc:
            raise ValueError(f"Missing or invalid iShares P/E: {value!r}") from exc
        if not number.is_finite() or number <= 0 or number > 1000:
            raise ValueError(f"Invalid iShares P/E: {value!r}")
        candidates.add((number, _ishares_date(as_of)))

    for field in soup.select(".product-data-item.col-priceEarnings"):
        caption = field.select_one(".caption")
        value = field.select_one(".data")
        as_of = field.select_one(".as-of-date")
        if caption and "P/E Ratio" in caption.get_text(" ", strip=True):
            if value is None or as_of is None:
                raise ValueError("iShares P/E field is missing its value or date")
            add(value.get_text(" ", strip=True), as_of.get_text(" ", strip=True))

    for component in soup.select("walrus-render-on-client[componentprops]"):
        try:
            payload = json.loads(component["componentprops"])
        except (json.JSONDecodeError, TypeError):
            continue
        for field in _ishares_objects(payload):
            if (
                field.get("name") == "priceEarnings"
                and field.get("label") == "P/E Ratio"
                and field.get("visible", True)
            ):
                add(
                    field.get("formattedValue"),
                    field.get("asOfDate") or field.get("formattedAsOfDate"),
                )

    if not candidates:
        raise ValueError("No labeled iShares P/E ratio found; page structure may have changed")
    if len(candidates) != 1:
        raise ValueError(f"Conflicting iShares P/E values: {candidates!r}")
    return candidates.pop()


def fetch_ishares_pe(client: HttpClient, column: str) -> Observation:
    response = client.get(ISHARES_URLS[column])
    response.raise_for_status()
    value, as_of = parse_ishares_pe(response.text)
    return Observation(value, as_of.isoformat(), ISHARES_URLS[column], "Provider-reported ETF P/E")


NYFED_PAGES = {
    "EFFR": "https://www.newyorkfed.org/markets/reference-rates/effr",
    "SOFR": "https://www.newyorkfed.org/markets/reference-rates/sofr",
}
NYFED_API = "https://markets.newyorkfed.org/api/rates/all/latest.json"
FRED_SERIES = {"10y-US": "DGS10", "30y-US": "DGS30", "Baa premium": "BAA10Y"}
FRED_PAGES = {
    column: f"https://fred.stlouisfed.org/series/{series}" for column, series in FRED_SERIES.items()
}
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
SHIBOR_PAGE = "https://data.eastmoney.com/shibor/default.html"
SHIBOR_JS = "https://data.eastmoney.com/newstatic/js/shibor/default.js"
SHIBOR_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
SNB_PAGE = "https://www.snb.ch/en/the-snb/mandates-goals/statistics/statistics-pub/current_interest_exchange_rates#t00"


def rate_observation(value, observed_on, source, fetched_from=None) -> Observation:
    date.fromisoformat(observed_on)
    return Observation(
        number(value),
        observed_on,
        fetched_from or source,
        "Percentage points; original page: " + source,
    )


def fetch_nyfed(client: HttpClient, column: str | None = None):
    response = client.get(NYFED_API)
    response.raise_for_status()
    rows = response.json()["refRates"]
    result = {}
    for rate, source in NYFED_PAGES.items():
        if column is not None and rate != column:
            continue
        matches = [
            row for row in rows if row.get("type") == rate and row.get("percentRate") is not None
        ]
        latest = max(matches, key=lambda row: row["effectiveDate"])
        result[rate] = rate_observation(
            latest["percentRate"], latest["effectiveDate"], source, str(response.url)
        )
    return result


def parse_fred_html(html: str, source: str):
    """Select latest numeric observation, skipping the '.' used for holidays."""
    soup = BeautifulSoup(html, "html.parser")
    units = soup.select_one(".series-meta-value-units")
    if not units or units.get_text(strip=True) != "Percent":
        raise ValueError(f"Missing or changed FRED units: {source}")
    observations = []
    for row in soup.select("#recent-obs tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        date_match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}):", cells[0].get_text(strip=True))
        raw_value = cells[1].get_text(strip=True)
        if date_match and re.fullmatch(r"[+-]?\d+(?:\.\d+)?", raw_value):
            observations.append(rate_observation(raw_value, date_match[1], source))
    if not observations:
        raise ValueError(f"FRED recent-observations table has no numeric observations: {source}")
    return max(observations, key=lambda item: item.as_of)


def fetch_fred(client: HttpClient, column: str):
    """Download the source's own CSV; HTML is a format/network fallback."""
    source, series = FRED_PAGES[column], FRED_SERIES[column]
    # FRED currently times out for several custom/browser User-Agent strings;
    # the native httpx identifier works for its public downloadable CSV.
    headers = {"User-Agent": f"python-httpx/{httpx.__version__}"}
    try:
        response = client.get(FRED_CSV, params={"id": series}, headers=headers)
        response.raise_for_status()
        rows = csv.DictReader(io.StringIO(response.text))
        observations = [
            rate_observation(row[series], row["observation_date"], source, str(response.url))
            for row in rows
            if row.get(series) not in (None, "", ".")
        ]
        if not observations:
            raise ValueError(f"No numeric observations in FRED CSV for {series}")
        return max(observations, key=lambda item: item.as_of)
    except (ValueError, KeyError, httpx.HTTPError):
        response = client.get(source, headers=headers)
        response.raise_for_status()
        return parse_fred_html(response.text, source)


def fetch_shibor(client: HttpClient):
    # Parameters come from SHIBOR_JS. Crucially, indicator 001 means overnight;
    # the report also contains 1-week through 1-year rates in no guaranteed order.
    response = client.get(
        SHIBOR_API,
        params={
            "reportName": "RPT_IMP_INTRESTRATEN",
            "columns": "REPORT_DATE,REPORT_PERIOD,IR_RATE,INDICATOR_ID,LATEST_RECORD,MARKET_CODE,CURRENCY_CODE",
            "filter": '(LATEST_RECORD=1)(MARKET_CODE="001")',
            "pageNumber": 1,
            "pageSize": 100,
            "sortTypes": 1,
            "sortColumns": "INDICATOR_ID",
            "source": "WEB",
            "client": "WEB",
        },
        headers={"Referer": SHIBOR_PAGE},
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise ValueError(f"Eastmoney returned an error: {payload.get('message')}")
    rows = [
        row
        for row in payload["result"]["data"]
        if row.get("MARKET_CODE") == "001"
        and row.get("CURRENCY_CODE") == "CNY"
        and row.get("INDICATOR_ID") == "001"
        and row.get("IR_RATE") is not None
    ]
    latest = max(rows, key=lambda row: row["REPORT_DATE"])
    if "O/N" not in latest["REPORT_PERIOD"]:
        raise ValueError("Unexpected SHIBOR tenor returned by Eastmoney")
    return rate_observation(
        latest["IR_RATE"], latest["REPORT_DATE"][:10], SHIBOR_PAGE, str(response.url)
    )


def parse_snb_html(html: str, column: str | None = None):
    soup = BeautifulSoup(html, "html.parser")
    desired = {"SARON": "SARON-overnight", "Yield on Swiss Confederation bonds": "10y-swiss"}
    if column is not None:
        desired = {label: key for label, key in desired.items() if key == column}
    results = {}
    for block in soup.select(".rates-values-item"):
        heading = block.select_one(".heading")
        if heading is None or heading.get_text(strip=True) not in desired:
            continue
        label = heading.get_text(strip=True)
        # Read this card alone so digits elsewhere on the page cannot be mixed in.
        raw = block.get_text(" ", strip=True).replace("−", "-")
        value_match = re.search(r"([+-]?\d+(?:\.\d+)?)\s*%", raw)
        date_match = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", raw)
        if not value_match or not date_match:
            raise ValueError(f"Missing SNB rate/date for {label}")
        if label == "Yield on Swiss Confederation bonds" and "10-year" not in raw:
            raise ValueError("Unexpected SNB bond maturity")
        results[desired[label]] = rate_observation(
            value_match[1],
            datetime.strptime(date_match[1], "%d.%m.%Y").date().isoformat(),
            SNB_PAGE,
        )
    if set(results) != set(desired.values()):
        raise ValueError("SNB rate cards missing or changed")
    return results


def fetch_snb(client: HttpClient, column: str | None = None):
    response = client.get(SNB_PAGE)
    response.raise_for_status()
    return parse_snb_html(response.text, column)


ORIGINAL_QUOTE_URLS = {
    "10y-China": "https://www.wsj.com/market-data/quotes/bond/BX/AMBMKRM-10Y",
    "10y-Germany": "https://www.marketwatch.com/investing/bond/tmbmkde-10y?countrycode=bx",
    "10y-France": "https://www.marketwatch.com/investing/bond/tmbmkfr-10y?countrycode=bx",
    "10y-Italy": "https://www.marketwatch.com/investing/bond/tmbmkit-10y?countrycode=bx",
    # No Japan URL was specified in the source notes; same provider's 10Y benchmark.
    "10y-Japan": "https://www.marketwatch.com/investing/bond/tmbmkjp-10y?countrycode=bx",
    "USD index": "https://www.marketwatch.com/investing/index/dxy",
    "WTI($/barrel)": "https://www.marketwatch.com/investing/future/cl.1",
    "Natural Gas($/MMBtu)": "https://www.marketwatch.com/investing/future/ng.1",
    "HRC($/20short tons )": "https://www.marketwatch.com/investing/future/hrn00",
    "silver($/toz)": "https://www.marketwatch.com/investing/future/si.1",
    "wheat(¢/bu)": "https://www.marketwatch.com/investing/future/w.1",
}

TE_QUOTES = {
    "10y-China": (
        "https://tradingeconomics.com/china/government-bond-yield",
        "GCNY10YR:GOV",
        "Bond",
    ),
    "10y-Germany": (
        "https://tradingeconomics.com/germany/government-bond-yield",
        "GDBR10:IND",
        "Bond",
    ),
    "10y-France": (
        "https://tradingeconomics.com/france/government-bond-yield",
        "GFRN10:IND",
        "Bond",
    ),
    "10y-Italy": (
        "https://tradingeconomics.com/italy/government-bond-yield",
        "GBTPGR10:IND",
        "Bond",
    ),
    "10y-Japan": ("https://tradingeconomics.com/japan/government-bond-yield", "GJGB10:IND", "Bond"),
    "USD index": ("https://tradingeconomics.com/united-states/currency", "DXY:CUR", "Currency"),
}

FT_QUOTES = {
    "WTI($/barrel)": (
        "https://markets.ft.com/data/commodities/tearsheet/summary?c=WTI+Crude+Oil",
        "CL.1:NYM",
        "USD",
    ),
    "Natural Gas($/MMBtu)": (
        "https://markets.ft.com/data/commodities/tearsheet/summary?c=Natural+Gas",
        "US@NG.1:NYM",
        "USD",
    ),
    "silver($/toz)": (
        "https://markets.ft.com/data/commodities/tearsheet/summary?c=Silver+5000oz",
        "US@SI.1:CMX",
        "USD",
    ),
    "wheat(¢/bu)": (
        "https://markets.ft.com/data/commodities/tearsheet/summary?c=Wheat",
        "Wc1:CBT",
        "USc",
    ),
}

HRC_TRADINGVIEW_URL = "https://www.tradingview.com/symbols/COMEX-HRC1!/"


def quote_decimal(text: str) -> Decimal:
    cleaned = text.strip().replace(",", "").replace("\N{MINUS SIGN}", "-")
    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid quote: {text!r}") from exc
    if not value.is_finite():
        raise ValueError(f"Non-finite quote: {text!r}")
    return value


def parse_tradingeconomics_quote(html: str, url: str, symbol: str, asset_type: str) -> Observation:
    soup = BeautifulSoup(html, "html.parser")
    match = re.search(r"TEChartsMeta\s*=\s*(\[.*?\]);", html, re.DOTALL)
    if not match:
        raise ValueError("Trading Economics quote metadata is missing")
    metadata = json.loads(match.group(1), parse_float=Decimal)
    matching = [
        item for item in metadata if item.get("symbol") == symbol and item.get("type") == asset_type
    ]
    if len(matching) != 1:
        raise ValueError(f"Trading Economics did not identify {symbol} ({asset_type})")
    # Never substitute a price in a sidebar or an economic forecast.
    main = soup.select_one("#market_last")
    if main is None:
        raise ValueError("Trading Economics main quote is missing")
    value = quote_decimal(main.get_text(strip=True))
    # TELastUpdate is assigned a generic default before the instrument's assignment.
    # The LAST assignment is the timestamp of this HTML/chart snapshot, not a trade timestamp.
    dates = re.findall(r"TELastUpdate\s*=\s*['\"](\d{12,14})['\"]", html)
    if not dates:
        raise ValueError("Trading Economics quote snapshot timestamp is missing")
    stamp = dates[-1]
    if matching[0].get("timezone") != "UTC":
        raise ValueError("Trading Economics quote timezone changed")
    snapshot = datetime.strptime(
        stamp, "%Y%m%d%H%M%S" if len(stamp) == 14 else "%Y%m%d%H%M"
    ).replace(tzinfo=UTC)
    if asset_type == "Bond":
        note = "Trading Economics OTC 10-year government benchmark yield, percent; quote provider may differ from MarketWatch/WSJ. As-of is the page/chart snapshot timestamp."
    else:
        note = "Trading Economics DXY dollar index (six-currency basket), not dollar index futures. As-of is the page/chart snapshot timestamp."
    return Observation(value, snapshot.isoformat(), url, note)


def fetch_tradingeconomics_quote(client: HttpClient, column: str) -> Observation:
    url, symbol, asset_type = TE_QUOTES[column]
    response = client.get(url)
    response.raise_for_status()
    return parse_tradingeconomics_quote(response.text, url, symbol, asset_type)


def parse_ft_quote(html: str, url: str, symbol: str, currency: str) -> Observation:
    soup = BeautifulSoup(html, "html.parser")
    identified = False
    for node in soup.select(".mod-tearsheet-add-to-watchlist[data-mod-config]"):
        config = json.loads(node["data-mod-config"])
        if config.get("symbol") == symbol and config.get("assetClass") == "Commodity":
            identified = True
    if not identified:
        raise ValueError(f"FT commodity page did not identify expected contract {symbol}")
    quote = soup.select_one(".mod-tearsheet-overview__quote")
    if quote is None:
        raise ValueError("FT main quote is missing")
    values = []
    for item in quote.select("li"):
        label = item.select_one(".mod-ui-data-list__label")
        value = item.select_one(".mod-ui-data-list__value")
        if label and value and label.get_text(strip=True) == f"Price ({currency})":
            values.append(quote_decimal(value.get_text(strip=True)))
    if len(values) != 1:
        raise ValueError("FT main price or expected currency is missing")
    stamp = quote.select_one(".mod-disclaimer")
    match = re.search(
        r"as of ([A-Za-z]{3} \d{1,2} \d{4} \d{2}:\d{2}) (BST|GMT)\.",
        stamp.get_text(" ", strip=True) if stamp else "",
    )
    if not match:
        raise ValueError("FT quote timestamp is missing or has changed format")
    dt = datetime.strptime(match.group(1), "%b %d %Y %H:%M").replace(
        tzinfo=ZoneInfo("Europe/London")
    )
    if dt.tzname() != match.group(2):
        raise ValueError("FT quote timezone does not match its date")
    return Observation(
        values[0],
        dt.isoformat(),
        url,
        f"FT/LSEG {symbol} front-month futures; displayed prices are rounded to two decimals. {stamp.get_text(' ', strip=True)}",
    )


def fetch_ft_quote(client: HttpClient, column: str) -> Observation:
    url, symbol, currency = FT_QUOTES[column]
    response = client.get(url)
    response.raise_for_status()
    return parse_ft_quote(response.text, url, symbol, currency)


def parse_tradingview_hrc(html: str, url: str = HRC_TRADINGVIEW_URL) -> Observation:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []
    for node in soup.find_all("script", type="application/prs.init-data+json"):
        payload = json.loads(node.get_text(), parse_float=Decimal)
        for layer in payload.values():
            if not isinstance(layer, dict):
                continue
            symbol = layer.get("data", {}).get("symbol", {})
            if symbol.get("pro_symbol") == "COMEX:HRC1!" and symbol.get("daily_bar"):
                candidates.append(symbol)
    if len(candidates) != 1:
        raise ValueError("TradingView expected one HRC continuous futures quote with timestamp")
    symbol = candidates[0]
    if (
        symbol.get("currency") != "USD"
        or symbol.get("unit_id") != "STN"
        or symbol.get("pointvalue") != 20
    ):
        raise ValueError("TradingView HRC currency, short-ton unit or contract size changed")
    if not re.fullmatch(r"HRC[FGHJKMNQUVXZ]\d{4}", symbol.get("front_contract", "")):
        raise ValueError("TradingView HRC front-contract identifier missing")
    bar = symbol["daily_bar"]
    value = quote_decimal(str(bar["close"]))
    updated = datetime.fromtimestamp(float(bar["data_update_time"]), tz=UTC)
    return Observation(
        value,
        updated.isoformat(),
        url,
        f"TradingView HRC1! front contract {symbol['front_contract']}; latest supplied daily-bar price and data-update time. USD per short ton; 20 short tons is contract size, not price unit. Provider continuous-contract roll timing may differ.",
    )


def fetch_tradingview_hrc(client: HttpClient) -> Observation:
    response = client.get(HRC_TRADINGVIEW_URL)
    response.raise_for_status()
    return parse_tradingview_hrc(response.text)


def build_tasks(client: HttpClient) -> dict[str, Callable[[], Observation]]:
    tasks: dict[str, Callable[[], Observation]] = {
        column: (lambda column=column: fetch_ishares_pe(client, column)) for column in ISHARES_URLS
    }
    for column in NYFED_PAGES:
        tasks[column] = lambda column=column: fetch_nyfed(client, column)[column]
    for column in FRED_SERIES:
        tasks[column] = lambda column=column: fetch_fred(client, column)
    for column in ("10y-swiss", "SARON-overnight"):
        tasks[column] = lambda column=column: fetch_snb(client, column)[column]
    for column in ("USD/CNY", "CHF/USD"):
        tasks[column] = lambda column=column: fetch_wise(client, column)
    tasks["SHIBOR-overnight"] = lambda: fetch_shibor(client)
    tasks["BTC($)"] = lambda: fetch_kraken(client)
    tasks["gold($/toz)"] = lambda: fetch_gold(client)
    tasks["copper($/metric tonne)"] = lambda: fetch_copper(client)
    for column in TE_QUOTES:
        tasks[column] = lambda column=column: fetch_tradingeconomics_quote(client, column)
    for column in FT_QUOTES:
        tasks[column] = lambda column=column: fetch_ft_quote(client, column)
    tasks["HRC($/20short tons )"] = lambda: fetch_tradingview_hrc(client)
    if set(tasks) != set(COLUMNS) - {"date", "gold-silver-ratio"}:
        raise RuntimeError("Scraper registry does not cover the CSV columns")
    return tasks


def validate_observation(column: str, observation: Observation, today: date, max_age: int) -> None:
    value = number(observation.value)
    as_of = date.fromisoformat(observation.as_of[:10])
    age = (today - as_of).days
    if age < -1 or age > max_age:
        raise SourceError(
            f"Source date {as_of} is outside the allowed age (−1–{max_age} days; 1 day timezone tolerance)"
        )
    rates = set(COLUMNS[6:17]) | {"10y-swiss", "SARON-overnight"}
    if column in rates:
        if not Decimal("-20") <= value <= Decimal("100"):
            raise SourceError(f"Implausible percentage-point rate: {value}")
    elif value <= 0:
        raise SourceError(f"Expected a positive price or ratio, got {value}")


def csv_layout(original: bytes) -> tuple[list[str], int, str, bool]:
    """Locate the insertion point without rewriting any historical bytes or notes."""
    bom = original.startswith(b"\xef\xbb\xbf")
    text = original.decode("utf-8-sig")
    lines = text.splitlines(keepends=True)
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    header = None
    insertion = None
    for row in reader:
        if row and row[0] == "date":
            if header is not None or len(row) != len(COLUMNS) or set(row) != set(COLUMNS):
                raise ValueError("CSV header does not match the expected 32 unique columns")
            header = row
            insertion = reader.line_num
        elif header is not None and row and re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", row[0]):
            datetime.strptime(row[0], "%Y/%m/%d")
            if len(row) != len(header):
                raise ValueError(
                    f"Observation on line {reader.line_num} has the wrong column count"
                )
            insertion = reader.line_num
    if header is None or insertion is None:
        raise ValueError("Could not find the CSV date header")
    offset = len("".join(lines[:insertion]).encode("utf-8")) + (3 if bom else 0)
    newline = "\r\n" if "\r\n" in text else "\n"
    return header, offset, newline, bom


def row_bytes(
    header: list[str], observations: dict[str, Observation], today: date, newline: str
) -> bytes:
    row = {column: format(item.value, "f") for column, item in observations.items()}
    row["date"] = f"{today.year}/{today.month}/{today.day}"
    stream = io.StringIO(newline="")
    csv.writer(stream, lineterminator=newline).writerow([row.get(column, "") for column in header])
    return stream.getvalue().encode("utf-8")


def append_snapshot(
    path: Path, observations: dict[str, Observation], today: date, run_id: str
) -> Path:
    with FileLock(str(path) + ".lock", timeout=30):
        original = path.read_bytes()
        header, offset, newline, _ = csv_layout(original)
        prefix = original[:offset]
        if prefix and not prefix.endswith((b"\n", b"\r")):
            prefix += newline.encode()
        updated = prefix + row_bytes(header, observations, today, newline) + original[offset:]
        backup_dir = path.parent / ".observations" / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup = backup_dir / f"{path.stem}-{run_id}.csv"
        with backup.open("xb") as stream:
            stream.write(original)
        # Atomic replacement avoids a half-written CSV if the process is interrupted.
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(updated)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, path.stat().st_mode & 0o777)
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return backup


def collect(
    client: HttpClient, today: date, max_age: int
) -> tuple[dict[str, Observation], dict[str, str]]:
    tasks = build_tasks(client)
    observations: dict[str, Observation] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(task): column for column, task in tasks.items()}
        for future in as_completed(futures):
            column = futures[future]
            try:
                observation = future.result()
                validate_observation(column, observation, today, max_age)
                observations[column] = observation
                print(
                    f"  {column}: {observation.value} (as of {observation.as_of})", file=sys.stderr
                )
            except Exception as exc:
                # Isolate provider/schema failures to one cell. Record the exception
                # type rather than silently treating malformed data as a valid value.
                # KeyboardInterrupt and SystemExit still propagate.
                errors[column] = f"{type(exc).__name__}: {exc}"
                print(f"  {column}: UNAVAILABLE — {exc}", file=sys.stderr)
    if "gold($/toz)" in observations and "silver($/toz)" in observations:
        gold, silver = observations["gold($/toz)"], observations["silver($/toz)"]
        observations["gold-silver-ratio"] = Observation(
            (gold.value / silver.value).quantize(Decimal("0.00000001")),
            min(gold.as_of[:10], silver.as_of[:10]),
            f"{gold.source_url} ; {silver.source_url}",
            "Gold spot / front-month silver futures, matching the existing CSV convention",
        )
    else:
        errors["gold-silver-ratio"] = "Requires both gold and silver observations"
    return observations, errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--csv", type=Path, default=DEFAULT_CSV, help="Existing CSV (default: beside this script)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Fetch and print the row without modifying the CSV"
    )
    parser.add_argument(
        "--strict", action="store_true", help="Do not append if any indicator is unavailable"
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=10,
        help="Reject source observations older than this (default: 10)",
    )
    parser.add_argument(
        "--timeout", type=float, default=25, help="HTTP timeout in seconds (default: 25)"
    )
    args = parser.parse_args(argv)
    if args.max_age_days < 0 or args.timeout <= 0:
        parser.error("--max-age-days must be non-negative and --timeout must be positive")
    path = args.csv.expanduser().resolve()
    try:
        header, _, newline, _ = csv_layout(path.read_bytes())
    except (OSError, ValueError, csv.Error) as exc:
        print(f"Cannot update {path}: {exc}", file=sys.stderr)
        return 1
    started = datetime.now().astimezone()
    today = started.date()
    run_id = started.strftime("%Y%m%dT%H%M%S%f%z")
    print(f"Fetching latest observations for {today}…", file=sys.stderr)
    client = HttpClient(args.timeout)
    try:
        observations, errors = collect(client, today, args.max_age_days)
    finally:
        client.close()
    print(row_bytes(header, observations, today, newline).decode().rstrip())
    report = {
        "started_at": started.isoformat(),
        "csv": str(path),
        "date": today.isoformat(),
        "dry_run": args.dry_run,
        "strict": args.strict,
        "appended": False,
        "original_sources": ORIGINAL_SOURCES,
        "observations": {column: asdict(item) for column, item in observations.items()},
        "errors": errors,
    }
    report_dir = path.parent / ".observations" / "runs"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{path.stem}-{run_id}.json"
    # Persist the source evidence before modifying the CSV.
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
    )
    if not observations or (args.strict and errors):
        print(
            f"No row appended: {len(errors)} unavailable indicators. Report: {report_path}",
            file=sys.stderr,
        )
        return 1
    if not args.dry_run:
        report["backup"] = str(append_snapshot(path, observations, today, run_id))
        report["appended"] = True
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
        )
    action = "Previewed" if args.dry_run else "Appended"
    print(
        f"{action} one row: {len(observations)}/31 indicators. Report: {report_path}",
        file=sys.stderr,
    )
    # A partial append is visible to both humans and schedulers.
    return 2 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
