# Monthly financial observations

## Purpose of the project 

This tiny project collects indicators that the author believes helpful for passive investing. 

For example, **stocks** are measured in a general market PE ratio, which are divided into U.S., exUS-developed markets, emerging markets. Just for reference, we also listed developed-Europe market and the Chinese market.

The **bonds** are evaluated in percentage, which is the annualized yield. We include major countries, and different maturities. 

The **currency** includes USD index and USD/CNY exchange rate. 

We also list the prices for **commodities**. 

For personal reason of the author, some indicators related to the Swiss market are included, including Swiss stocks PE ratio, 10y-swiss bond, SARON rate (overnight interest rate), and the CHF-USD exchange rate.

Note that the `date` indicates the time of running the script `update_data.py` by the user, instead of the time stamp of the indicators. 

For example, running the script on Oct 3, 2026, the PE ratio for US market on the iShare website is 30.16, which is an observation on Oct 1, 2026. 

All data is open and is scrapped from public websites. The sources are all in the script `update_data.py`.

This script is implemented mainly by GPT 5.6 Ultra.

`update_data.py` fetches the latest published values and inserts
one dated row after the last observation in `macro_data.csv`. It keeps
the existing headers, historical rows, blank rows, and source notes intact.
Running it again on the same day adds another snapshot.

## Run

From the parent directory:

```sh
uv sync
uv run update_data.py
```

`uv` creates/uses `.venv`. Dependencies are declared in `pyproject.toml` and
pinned in `uv.lock`. Python 3.12 or later is required. The `.numbers` files are
not used by the updater; every original source URL is embedded in the script.
`numbers-parser` is a development dependency used to extract the supplied
source list, not a runtime requirement.

Useful options:

```sh
# Fetch and preview without changing the CSV (still saves a source report).
uv run update_data.py --dry-run

# Append only when every indicator is available.
uv run update_data.py --strict

# Use another existing CSV with the same column names.
uv run update_data.py --csv /path/to/monthly_observations.csv
```

## Dates and backups

The row date is the computer's local date at the start of the run. Each cell is
the latest available observation from its source, not a monthly average or a
historical reconstruction. Sources update at different times; some quotes are
delayed or are the last daily close. The per-cell dates, actual URLs, fallback
notes, and retrieval errors are saved in `.observations/runs/*.json`.

By default, an unavailable indicator is left blank and reported in the terminal.
It is never replaced with zero or a previous month's value. `--strict` prevents
the append if any indicator is unavailable. A run where every source fails
never appends an empty row. Exit codes are `0` for complete success, `2` for a
partial snapshot or preview, and `1` for failure or a strict-mode refusal.

Observations older than 10 calendar days are rejected. Use `--max-age-days N`
to change that limit. One day of future-date tolerance accommodates source time
zones. The limit checks source observation or snapshot dates; it does not make
delayed quotes live. Trading Economics supplies a page/chart snapshot timestamp,
not a last-trade timestamp.
HTTP requests have timeouts and limited retries. A blocked or redesigned page
produces an explicit error if no usable fallback is available.

Before each append, the original CSV is saved under `.observations/backups/`.
The script locks the file during the update and replaces it atomically, so
simultaneous runs do not lose each other's rows. It does not update the separate
Numbers workbook.

## Sources and measurement conventions


| CSV fields | Source and interpretation |
| --- | --- |
| U.S., exUS-dev, EM, Europe-dev, China, Swiss Stocks | Labeled P/E fields from the exact iShares ETF pages in the source list. Swiss Core SPI is product 264107. These are provider-reported ETF P/E ratios; US and Swiss iShares pages use different earnings methodologies. |
| EFFR, SOFR | New York Fed's official public reference-rate feed. |
| 10y-US, 30y-US, Baa premium | FRED's public CSV downloads for DGS10, DGS30, BAA10Y, with HTML fallback. Holiday gaps are skipped. BAA10Y is the Baa yield minus the 10-year Treasury yield. |
| SHIBOR-overnight | Eastmoney's public data feed used by its SHIBOR page, filtered to CNY overnight. |
| 10y-swiss, SARON-overnight | Separate rate cards on the Swiss National Bank page, including their own dates. |
| USD/CNY, CHF/USD | Wise converter pages' embedded mid-market rates and provider timestamps. Values mean CNY per USD and USD per CHF respectively. |
| China, Germany, France, Italy, Japan 10-year yields | Trading Economics benchmark government-bond yields replace the inaccessible WSJ/MarketWatch pages. The source list did not provide a Japan URL. Providers may report slightly different benchmark quotes. |
| USD index | Trading Economics' DXY index replaces the inaccessible MarketWatch DXY page. |
| WTI, natural gas, silver, wheat | Financial Times quotes for the corresponding continuous front-month futures replace the inaccessible MarketWatch pages. Displayed FT prices are rounded to two decimals. These are futures, despite the original notes sometimes calling them spot prices. |
| HRC | TradingView COMEX:HRC1! US Midwest hot-rolled coil steel continuous futures replaces the inaccessible MarketWatch page. The public snapshot can be the prior daily close, and providers' contract-roll timing may differ. |
| Gold | GoldPrice's public spot feed; Gold-API XAU/USD spot quote as fallback. |
| Copper | Westmetall's dated LME Copper Cash-Settlement table. The original LME page requires dynamic loading and blocks some HTTP clients. No COMEX copper futures are substituted. |
| BTC | Kraken's official public recent-trades feed, including the latest executed BTC/USD trade timestamp. |
| Gold/silver ratio | New row's gold divided by its silver, rounded to eight decimal places, matching the existing calculation. |

Rates and spreads are stored in percentage points: `3.63` means `3.63%`.
Negative and zero interest rates are valid. Currency prices and ratios are
stored as numbers with a decimal point.

copper is quoted in USD per metric tonne  and HRC remains USD per
short ton (20 short tons per contract). 
See [LME copper contract specifications](https://www.lme.com/en/metals/non-ferrous/lme-copper/contract-specifications)
and [CME HRC specifications](https://www.cmegroup.com/markets/metals/ferrous/hrc-steel.contractSpecs.html).

Wheat is US cents per bushel, gold and silver are USD per troy ounce, WTI is
USD per barrel, and natural gas is USD per MMBtu. The gold/silver ratio mixes
spot gold with front-month silver, following the supplied sources.

## Checks

```sh
uv run pytest
uv run ruff check .
uv run update_data.py --dry-run --strict
```

The automated tests use local fixtures or mocked HTTP responses. The last
command checks live source access without changing historical data.
