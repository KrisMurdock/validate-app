# Stock Analyzer

A-share stock screening and strategy backtesting web service.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Copy or download stock data
cp ../jin-ce-zhi-suan/data/quantifydata.duckdb data/
# OR: python scripts/download_stock_data.py --years 15 --workers 8

python server.py
# Open http://localhost:8000/analyzer (screening) or /analyzer/validate (backtesting)
```

## Pages

| URL | Description |
|-----|-------------|
| `/analyzer` | Stock screening with multi-strategy scoring |
| `/analyzer/validate` | Strategy backtesting with random, sequential, grid, and SR-only modes |

## Data

Set `data_provider.duckdb_path` in `config.json` to point to your DuckDB file.
To download fresh data: `python scripts/download_stock_data.py --years 15 --workers 8`
