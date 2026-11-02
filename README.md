# Prediction market calibration analysis

This was a project created by Claude at my direction. It's vibecoded data exploration, not serious analysis.

---

Calibration and accuracy of resolved binary prediction markets, measured at fractions of each market's lifetime. Supports Manifold and Polymarket; each platform gets its own report.

The reports built with `--min-traders 15 --min-days 30 --by traders --by volume` are included:
[`out/manifold.html`](out/manifold.html) and [`out/polymarket.html`](out/polymarket.html). They are
self-contained apart from loading Plotly from a CDN; download one and open it in a browser.

## Manifold

The Manifold dumps are too large to include. Download the bets and contracts dumps from
[Manifold's data page](https://docs.manifold.markets/data) (about 1 GB zipped, 7 GB unzipped) and unzip
them into `manifold_download/`. The analysis here used `manifold-dump-bets-04072024.json.zip` and
`manifold-contracts-20240706.json.zip`. Manifold provides them for personal, non-commercial use only.

```sh
uv run ingest_manifold.py   # one-time: JSON dumps -> data/manifold/*.parquet (~1 min)
uv run calibration.py --platform manifold --min-traders 15 --min-days 30 --by traders --by volume
xdg-open out/manifold.html
```

## Polymarket

Data comes from the [SII-WANGZJ/Polymarket_data](https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data) dump on Hugging Face (~280 GB) and Polymarket's Gamma API. `fetch_polymarket.py` streams only what the analysis needs, keeping closed Yes/No markets that aren't `neg_risk` legs of a multi-outcome event:

```sh
uv run fetch_polymarket.py markets gamma resolve   # market metadata and outcomes
uv run fetch_polymarket.py trades --months 2025-01 # try one month first
uv run fetch_polymarket.py trades volume           # all months, then per-market volume
uv run calibration.py --platform polymarket --min-traders 15 --min-days 30 --by traders --by volume
xdg-open out/polymarket.html
```

| step | network | stored |
|---|---|---|
| `markets` | ~0.1 GB of `markets.parquet` columns | `data/polymarket/candidates.parquet` |
| `gamma` | ~5.6k Gamma API requests, ~0.3 GB (resolution time, outcome) | `data/polymarket/gamma.jsonl` |
| `resolve` | ~400 CLOB API requests (spot check) | `data/polymarket/markets.parquet` |
| `trades` | ~10.5 GB: five columns of `quant.parquet` | `data/polymarket/trades/YYYY-MM.parquet` (~0.5 GB) |
| `volume` | ~2.2 GB: `market_id` and `token_amount` of `quant.parquet` | `data/polymarket/volume.parquet` |

In total that's about 13 GB of network and 0.7 GB on disk. DuckDB reads only the needed columns from Hugging Face over HTTP range requests, so no Hugging Face tooling is needed. `quant.parquet` is sorted by time, so each month downloads only its own row groups. Every step resumes where it stopped.

API calls stay under Polymarket's [rate limits](https://docs.polymarket.com/api-reference/rate-limits): Gamma at most 15 requests/s (limit: 300 per 10 s), CLOB at most 5/s (documented limit: 9,000 per 10 s, but `/markets/{id}` returned HTTP 429 at 50/s), and backing off on HTTP 429.

How Polymarket outcomes are decided, and how its data maps onto the Manifold analysis, is in [METHODOLOGY.md](METHODOLOGY.md).

## Analysis

See `uv run calibration.py --help` for all filters (`--min-volume`, `--since`, `--end`, `--require-bet`, `--checkpoints`, ...).

All statistics are computed in DuckDB. The probability at each checkpoint is the
probability after the last trade before that moment (an `ASOF JOIN`). On Manifold, a
checkpoint before any trade uses the market's starting probability; Polymarket markets have
no probability before their first trade, so those checkpoints are dropped.
