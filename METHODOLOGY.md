# Methodology

## Shared analysis

For each resolved binary market, `calibration.py` samples the market's probability at checkpoints:
fractions of its lifetime, and fixed numbers of days after creation. It scores each sample against
the YES/NO outcome (Brier score, log loss, calibration bins). Everything after the per-platform setup
in `platforms.py` is the same for every platform.

## Polymarket

### Sources

| source | used for |
|---|---|
| Hugging Face [SII-WANGZJ/Polymarket_data](https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data) `markets.parquet` | candidate markets, question, event, creation time, scheduled end, the dump's `outcome_prices` |
| the same dump's `quant.parquet` | trades: time, market, price of the Yes token, maker and taker wallets, shares traded |
| Gamma API (`gamma-api.polymarket.com/markets`) | resolution time (`closedTime`), current `outcomePrices`, `umaResolutionStatus` |
| CLOB API (`clob.polymarket.com/markets/{condition_id}`) | spot check of outcomes (per-token `winner` flag) |

The dump is built only from the exchange contracts' `OrderFilled` events. It has no resolution
events and no resolution times. None of its trade files records who won.

### Which markets

Closed markets whose outcomes are exactly `Yes` and `No`, excluding `neg_risk` markets. Markets like
Over/Under, Up/Down or team-vs-team are binary too, but are left out to keep the question type close
to Manifold's.

`neg_risk` markets are the legs of mutually exclusive multi-outcome events, similar to the answers of
a Manifold multiple-choice market, which the Manifold analysis leaves out. But not every Polymarket
event made of several markets is `neg_risk`, so the remaining set still contains many such events,
for example "Which teams will make the NBA Playoffs?" with one Yes/No market per team. Every option
is included, winners and losers alike. With `--min-traders 15 --min-days 30`, about three quarters of
the analyzed markets share an event with at least one other analyzed market. Options of one event
are not independent, so the report's confidence intervals are somewhat too narrow, and a large event
weighs as much as its number of options.

### Outcomes

The dump's `outcome_prices` is the Gamma API's `outcomePrices`, copied when the dump was made. Gamma
reports a *price* for each outcome until a market settles, then replaces it with the *payout*. So the
field is sometimes a resolution and sometimes only a price. For the 561,385 candidate markets:

| dump `outcome_prices` | markets | meaning |
|---|---:|---|
| exactly `1/0` or `0/1` | 428,798 | a settled payout. The order book's highest bid is 0.999, so a price can't be exactly 1. |
| between 0.99 and 1, e.g. `0.9995/0.0005` | 117,464 | a price: mostly 2026 markets not yet settled when the dump was made, plus the 2020–22 AMM markets' final prices |
| `0.5/0.5` | 14,455 | either a 50-50 payout or a midpoint price |
| anything else | 668 | unknown, including 4 markets at `1/1` |

**A market's price is never used as its outcome.** Doing so would be circular: a market at 99% that
resolved the other way would be scored as correct, so the most confident bins would look perfectly
calibrated by construction.

Each market's outcome is decided like this (`fetch_polymarket.py resolve`):

1. If the dump's value is exactly `1/0` or `0/1`, the outcome comes from the dump.
2. Otherwise, the outcome comes from a fresh Gamma lookup, if Gamma now reports exactly `1/0` or
   `0/1` and `umaResolutionStatus = resolved`.
3. If both exist and disagree, the market is excluded.
4. Every other market is excluded, with the reason recorded in `markets.parquet`:
   - **resolved 50-50**: the payout was split. Manifold's analysis also leaves out resolutions other
     than YES/NO.
   - **not settled**: Gamma still has no payout.
   - **no recorded payout**: mostly 2020–22 markets from Polymarket's earlier AMM. Gamma still shows
     only their final price and the CLOB API doesn't know them. Their payouts exist only on-chain,
     and this project doesn't query the chain.
   - **not returned by Gamma**
5. A sample of 200 outcomes from each source is compared with the CLOB API's `winner` flag. Any
   disagreement stops the fetch.

The report lists how many markets fell into each group. Results with the Gamma data fetched on
2026-11-02:

| group | markets | by year created |
|---|---:|---|
| outcome from the dump | 428,798 | 2022: 32, 2023: 1,440, 2024: 5,191, 2025: 40,980, 2026: 381,155 |
| outcome from Gamma | 114,047 | 2025: 429, 2026: 113,618 |
| excluded: resolved 50-50 | 13,103 | almost all 2026 |
| excluded: not returned by Gamma | 1,965 | almost all 2026 |
| excluded: no recorded payout | 1,824 | 2020–22, plus a handful later |
| excluded: not settled | 1,648 | mostly 2022 |

The dump and Gamma disagreed on no market. The CLOB spot check agreed on 200 of 200 outcomes from
each source.

`quant.parquet` starts in November 2022, and almost all of its trades are from 2024 onward. So the
excluded AMM-era markets would mostly have had no trades to analyze anyway.

### Field mapping to Manifold

| Manifold | Polymarket |
|---|---|
| `createdTime` | `created_at` |
| `closeTime` | `end_date`, the scheduled end |
| `resolutionTime` | Gamma `closedTime`; the last trade's time if it is missing |
| `probAfter` of the last bet | price of the last trade, from `quant.parquet`, as the Yes-token price |
| starting probability | none (see below) |
| `uniqueBettorCount` | distinct maker and taker wallets (hashed when downloaded) |
| bet count | trade count |
| `volume` (mana) | shares traded: sum of `quant.parquet`'s `token_amount` (see below) |
| `totalLiquidity` | not available, so `--min-liquidity` and `--by liquidity` are Manifold-only |

### Volume

Volume is the number of shares traded in each market, summed over every trade in `quant.parquet`
(`token_amount`). Each share pays $1, so this is the dollar volume Polymarket itself reports: for a
sampled market, the sum of `token_amount` was 1,105.88 and Gamma's `volume` 1,105.89.

`quant.parquet`'s `usd_amount` is not used. It is the cash paid at the Yes price, so a trade where
someone buys No at 99.9¢ shows up as a fraction of a cent. Summed, it came to about half of Gamma's
volume for typical markets and around 1% for long shots.

Volume is computed from the trades because neither metadata source is complete. Of the 374,003
resolved markets with trades:

- the dump's `volume` is 0 for 25,382, including 3,029 with 100 or more trades;
- the Gamma API returns no volume at all for 22,963 of those same markets.

A wrong 0 would put a busy market in the lowest bucket of `--by volume` and let it slip past
`--min-volume`. `markets.parquet` keeps both metadata values (`dump_volume`, `gamma_volume`) for
reference.

### Caveats when comparing with Manifold

- **No starting probability.** Polymarket is an order book, and a market has no price until its
  first trade. Checkpoints before the first trade are dropped, which is what `--require-bet` does
  for Manifold.
- **Trade prices, not an AMM probability.** A Polymarket trade can happen at either side of the
  spread, so one trade's price is noisier than Manifold's probability after a bet.
- **Different questions.** The two platforms list different kinds of questions, so a difference in
  scores can reflect the questions as much as the forecasters.
