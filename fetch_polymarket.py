"""Fetch the slice of the Polymarket dataset the calibration analysis needs.

Streams from the Hugging Face dump (SII-WANGZJ/Polymarket_data) and the
Polymarket Gamma API, keeping only closed Yes/No markets that are not neg_risk:

  markets  candidate markets from markets.parquet  -> data/polymarket/candidates.parquet
  gamma    closedTime and current outcomePrices     -> data/polymarket/gamma.jsonl
  resolve  decide each market's outcome             -> data/polymarket/markets.parquet
  trades   slim trades for resolved markets         -> data/polymarket/trades/YYYY-MM.parquet
  volume   shares traded per resolved market        -> data/polymarket/volume.parquet

Every step can resume where it stopped. See METHODOLOGY.md for how outcomes
are decided and why market prices are never used as outcomes.

Usage: uv run fetch_polymarket.py [steps ...] [--months 2025-01:2025-03]
"""

import argparse
import concurrent.futures
import datetime
import gzip
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

import duckdb

HF = "https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data/resolve/main"
GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB = "https://clob.polymarket.com/markets"
DATA = "data/polymarket"
CANDIDATES = f"{DATA}/candidates.parquet"
GAMMA_CACHE = f"{DATA}/gamma.jsonl"
MARKETS = f"{DATA}/markets.parquet"
TRADES = f"{DATA}/trades"
VOLUME = f"{DATA}/volume.parquet"
STEPS = ["markets", "gamma", "resolve", "trades", "volume"]

# Gamma silently returns a partial result for much larger id batches.
GAMMA_BATCH = 100
SPOT_CHECK = 200

# Parses a price array like "['1', '0']" or '["0.5", "0.5"]' into DOUBLE[].
PRICES_SQL = "list_transform(regexp_extract_all({}, '[0-9.eE-]+'), lambda x: try_cast(x AS DOUBLE))"


def connect(memory_limit: str):
    os.makedirs("data/tmp", exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET temp_directory = 'data/tmp'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET http_retries = 10")
    return con


class RateLimit:
    """Spaces out requests across threads to at most `per_second`."""

    def __init__(self, per_second: float):
        self.interval = 1 / per_second
        self.next = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            delay = self.next - now
            self.next = max(now, self.next) + self.interval
        if delay > 0:
            time.sleep(delay)


# Published limits (docs.polymarket.com/api-reference/rate-limits) are per IP:
# Gamma /markets 300 per 10s, CLOB general 9,000 per 10s. CLOB /markets/{id}
# returned 429s at 50/s right after a full Gamma run, so it is kept much lower.
GAMMA_LIMIT = RateLimit(15)
CLOB_LIMIT = RateLimit(5)


def get_json(url: str, limit: RateLimit, tries: int = 8):
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip", "User-Agent": "manifold-analysis"})
    for attempt in range(tries):
        limit.wait()
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                return json.loads(body)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if attempt == tries - 1:
                raise
            if e.code == 429:
                time.sleep(max(float(e.headers.get("Retry-After") or 0), 2 ** attempt * 5))
                continue
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            if attempt == tries - 1:
                raise
        time.sleep(2 ** attempt)


def step_markets(con, args):
    con.execute(f"""
        COPY (
            SELECT id, slug, question, event_title, condition_id,
                   epoch_ms(created_at) AS created_ms, epoch_ms(end_date) AS end_ms,
                   volume, outcome_prices AS dump_prices
            FROM '{HF}/markets.parquet'
            WHERE closed = 1 AND answer1 = 'Yes' AND answer2 = 'No' AND neg_risk = 0
            ORDER BY id
        ) TO '{CANDIDATES}' (FORMAT parquet, COMPRESSION zstd)
    """)
    n = con.execute(f"SELECT count(*) FROM '{CANDIDATES}'").fetchone()[0]
    print(f"{n:,} candidate markets -> {CANDIDATES}")


GAMMA_FIELDS = ["closedTime", "outcomePrices", "umaResolutionStatus", "volume"]


def step_gamma(con, args):
    ids = [r[0] for r in con.execute(f"SELECT id FROM '{CANDIDATES}'").fetchall()]
    done = set()
    if os.path.exists(GAMMA_CACHE):
        # Cached rows missing a field (from before it was added) are dropped and fetched again.
        with open(GAMMA_CACHE) as f:
            rows = [json.loads(line) for line in f]
        complete = [r for r in rows if all(k in r for k in GAMMA_FIELDS)]
        if len(complete) < len(rows):
            with open(f"{GAMMA_CACHE}.tmp", "w") as f:
                f.writelines(json.dumps(r) + "\n" for r in complete)
            os.replace(f"{GAMMA_CACHE}.tmp", GAMMA_CACHE)
        done = {r["id"] for r in complete}
    todo = [i for i in ids if i not in done]
    print(f"Gamma: {len(done):,} cached, {len(todo):,} to fetch")
    batches = [todo[i:i + GAMMA_BATCH] for i in range(0, len(todo), GAMMA_BATCH)]

    def fetch(batch):
        query = "&".join(f"id={i}" for i in batch)
        found = {str(m["id"]): m for m in get_json(f"{GAMMA}?{query}&closed=true&limit={len(batch)}", GAMMA_LIMIT) or []}
        # Ids Gamma doesn't return are cached too, so they aren't fetched again.
        return [{"id": i, "found": i in found, **{k: found.get(i, {}).get(k) for k in GAMMA_FIELDS}}
                for i in batch]

    start = time.time()
    with open(GAMMA_CACHE, "a") as out, concurrent.futures.ThreadPoolExecutor(args.workers) as pool:
        for n, rows in enumerate(pool.map(fetch, batches), 1):
            out.writelines(json.dumps(r) + "\n" for r in rows)
            if n % 100 == 0 or n == len(batches):
                out.flush()
                print(f"  {n:,}/{len(batches):,} batches, {time.time() - start:.0f}s", flush=True)


def step_resolve(con, args):
    con.execute(f"""
        CREATE TEMP TABLE resolved AS
        WITH g AS (
            SELECT id, found, umaResolutionStatus AS uma_status, outcomePrices AS gamma_prices,
                   epoch_ms(try_cast(closedTime AS TIMESTAMPTZ)) AS resolved_ms,
                   try_cast(volume AS DOUBLE) AS gamma_volume
            FROM read_json('{GAMMA_CACHE}', columns = {{
                'id': 'VARCHAR', 'found': 'BOOLEAN', 'closedTime': 'VARCHAR',
                'outcomePrices': 'VARCHAR', 'umaResolutionStatus': 'VARCHAR', 'volume': 'VARCHAR'}})
        ),
        parsed AS (
            -- Neither volume is used by the analysis (see the volume step); kept for reference.
            SELECT c.* EXCLUDE (volume), c.volume AS dump_volume, g.gamma_volume,
                   g.found, g.uma_status, g.gamma_prices, g.resolved_ms,
                   {PRICES_SQL.format('c.dump_prices')} AS dp,
                   {PRICES_SQL.format('g.gamma_prices')} AS gp
            FROM '{CANDIDATES}' c LEFT JOIN g USING (id)
        ),
        outcomes AS (
            -- Only an exact 1/0 is a payout; anything else is a price.
            SELECT *,
                   CASE WHEN dp = [1, 0] THEN true WHEN dp = [0, 1] THEN false END AS dump_yes,
                   CASE WHEN uma_status = 'resolved' AND gp = [1, 0] THEN true
                        WHEN uma_status = 'resolved' AND gp = [0, 1] THEN false END AS gamma_yes
            FROM parsed
        )
        SELECT * EXCLUDE (dp, gp, found, dump_yes, gamma_yes),
               CASE WHEN dump_yes IS NOT NULL AND gamma_yes IS NOT NULL AND dump_yes <> gamma_yes THEN NULL
                    ELSE coalesce(dump_yes, gamma_yes) END AS yes,
               CASE WHEN dump_yes IS NOT NULL AND gamma_yes IS NOT NULL AND dump_yes <> gamma_yes THEN NULL
                    WHEN dump_yes IS NOT NULL THEN 'dump'
                    WHEN gamma_yes IS NOT NULL THEN 'gamma' END AS outcome_source,
               CASE WHEN dump_yes IS NOT NULL AND gamma_yes IS NOT NULL AND dump_yes <> gamma_yes
                        THEN 'dump and Gamma disagree'
                    WHEN dump_yes IS NOT NULL OR gamma_yes IS NOT NULL THEN NULL
                    WHEN found IS NULL THEN 'not fetched from Gamma'
                    WHEN NOT found THEN 'not returned by Gamma'
                    WHEN gp = [0.5, 0.5] AND uma_status = 'resolved' THEN 'resolved 50-50'
                    WHEN uma_status IS NULL THEN 'no recorded payout'
                    ELSE 'not settled' END AS excluded
        FROM outcomes
    """)
    print(f"{'outcome / exclusion':<28} {'markets':>9}   by year created")
    rows = con.execute("""
        WITH t AS (
            SELECT coalesce('outcome from ' || outcome_source, 'excluded: ' || excluded) AS k,
                   year(to_timestamp(created_ms / 1000)) AS y, count(*) AS n
            FROM resolved GROUP BY ALL
        )
        SELECT k, sum(n)::BIGINT AS n, string_agg(y || ': ' || n, ', ' ORDER BY y) FROM t GROUP BY k ORDER BY n DESC
    """).fetchall()
    for k, n, years in rows:
        print(f"{k:<28} {n:>9,}   {years}")

    if not args.no_spot_check:
        spot_check(con)
    con.execute(f"COPY (SELECT * FROM resolved ORDER BY id) TO '{MARKETS}' (FORMAT parquet, COMPRESSION zstd)")
    print(f"Wrote {MARKETS}")


def spot_check(con):
    """Compares outcomes with the CLOB API's per-token winner flag."""
    sample = con.execute(f"""
        SELECT id, condition_id, yes, outcome_source FROM resolved
        WHERE outcome_source IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY outcome_source ORDER BY hash(id)) <= {SPOT_CHECK}
    """).fetchall()

    def clob_yes(cid):
        tokens = (get_json(f"{CLOB}/{cid}", CLOB_LIMIT) or {}).get("tokens") or []
        winners = [t.get("outcome") for t in tokens if t.get("winner")]
        return {"Yes": True, "No": False}.get(winners[0]) if len(winners) == 1 else None

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        clob = list(pool.map(clob_yes, [s[1] for s in sample]))
    stats = {}
    bad = []
    for (mid, cid, yes, source), c in zip(sample, clob):
        s = stats.setdefault(source, {"agree": 0, "disagree": 0, "no CLOB winner": 0})
        if c is None:
            s["no CLOB winner"] += 1
        elif c == yes:
            s["agree"] += 1
        else:
            s["disagree"] += 1
            bad.append((mid, cid, yes, c))
    for source, s in sorted(stats.items()):
        print(f"CLOB spot check, outcome from {source}: " + ", ".join(f"{v} {k}" for k, v in s.items()))
    if bad:
        for b in bad:
            print(f"  market {b[0]} ({b[1]}): ours yes={b[2]}, CLOB yes={b[3]}")
        raise SystemExit("Spot check failed: outcomes disagree with the CLOB API.")


def month_range(spec: str | None, con):
    if spec:
        lo, _, hi = spec.partition(":")
        first, last = (datetime.date.fromisoformat(f"{s}-01") for s in (lo, hi or lo))
    else:
        lo, hi = con.execute(f"""
            SELECT min(stats_min_value::BIGINT), max(stats_max_value::BIGINT)
            FROM parquet_metadata('{HF}/quant.parquet') WHERE path_in_schema = 'timestamp'
        """).fetchone()
        first, last = (datetime.datetime.fromtimestamp(t, datetime.UTC).date().replace(day=1) for t in (lo, hi))
    while first <= last:
        nxt = (first + datetime.timedelta(days=32)).replace(day=1)
        yield first, nxt
        first = nxt


def step_trades(con, args):
    os.makedirs(TRADES, exist_ok=True)
    con.execute(f"CREATE TEMP TABLE included AS SELECT id FROM '{MARKETS}' WHERE yes IS NOT NULL")
    for lo, hi in month_range(args.months, con):
        dst = f"{TRADES}/{lo:%Y-%m}.parquet"
        if os.path.exists(dst):
            continue
        start = time.time()
        ts_lo = int(datetime.datetime.combine(lo, datetime.time(), datetime.UTC).timestamp())
        ts_hi = int(datetime.datetime.combine(hi, datetime.time(), datetime.UTC).timestamp())
        # quant.parquet is sorted by timestamp, so this range skips other months' row groups.
        con.execute(f"""
            COPY (
                SELECT q.timestamp::BIGINT * 1000 AS t, q.market_id, q.price,
                       hash(q.maker) AS maker, hash(q.taker) AS taker
                FROM read_parquet('{HF}/quant.parquet') q
                WHERE q.timestamp >= {ts_lo} AND q.timestamp < {ts_hi}
                  AND q.market_id IN (SELECT id FROM included)
                ORDER BY q.market_id, t
            ) TO '{dst}.tmp' (FORMAT parquet, COMPRESSION zstd)
        """)
        os.replace(f"{dst}.tmp", dst)
        n = con.execute(f"SELECT count(*) FROM '{dst}'").fetchone()[0]
        print(f"{dst}: {n:,} trades, {os.path.getsize(dst) / 1e6:.1f} MB, {time.time() - start:.0f}s", flush=True)


def step_volume(con, args):
    """Sums shares traded per market over all of quant.parquet.

    Volume is computed from the trades because the dump and the Gamma API both
    lack it for many traded markets. Like Polymarket's own volume, it counts
    shares (each paying $1), not usd_amount, which is the cash at the Yes price
    and so nearly 0 for trades in long shots. Only market_id and token_amount
    are read.
    """
    if os.path.exists(VOLUME):
        print(f"{VOLUME} exists, skipping")
        return
    start = time.time()
    con.execute(f"""
        COPY (
            SELECT market_id, sum(token_amount) AS volume
            FROM read_parquet('{HF}/quant.parquet')
            WHERE market_id IN (SELECT id FROM '{MARKETS}' WHERE yes IS NOT NULL)
            GROUP BY market_id ORDER BY market_id
        ) TO '{VOLUME}.tmp' (FORMAT parquet, COMPRESSION zstd)
    """)
    os.replace(f"{VOLUME}.tmp", VOLUME)
    n = con.execute(f"SELECT count(*) FROM '{VOLUME}'").fetchone()[0]
    print(f"{VOLUME}: {n:,} markets, {time.time() - start:.0f}s")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("steps", nargs="*", choices=STEPS, help="steps to run (default: all, in order)")
    p.add_argument("--months", help="trades only: YYYY-MM or YYYY-MM:YYYY-MM (default: every month in the dump)")
    p.add_argument("--workers", type=int, default=4, help="parallel Gamma requests")
    p.add_argument("--no-spot-check", action="store_true", help="skip comparing outcomes with the CLOB API")
    p.add_argument("--memory-limit", default="8GB")
    args = p.parse_args()

    os.makedirs(DATA, exist_ok=True)
    con = connect(args.memory_limit)
    for step in STEPS:
        if step in (args.steps or STEPS):
            print(f"== {step}", flush=True)
            globals()[f"step_{step}"](con, args)


if __name__ == "__main__":
    sys.exit(main())
