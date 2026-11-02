"""Calibration analysis of resolved binary prediction markets.

For each market, the probability is sampled at fractions of its lifetime
(the checkpoints), then scored against the YES/NO resolution. Writes an
interactive report to out/<platform>.html.

Usage: uv run calibration.py --platform manifold --min-traders 15 --min-days 30 --by traders --by volume
"""

import argparse
import datetime
import json
import os

import duckdb

from platforms import PLATFORMS

SLICES = sorted({name for p in PLATFORMS.values() for name in p.slices})

# Probabilities are clipped before taking logs so a market sitting at 0% or
# 100% doesn't produce an infinite log loss.
EPS = 0.001
Z = 1.96


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--platform", choices=PLATFORMS, required=True)
    p.add_argument("--data", help="directory with the platform's Parquet files (default: data/<platform>)")
    p.add_argument("--out", help="report path (default: out/<platform>.html)")
    p.add_argument("--checkpoints", default="0.1,0.25,0.5,0.75,0.9",
                   help="comma-separated fractions of market lifetime")
    p.add_argument("--day-checkpoints", default="7,30,90,180,365",
                   help="comma-separated days since market creation; each only includes markets still open then")
    p.add_argument("--bins", type=int, default=10, help="calibration bins")
    p.add_argument("--min-traders", type=int, default=0, help="minimum unique traders")
    p.add_argument("--min-days", type=float, default=0, help="minimum market lifetime in days")
    p.add_argument("--min-volume", type=float, default=0)
    p.add_argument("--min-liquidity", type=float, default=0, help="Manifold only")
    p.add_argument("--since", type=datetime.date.fromisoformat, help="only markets created on or after this date")
    p.add_argument("--until", type=datetime.date.fromisoformat, help="only markets created before this date")
    p.add_argument("--end", choices=["min", "close", "resolution"], default="min",
                   help="what counts as the end of a market's lifetime (default: earlier of close and resolution)")
    p.add_argument("--require-bet", action="store_true",
                   help="drop checkpoints that come before the market's first trade (always on for Polymarket)")
    p.add_argument("--by", action="append", choices=SLICES, default=[],
                   help="break results down by quantile buckets of this variable (repeatable)")
    p.add_argument("--buckets", type=int, default=4, help="number of quantile buckets for --by")
    args = p.parse_args()

    platform = PLATFORMS[args.platform]
    args.data = args.data or f"data/{args.platform}"
    args.out = args.out or f"out/{args.platform}.html"
    unsupported = [name for name in args.by if name not in platform.slices]
    if unsupported:
        p.error(f"--by {', '.join(unsupported)} is not available for {platform.name}")
    if args.min_liquidity and "liquidity" not in platform.slices:
        p.error(f"--min-liquidity is not available for {platform.name}")
    return args


def rows(con, sql, params=None):
    cur = con.execute(sql, params or [])
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def epoch_ms(d: datetime.date) -> int:
    return int(datetime.datetime.combine(d, datetime.time(), datetime.UTC).timestamp() * 1000)


def build_forecasts(con, args, platform, checkpoints, day_checkpoints):
    filters = []
    params = []
    if args.since:
        filters.append("start_t >= ?")
        params.append(epoch_ms(args.since))
    if args.until:
        filters.append("start_t < ?")
        params.append(epoch_ms(args.until))
    extra = "".join(f" AND {f}" for f in filters)

    platform.setup(con, args.data, args.end)
    con.execute(f"""
        CREATE TEMP TABLE markets AS
        SELECT * FROM market_base
        WHERE end_t > start_t
          AND (end_t - start_t) >= ? * 86400000
          AND traders >= ?
          AND volume >= ?
          AND liquidity >= ?
          {extra}
    """, [args.min_days, args.min_traders, args.min_volume, args.min_liquidity] + params)

    # Lifetime checkpoints depend on when a market resolved, so a market that
    # resolves YES as soon as its event happens is sampled just before that.
    # Day checkpoints are fixed times after creation and avoid this.
    con.execute("""
        CREATE TEMP TABLE forecasts AS
        WITH points AS (
            SELECT m.*, 'lifetime' AS mode, f, (start_t + f * (end_t - start_t))::BIGINT AS t
            FROM markets m, unnest(?::DOUBLE[]) AS cp(f)
            UNION ALL
            SELECT m.*, 'days' AS mode, f, (start_t + f * 86400000)::BIGINT AS t
            FROM markets m, unnest(?::DOUBLE[]) AS cp(f)
            WHERE start_t + f * 86400000 < end_t
        )
        SELECT points.id, points.mode, points.f, points.yes::INT AS y,
               coalesce(b.prob, points.p0) AS p,
               b.t IS NOT NULL AS traded
        FROM points ASOF LEFT JOIN bets b
          ON b.market_id = points.id AND points.t >= b.t
    """, [checkpoints, day_checkpoints])
    if args.require_bet or not platform.has_p0:
        con.execute("DELETE FROM forecasts WHERE NOT traded")


def summarize(con, args, group: str = ""):
    """Per-checkpoint scores, optionally also grouped by a bucket column.

    `group` is a column of the forecasts_g view, joined from markets.
    """
    g = f"{group}, " if group else ""
    base_rate = con.execute("SELECT avg(yes::INT) FROM markets").fetchone()[0]
    # Bins are centred on round probabilities: with 10 bins, "60%" covers 55-65%.
    bins_cte = f"""
        bins AS (
            SELECT {g}mode, f, round(p * {args.bins})::INT AS bin,
                   count(*) AS n, avg(p) AS mean_p, avg(y) AS yes_rate
            FROM forecasts_g GROUP BY ALL
        )"""
    # Murphy decomposition: brier ≈ reliability - resolution + uncertainty.
    # Reliability is the calibration error; resolution is how well forecasts
    # separate YES from NO; uncertainty depends only on the base rate.
    summary = rows(con, f"""
        WITH {bins_cte},
        totals AS (
            SELECT {g}mode, f, count(*) AS n, avg(y) AS yes_rate, avg(p) AS mean_p,
                   avg(power(p - y, 2)) AS brier,
                   -avg(y * ln(greatest(p, {EPS})) + (1 - y) * ln(greatest(1 - p, {EPS}))) AS logloss,
                   1 - avg(power(p - y, 2)) / avg(power(? - y, 2)) AS skill,
                   avg(traded::INT) AS traded_share
            FROM forecasts_g GROUP BY ALL
        ),
        parts AS (
            SELECT {g}mode, f,
                   sum(bins.n * abs(bins.mean_p - bins.yes_rate)) / sum(bins.n) AS ece,
                   sum(bins.n * power(bins.mean_p - bins.yes_rate, 2)) / sum(bins.n) AS reliability,
                   sum(bins.n * power(bins.yes_rate - totals.yes_rate, 2)) / sum(bins.n) AS resolution
            FROM bins JOIN totals USING ({g}mode, f)
            GROUP BY ALL
        )
        SELECT *, yes_rate * (1 - yes_rate) AS uncertainty
        FROM totals JOIN parts USING ({g}mode, f)
        ORDER BY ALL
    """, [base_rate])
    calib = rows(con, f"""
        WITH {bins_cte}
        SELECT *,
               (yes_rate + {Z}^2 / (2 * n)) / (1 + {Z}^2 / n)
                 - {Z} * sqrt(yes_rate * (1 - yes_rate) / n + {Z}^2 / (4 * n * n)) / (1 + {Z}^2 / n) AS lo,
               (yes_rate + {Z}^2 / (2 * n)) / (1 + {Z}^2 / n)
                 + {Z} * sqrt(yes_rate * (1 - yes_rate) / n + {Z}^2 / (4 * n * n)) / (1 + {Z}^2 / n) AS hi
        FROM bins ORDER BY ALL
    """)
    return summary, calib


def slice_by(con, args, platform, name):
    col = platform.slices[name]
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE buckets AS
        SELECT id, ntile({args.buckets}) OVER (ORDER BY {col}, id) AS bucket, {col} AS value
        FROM markets
    """)
    buckets = rows(con, """
        SELECT bucket, count(*) AS markets, min(value) AS lo, max(value) AS hi,
               avg(yes::INT) AS yes_rate
        FROM buckets JOIN markets USING (id) GROUP BY bucket ORDER BY bucket
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW forecasts_g AS
        SELECT forecasts.*, buckets.bucket FROM forecasts JOIN buckets USING (id)
    """)
    summary, calib = summarize(con, args, "bucket")
    con.execute("CREATE OR REPLACE TEMP VIEW forecasts_g AS SELECT * FROM forecasts")
    return {"buckets": buckets, "summary": summary, "calib": calib}


def checkpoint_label(r):
    return f"{r['f']:.0%} through" if r["mode"] == "lifetime" else f"day {r['f']:.0f}"


def print_summary(summary):
    print(f"{'checkpoint':>12} {'n':>7} {'brier':>7} {'logloss':>8} {'skill':>7} {'ece':>7}")
    for r in sorted(summary, key=lambda r: (r["mode"] != "lifetime", r["f"])):
        print(f"{checkpoint_label(r):>12} {r['n']:>7} {r['brier']:>7.4f} {r['logloss']:>8.4f} "
              f"{r['skill']:>7.3f} {r['ece']:>7.4f}")


def main():
    args = parse_args()
    platform = PLATFORMS[args.platform]
    checkpoints = sorted(float(x) for x in args.checkpoints.split(","))
    day_checkpoints = sorted(float(x) for x in args.day_checkpoints.split(","))

    con = duckdb.connect()
    build_forecasts(con, args, platform, checkpoints, day_checkpoints)
    con.execute("CREATE TEMP VIEW forecasts_g AS SELECT * FROM forecasts")

    meta = rows(con, """
        SELECT count(*) AS markets, avg(yes::INT) AS base_rate,
               median(traders) AS median_traders, median(volume) AS median_volume,
               median(end_t - start_t) / 86400000 AS median_days,
               strftime(to_timestamp(min(start_t) / 1000), '%b %Y') AS first_created,
               strftime(to_timestamp(max(start_t) / 1000), '%b %Y') AS last_created
        FROM markets
    """)[0]
    if meta["markets"] == 0:
        raise SystemExit("No markets match these filters.")
    summary, calib = summarize(con, args)
    hist = rows(con, """
        SELECT mode, f, least(floor(p * 50), 49)::INT AS bin, count(*) AS n
        FROM forecasts GROUP BY ALL ORDER BY ALL
    """)
    slices = {name: slice_by(con, args, platform, name) for name in dict.fromkeys(args.by)}

    filters = {
        "min traders": args.min_traders, "min days": args.min_days,
        "min volume": args.min_volume, "min liquidity": args.min_liquidity,
        "since": args.since, "until": args.until, "lifetime end": args.end,
        "require bet": args.require_bet,
    }
    data = {
        "meta": {
            "generated": datetime.datetime.now().isoformat(timespec="seconds"),
            "platform": {"name": platform.name, "currency": platform.currency, "notes": platform.notes,
                         **platform.report_extra(con, args.data)},
            "filters": {k: str(v) for k, v in filters.items() if v not in (None, 0, False)},
            **meta,
            "bins": args.bins, "checkpoints": {"lifetime": checkpoints, "days": day_checkpoints},
        },
        "summary": summary, "calib": calib, "hist": hist, "slices": slices,
    }

    print(f"{meta['markets']:,} markets created {meta['first_created']} to {meta['last_created']}, "
          f"{meta['base_rate']:.1%} resolved YES, median length {meta['median_days']:.0f} days")
    print_summary(summary)
    for name, s in slices.items():
        print(f"\nBrier by {name} bucket, at {', '.join(f'{f:.0%}' for f in checkpoints)} through:")
        for b in s["buckets"]:
            scores = " ".join(f"{r['brier']:.4f}" for r in s["summary"]
                              if r["bucket"] == b["bucket"] and r["mode"] == "lifetime")
            print(f"  {b['lo']:>10.0f} – {b['hi']:<10.0f} ({b['markets']:>5} markets): {scores}")

    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_template.html")
    with open(template_path) as f:
        template = f.read()
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        f.write(template.replace("/*DATA*/null", json.dumps(data, default=str)))
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
