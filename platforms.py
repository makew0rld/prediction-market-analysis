"""Per-platform setup for calibration.py.

Each platform turns its own data into two normalized temp objects that the
rest of the analysis works from:

  bets(market_id, t, prob)       every trade, t in epoch ms, prob after the trade
  market_base(id, slug, question, start_t, end_t, yes, traders, volume,
              liquidity, p0, nbets, first_t)
                                 one row per resolved binary market; p0 is the
                                 probability before any trade and first_t the
                                 time of the first trade
"""

from dataclasses import dataclass, field


@dataclass
class Platform:
    name: str
    currency: str
    # Columns of market_base that --by may slice on.
    slices: dict[str, str]
    # Whether p0 is a real probability before the first trade. If not,
    # checkpoints before the first trade are dropped.
    has_p0: bool
    # Shown in the report's methodology section (HTML allowed).
    notes: list[str] = field(default_factory=list)

    def setup(self, con, data: str, end: str):
        raise NotImplementedError

    def report_extra(self, con, data: str) -> dict:
        return {}


class Manifold(Platform):
    END_EXPRS = {
        "min": "least(closeTime, resolutionTime)",
        "close": "closeTime",
        "resolution": "resolutionTime",
    }

    def setup(self, con, data, end):
        con.execute(f"""
            CREATE TEMP VIEW bets AS
            SELECT contractId AS market_id, createdTime AS t, probAfter AS prob
            FROM '{data}/bets.parquet'
            WHERE answerId IS NULL
        """)
        con.execute(f"""
            CREATE TEMP TABLE market_base AS
            WITH base AS (
                SELECT id, slug, question, createdTime AS start_t,
                       {self.END_EXPRS[end]} AS end_t,
                       resolution = 'YES' AS yes,
                       coalesce(uniqueBettorCount, 0) AS traders,
                       coalesce(volume, 0) AS volume,
                       coalesce(totalLiquidity, 0) AS liquidity
                FROM '{data}/contracts.parquet'
                WHERE outcomeType = 'BINARY'
                  AND resolution IN ('YES', 'NO')
                  AND coalesce(visibility, 'public') = 'public'
                  AND NOT coalesce(deleted, false)
            ),
            first_bet AS (
                SELECT contractId, arg_min(probBefore, createdTime) AS p0, count(*) AS nbets,
                       min(createdTime) AS first_t
                FROM '{data}/bets.parquet' WHERE answerId IS NULL GROUP BY contractId
            )
            SELECT base.*, first_bet.p0, first_bet.nbets, first_bet.first_t
            FROM base JOIN first_bet ON first_bet.contractId = base.id
        """)


class Polymarket(Platform):
    # resolved_ms is Gamma's closedTime; the last trade stands in when it's missing.
    END_EXPRS = {
        "min": "least(end_ms, coalesce(resolved_ms, last_t))",
        "close": "end_ms",
        "resolution": "coalesce(resolved_ms, last_t)",
    }

    def setup(self, con, data, end):
        con.execute(f"""
            CREATE TEMP VIEW bets AS
            SELECT market_id, t, price AS prob FROM '{data}/trades/*.parquet'
        """)
        con.execute(f"""
            CREATE TEMP TABLE market_base AS
            WITH stats AS (
                SELECT market_id, arg_min(price, t) AS p0, count(*) AS nbets,
                       min(t) AS first_t, max(t) AS last_t
                FROM '{data}/trades/*.parquet' GROUP BY market_id
            ),
            traders AS (
                SELECT market_id, count(DISTINCT addr) AS traders
                FROM (SELECT market_id, maker AS addr FROM '{data}/trades/*.parquet'
                      UNION ALL
                      SELECT market_id, taker FROM '{data}/trades/*.parquet')
                GROUP BY market_id
            )
            SELECT m.id, m.slug, m.question, m.created_ms AS start_t,
                   {self.END_EXPRS[end]} AS end_t,
                   m.yes, traders.traders, coalesce(v.volume, 0) AS volume, 0 AS liquidity,
                   stats.p0, stats.nbets, stats.first_t
            FROM '{data}/markets.parquet' m
            JOIN stats ON stats.market_id = m.id
            JOIN traders ON traders.market_id = m.id
            LEFT JOIN '{data}/volume.parquet' v ON v.market_id = m.id
            WHERE m.yes IS NOT NULL
        """)

    def report_extra(self, con, data):
        cur = con.execute(f"""
            SELECT coalesce('Outcome from ' || CASE outcome_source WHEN 'dump' THEN 'dataset' WHEN 'gamma' THEN 'Gamma API' END,
                            'Excluded: ' || excluded) AS label,
                   count(*) AS markets
            FROM '{data}/markets.parquet' GROUP BY outcome_source, excluded
            ORDER BY outcome_source NULLS LAST, markets DESC
        """)
        return {"resolution_counts": [dict(zip(("label", "markets"), r)) for r in cur.fetchall()]}


PLATFORMS = {
    "manifold": Manifold(
        name="Manifold",
        currency="Ṁ",
        slices={"traders": "traders", "volume": "volume", "liquidity": "liquidity", "bets": "nbets"},
        has_p0=True,
        notes=[
            "Public binary markets that resolved YES or NO.",
            "The probability at a checkpoint is the market probability after the last bet before it, "
            "or the market's starting probability if nobody had traded yet.",
            "Traders is Manifold's <code>uniqueBettorCount</code>.",
        ],
    ),
    "polymarket": Polymarket(
        name="Polymarket",
        currency="$",
        slices={"traders": "traders", "volume": "volume", "bets": "nbets"},
        has_p0=False,
        notes=[
            "Closed Yes/No markets that settled to a Yes or No payout, excluding <code>neg_risk</code> legs of "
            "multi-outcome events (other multi-market events are included). "
            "The outcome comes from a recorded payout, never from the market's price: the dataset's "
            "<code>outcome_prices</code> when it is exactly 1/0, otherwise Polymarket's Gamma API. Markets "
            "with no recorded payout, including the 2020–22 AMM markets, are excluded. See METHODOLOGY.md.",
            "The probability at a checkpoint is the price of the last trade before it. Polymarket markets have "
            "no probability before their first trade, so checkpoints before it are left out.",
            "Resolution time is the Gamma API's <code>closedTime</code>. Close time is the scheduled "
            "<code>end_date</code>.",
            "Traders counts distinct maker and taker wallets. Volume is shares traded (each pays $1), as Polymarket counts it.",
        ],
    ),
}
