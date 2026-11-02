"""Convert the Manifold JSON dumps into slim Parquet files for analysis.

Usage: uv run ingest_manifold.py [--bets manifold_download/*bets*.json]
       [--contracts manifold_download/manifold-contracts-*.json]
"""

import argparse
import glob
import os
import time

import duckdb

BET_COLUMNS = {
    "id": "VARCHAR",
    "contractId": "VARCHAR",
    "answerId": "VARCHAR",
    "userId": "VARCHAR",
    "createdTime": "BIGINT",
    "probBefore": "DOUBLE",
    "probAfter": "DOUBLE",
    "amount": "DOUBLE",
    "shares": "DOUBLE",
    "outcome": "VARCHAR",
    "isRedemption": "BOOLEAN",
    "isCancelled": "BOOLEAN",
    "isFilled": "BOOLEAN",
    "isAnte": "BOOLEAN",
    "limitProb": "DOUBLE",
}

CONTRACT_COLUMNS = {
    "id": "VARCHAR",
    "slug": "VARCHAR",
    "question": "VARCHAR",
    "outcomeType": "VARCHAR",
    "mechanism": "VARCHAR",
    "wasDpm": "BOOLEAN",
    "resolution": "VARCHAR",
    "isResolved": "BOOLEAN",
    "createdTime": "BIGINT",
    "closeTime": "BIGINT",
    "resolutionTime": "BIGINT",
    "uniqueBettorCount": "BIGINT",
    "volume": "DOUBLE",
    "totalLiquidity": "DOUBLE",
    "visibility": "VARCHAR",
    "isRanked": "BOOLEAN",
    "nonPredictive": "BOOLEAN",
    "deleted": "BOOLEAN",
}


def columns_sql(columns: dict[str, str]) -> str:
    return "{" + ", ".join(f"'{k}': '{v}'" for k, v in columns.items()) + "}"


def convert(con, src: str, dst: str, columns: dict[str, str], order_by: str):
    start = time.time()
    print(f"{src} -> {dst} ...", flush=True)
    con.execute(f"""
        COPY (
            SELECT * FROM read_json(
                ?, format = 'array', columns = {columns_sql(columns)},
                maximum_object_size = 1000000000
            )
            ORDER BY {order_by}
        ) TO '{dst}' (FORMAT parquet, COMPRESSION zstd)
    """, [src])
    n = con.execute(f"SELECT count(*) FROM '{dst}'").fetchone()[0]
    print(f"  {n:,} rows in {time.time() - start:.0f}s")


def main():
    contracts = sorted(glob.glob("manifold_download/manifold-contracts-*.json"))
    bets = sorted(glob.glob("manifold_download/*bets*.json"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bets", default=bets[-1] if bets else None)
    parser.add_argument("--contracts", default=contracts[-1] if contracts else None)
    parser.add_argument("--memory-limit", default="8GB")
    args = parser.parse_args()

    os.makedirs("data/manifold", exist_ok=True)
    os.makedirs("data/tmp", exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute("SET temp_directory = 'data/tmp'")
    con.execute("SET preserve_insertion_order = false")

    convert(con, args.contracts, "data/manifold/contracts.parquet", CONTRACT_COLUMNS, "id")
    convert(con, args.bets, "data/manifold/bets.parquet", BET_COLUMNS, "contractId, createdTime")


if __name__ == "__main__":
    main()
