#!/usr/bin/env python3
import duckdb
import yaml
import pandas as pd
from pathlib import Path
import os

def load_config(path="config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)

def open_ducklake(cfg):
    db_path = cfg["metadata"]["duckdb_file"]
    data_path = f"s3://{cfg['storage']['bucket']}/{cfg['storage']['prefix']}"
    alias = cfg["catalog"]["alias"]

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"""
        CREATE OR REPLACE SECRET minio (
          TYPE S3,
          KEY_ID '{cfg['storage'].get('access_key', 'minioadmin')}',
          SECRET '{cfg['storage'].get('secret_key', 'minioadmin')}',
          ENDPOINT '{cfg['storage']['endpoint'].replace('http://','').replace('https://','')}',
          URL_STYLE '{cfg['storage'].get('url_style','path')}',
          USE_SSL {'true' if cfg['storage'].get('use_ssl', False) else 'false'},
          REGION '{cfg['storage'].get('region','us-east-1')}'
        );
    """)
    con.execute(f"ATTACH 'ducklake:{db_path}' AS {alias} (DATA_PATH '{data_path}');")
    con.execute(f"USE {alias};")
    return con

def open_reference(scale=1):
    local_db = f"tpch-sf{scale}.duckdb"
    if not os.path.exists(local_db):
        raise FileNotFoundError(
            f"{local_db} not found — run bootstrap/load first to download it."
        )
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"ATTACH '{local_db}' AS tpch_ref;")
    return con

def run_query(con, sql, alias=None):
    if alias:
        sql = sql.replace("FROM ", f"FROM {alias}.")
    return con.execute(sql).fetch_df()

def validate_tpch(cfg, scale=1, query_ids=None):
    if not query_ids:
        query_ids = range(1, 23)

    ducklake_con = open_ducklake(cfg)
    ref_con = open_reference(scale)

    results_dir = Path("tpch_validation")
    results_dir.mkdir(exist_ok=True)

    summary = []

    for q in query_ids:
        sql = ducklake_con.execute(
            f"SELECT query FROM tpch_queries() WHERE query_nr={q};"
        ).fetchone()[0]
        print(f"\n[Q{q:02d}] Validating...")

        try:
            ref_con.execute("USE tpch_ref;")
            df_ref = ref_con.execute(sql).fetch_df()
            ducklake_con.execute(f"USE {cfg['catalog']['alias']};")
            df_dl = ducklake_con.execute(sql).fetch_df()

            match, reason = True, ""

            if list(df_ref.columns) != list(df_dl.columns):
                match, reason = False, "Column mismatch"
            elif len(df_ref) != len(df_dl):
                match, reason = False, f"Row count mismatch ({len(df_ref)} vs {len(df_dl)})"
            else:
                try:
                    pd.testing.assert_frame_equal(
                        df_ref.sort_index(axis=1),
                        df_dl.sort_index(axis=1),
                        atol=1e-6,
                        check_dtype=False,
                        check_like=True,
                    )
                except AssertionError:
                    match, reason = False, "Data mismatch"

            summary.append({"query": f"Q{q:02d}", "match": match, "reason": reason})
            print(f"[{'✓' if match else '✗'}] {reason or 'Results match'}")

            if not match:
                df_ref.to_csv(results_dir / f"q{q:02d}_ref.csv", index=False)
                df_dl.to_csv(results_dir / f"q{q:02d}_ducklake.csv", index=False)

        except Exception as e:
            summary.append({"query": f"Q{q:02d}", "match": False, "reason": str(e)})
            print(f"[x] Error executing Q{q}: {e}")

    pd.DataFrame(summary).to_csv(results_dir / "validation_summary.csv", index=False)
    print("\nValidation summary saved to tpch_validation/validation_summary.csv")

def main():
    cfg = load_config()
    validate_tpch(cfg, scale=cfg["tpch"].get("default_scale", 1))

if __name__ == "__main__":
    main()
