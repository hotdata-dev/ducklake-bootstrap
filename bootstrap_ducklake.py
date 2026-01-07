#!/usr/bin/env python3
import argparse
import os
import sys
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import urllib.request


import duckdb  # pip install duckdb
import yaml    # pip install pyyaml

# --------------------------
# Config & backend adapters
# --------------------------

@dataclass
class MetadataDuckDB:
    file_path: str

    def attach_sql(self, alias: str, data_path: str) -> str:
        # ATTACH ducklake with DuckDB metadata file
        # Example:
        #   ATTACH 'ducklake:metadata.ducklake' AS alias (DATA_PATH 's3://bucket/prefix/');
        return f"ATTACH 'ducklake:{self.file_path}' AS {alias} (DATA_PATH '{data_path}');"


@dataclass
class StorageMinIO:
    bucket: str
    prefix: str
    endpoint: str
    region: str = "us-east-1"
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    use_ssl: bool = False
    url_style: str = "path"

    def data_path(self) -> str:
        # S3-style path DuckDB understands
        prefix = self.prefix.strip("/")
        if prefix:
            return f"s3://{self.bucket}/{prefix}/"
        return f"s3://{self.bucket}/"

    def create_secret_sql(self, name: str = "minio") -> str:
        # We rely on DuckDB's S3 Secret provider. For MinIO we typically set:
        # ENDPOINT, URL_STYLE, USE_SSL plus creds & region.
        # You can also omit creds here and rely on env or credential_chain.
        parts = [
            "CREATE OR REPLACE SECRET {name} (",
            "  TYPE S3,",
        ]
        if self.access_key and self.secret_key:
            parts.append(f"  KEY_ID '{self.access_key}',")
            parts.append(f"  SECRET '{self.secret_key}',")
        parts.append(f"  ENDPOINT '{self.endpoint.replace('http://','').replace('https://','')}',")
        parts.append(f"  URL_STYLE '{self.url_style}',")
        parts.append(f"  USE_SSL {'true' if self.use_ssl else 'false'},")
        parts.append(f"  REGION '{self.region}'")
        parts.append(");")
        sql = "\n".join(parts).format(name=name)
        return sql


@dataclass
class CatalogConfig:
    alias: str = "my_ducklake"


@dataclass
class TPCHConfig:
    default_scale: int = 1


@dataclass
class AppConfig:
    metadata: MetadataDuckDB
    storage: StorageMinIO
    catalog: CatalogConfig
    tpch: TPCHConfig

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "AppConfig":
        md = d.get("metadata", {})
        st = d.get("storage", {})
        cg = d.get("catalog", {})
        tp = d.get("tpch", {})

        storage = StorageMinIO(
            bucket=st.get("bucket", "ducklake-data"),
            prefix=st.get("prefix", "tpch/"),
            endpoint=st.get("endpoint", "http://localhost:9000"),
            region=st.get("region", "us-east-1"),
            access_key=st.get("access_key", os.getenv("MINIO_ACCESS_KEY")),
            secret_key=st.get("secret_key", os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_SECRET_ACCESS_KEY")),
            use_ssl=bool(st.get("use_ssl", False)),
            url_style=st.get("url_style", "path"),
        )

        # Validate storage credentials
        if not storage.access_key:
            raise ValueError("storage.access_key is required but was not provided in config or environment variables")
        if not storage.secret_key:
            raise ValueError("storage.secret_key is required but was not provided in config or environment variables")

        return AppConfig(
            metadata=MetadataDuckDB(file_path=md.get("duckdb_file", "./metadata.ducklake")),
            storage=storage,
            catalog=CatalogConfig(alias=cg.get("alias", "my_ducklake")),
            tpch=TPCHConfig(default_scale=int(tp.get("default_scale", 1)))
        )


# --------------------------
# DuckDB helpers
# --------------------------

def open_duckdb_for_session(cfg: AppConfig) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(cfg.metadata.file_path) or ".", exist_ok=True)
    con = duckdb.connect(database=cfg.metadata.file_path)

    # Install/load needed extensions. INSTALL is idempotent.
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("INSTALL tpch; LOAD tpch;")
    return con


def ensure_minio_secret(con: duckdb.DuckDBPyConnection, storage: StorageMinIO, secret_name: str = "minio") -> None:
    sql = storage.create_secret_sql(secret_name)
    con.execute(sql)


def attach_ducklake(con: duckdb.DuckDBPyConnection, cfg: AppConfig) -> None:
    # Make sure the metadata dir exists on disk for the DuckDB file path
    md_dir = os.path.dirname(os.path.abspath(cfg.metadata.file_path)) or "."
    os.makedirs(md_dir, exist_ok=True)

    # Configure S3/MinIO access
    ensure_minio_secret(con, cfg.storage)

    # ATTACH ducklake
    data_path = cfg.storage.data_path()
    attach_sql = cfg.metadata.attach_sql(alias=cfg.catalog.alias, data_path=data_path)
    con.execute(attach_sql)
    # Use the attached catalog
    con.execute(f"USE {cfg.catalog.alias};")



def generate_tpch_and_load(con, scale: int, ducklake_alias: str):
    """
    Downloads a pre-generated TPC-H dataset (if missing) and copies it into DuckLake.
    """
    # Determine dataset URL and local file path
    url = f"https://blobs.duckdb.org/data/tpch-sf{scale}.db"
    local_db = f"tpch-sf{scale}.duckdb"

    # Download only if missing
    if not os.path.exists(local_db):
        print(f"[+] Downloading TPC-H scale factor {scale} dataset from {url} ...")
        urllib.request.urlretrieve(url, local_db)
        print(f"[✓] Download complete: {local_db}")
    else:
        print(f"[skip] Using cached dataset: {local_db}")

    # Attach DuckLake and TPC-H databases
    print(f"[+] Attaching TPC-H database...")
    con.execute(f"ATTACH '{local_db}' AS tpch_src;")

    # Copy all tables into DuckLake
    print(f"[+] Copying tables from TPC-H dataset into DuckLake catalog...")
    con.execute(f"COPY FROM DATABASE tpch_src TO {ducklake_alias};")

    # Clean up
    con.execute("DETACH DATABASE tpch_src;")
    print("[✓] TPC-H dataset successfully loaded into DuckLake.")


# --------------------------
# CLI
# --------------------------

def load_config(path: str) -> AppConfig:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return AppConfig.from_dict(data)


def cmd_attach(args):
    cfg = load_config(args.config)
    con = open_duckdb_for_session(cfg)
    attach_ducklake(con, cfg)
    # Show summary
    data_path = cfg.storage.data_path()
    print(f"Attached DuckLake catalog '{cfg.catalog.alias}'")
    print(f"  metadata: {cfg.metadata.file_path}")
    print(f"  data_path: {data_path}")
    # Confirm we can create a tiny table
    con.execute("CREATE TABLE IF NOT EXISTS bootstrap_check(x INTEGER);")
    print("Sanity: created table 'bootstrap_check' in DuckLake catalog")


def cmd_load_tpch(args):
    cfg = load_config(args.config)
    con = open_duckdb_for_session(cfg)
    attach_ducklake(con, cfg)
    scale = args.scale or cfg.tpch.default_scale
    generate_tpch_and_load(con, scale, cfg.catalog.alias)
    # Count a couple of tables
    for t in ["region", "nation", "customer", "orders", "lineitem"]:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {t};").fetchone()[0]
            print(f"{t:>9}: {cnt:,}")
        except Exception as e:
            print(f"{t:>9}: (not found)")
    print("Done.")


def cmd_init_config(args):
    target = args.path or "config.yaml"
    if os.path.exists(target) and not args.force:
        print(f"{target} already exists (use --force to overwrite)", file=sys.stderr)
        return        
    with open(target, "w") as f:
        f.write("""metadata:\n  duckdb_file: "./metadata.ducklake"\n\nstorage:\n  type: "minio"\n  bucket: "ducklake-data"\n  prefix: "tpch/"\n  endpoint: "http://localhost:9000"\n  region: "us-east-1"\n  use_ssl: false\n  url_style: "path"\n\ncatalog:\n  alias: "my_ducklake"\n\ntpch:\n  default_scale: 1\n""")
    print(f"Wrote {target}")


def main():
    parser = argparse.ArgumentParser(description="DuckLake bootstrap CLI (DuckDB metadata + MinIO data)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_attach = sub.add_parser("attach", help="Attach DuckLake catalog and set up secrets")
    p_attach.add_argument("--config", default="config.yaml", help="Path to config file")
    p_attach.set_defaults(func=cmd_attach)

    p_tpch = sub.add_parser("load-tpch", help="Generate TPC-H data using DuckDB 'tpch' and load into DuckLake")
    p_tpch.add_argument("--config", default="config.yaml", help="Path to config file")
    p_tpch.add_argument("--scale", type=int, help="TPC-H scale factor (overrides config)")
    p_tpch.set_defaults(func=cmd_load_tpch)

    p_init = sub.add_parser("init-config", help="Write a starter config.yaml")
    p_init.add_argument("--path", help="Target path (default: ./config.yaml)")
    p_init.add_argument("--force", action="store_true", help="Overwrite if exists")
    p_init.set_defaults(func=cmd_init_config)

    p_bucket = sub.add_parser("ensure-bucket", help="Create bucket in MinIO if missing")
    p_bucket.add_argument("--config", default="config.yaml", help="Path to config file")

    def cmd_ensure_bucket(args):
        cfg = load_config(args.config)
        st = cfg.storage
        from minio import Minio
        from minio.error import S3Error

        client = Minio(
            st.endpoint.replace("http://", "").replace("https://", ""),
            access_key=st.access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=st.secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=st.use_ssl,
        )
        try:
            if not client.bucket_exists(st.bucket):
                client.make_bucket(st.bucket)
                print(f"[ok] created bucket '{st.bucket}' on {st.endpoint}")
            else:
                print(f"[skip] bucket '{st.bucket}' already exists")
        except S3Error as e:
            print(f"[error] failed to ensure bucket: {e}")
            sys.exit(1)

    p_bucket.set_defaults(func=cmd_ensure_bucket)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
