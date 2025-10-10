# DuckLake Bootstrap (DuckDB metadata + MinIO data)

This starter helps you spin up a **DuckLake** catalog backed by a local **DuckDB metadata DB** and **MinIO** (S3-compatible) for Parquet data files.
It also includes a CLI to **generate and load a TPC-H dataset** into DuckLake using **DuckDB's `ducklake` and `tpch` extensions** (no manual data loads).

## What's inside
- `docker-compose.minio.yml` — launches a local MinIO server + console.
- `config.example.yaml` — minimal config for metadata path, MinIO, and dataset options.
- `bootstrap_ducklake.py` — Python CLI to attach a DuckLake catalog and load TPC-H tables.
- This `README.md`.

## Quick start

1) **Run MinIO (Docker required)**

```bash
docker compose -f docker-compose.minio.yml up -d
# Console: http://localhost:9001  |  S3 endpoint: http://localhost:9000
# Default creds in compose file: minioadmin / minioadmin
# Create a bucket, e.g. 'ducklake-data' (via the console)
```

2) **Copy config & edit**

```bash
cp config.example.yaml config.yaml
# set: bucket, prefix, region, and (optionally) MINIO creds via env or config
```

3) **Use the CLI**

```bash
python3 bootstrap_ducklake.py attach --config config.yaml
python3 bootstrap_ducklake.py load-tpch --config config.yaml --scale 1
# Re-run load-tpch with a different scale if you want (will CTAS into DuckLake)
```

## Design notes
- **Extensible backends**: The CLI is structured so you can add new metadata backends (e.g., Postgres) and storage backends (e.g., AWS S3) later.
- **Pure DuckDB/DuckLake**: We use only DuckDB SQL: `CREATE SECRET ...`, `ATTACH 'ducklake:...' (DATA_PATH ...)`, and `CALL dbgen(...)` followed by `CREATE TABLE ... AS SELECT ...` into DuckLake.
- **No manual file writing**: Parquet files are created by DuckLake within your object store path.

## Requirements
- Python 3.9+
- `pip install duckdb pyyaml`
- Docker (for running MinIO).

