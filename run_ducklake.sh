#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

source venv/bin/activate

echo "[+] Starting MinIO..."
docker compose -f docker-compose.minio.yml up -d

echo "[...] Waiting for MinIO..."
until curl -sSf http://localhost:9000/minio/health/live >/dev/null; do
  sleep 2
done
echo "[✓] MinIO is live."

echo "[+] Init config"
python3 bootstrap_ducklake.py init-config

echo "[+] Ensuring bucket..."
python3 bootstrap_ducklake.py ensure-bucket --config config.yaml



echo "[+] Attaching DuckLake catalog..."
python3 bootstrap_ducklake.py attach --config config.yaml

SCALE_FACTOR=${TPCH_SCALE:-1}
echo "[+] Loading TPC-H scale factor = ${SCALE_FACTOR}"
python3 bootstrap_ducklake.py load-tpch --config config.yaml --scale ${SCALE_FACTOR}

echo "[✓] DuckLake ready at metadata.ducklake + MinIO s3://ducklake-data/"
