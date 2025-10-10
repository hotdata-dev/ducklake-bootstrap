#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

if [ ! -d "venv" ]; then
  echo "[+] Creating virtual environment..."
  python3 -m venv venv
fi

source venv/bin/activate

echo "[+] Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "[✓] Environment ready. To activate manually later:"
echo "source venv/bin/activate"
