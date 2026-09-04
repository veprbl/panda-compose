#!/usr/bin/env bash
# Generate OpenSSL hash symlink for Rucio CA certificate
# Usage: ./generate-ca-hash.sh <path-to-ca.pem> <output-dir>

set -euo pipefail

CA_PEM="${1:-config/rucio/rucio_ca.pem}"
OUT_DIR="${2:-config/rucio}"

if [ ! -f "$CA_PEM" ]; then
    echo "Error: CA certificate not found at $CA_PEM"
    exit 1
fi

mkdir -p "$OUT_DIR"

# Compute subject hash
HASH=$(openssl x509 -hash -noout -in "$CA_PEM")

# Create symlink
ln -sf "$(basename "$CA_PEM")" "$OUT_DIR/${HASH}.0"

echo "Created symlink: $OUT_DIR/${HASH}.0 -> $(basename "$CA_PEM")"
echo "Subject hash: $HASH"

# Also verify it works
openssl verify -CAfile "$CA_PEM" "$CA_PEM" && echo "Certificate self-verifies OK"
