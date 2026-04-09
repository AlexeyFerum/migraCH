#!/bin/bash
# =============================================================================
# gen_certs.sh  –  Generate a self-signed CA + server certificates for every
# ClickHouse node in the Script 2 test environment.
#
# Run this ONCE from the script2/ directory before `docker compose up`:
#   cd ch-migration-testenv/script2
#   bash tls/gen_certs.sh
#
# Output layout (inside tls/)
# ---------------------------
#   ca.crt / ca.key          – root CA (shared trust anchor)
#   old-shard1.crt/.key      – old cluster node certs
#   old-shard2.crt/.key
#   new-s1-r1.crt/.key       – new cluster node certs
#   new-s1-r2.crt/.key
#   new-s2-r1.crt/.key
#   new-s2-r2.crt/.key
#   dhparam.pem              – DH parameters (2048-bit)
# =============================================================================

set -euo pipefail
cd "$(dirname "$0")"

DAYS=825   # max accepted by many TLS stacks
BITS=2048

NODES=(
    old-shard1
    old-shard2
    new-s1-r1
    new-s1-r2
    new-s2-r1
    new-s2-r2
)

echo "── Generating CA key and certificate ──────────────────────────"
openssl genrsa -out ca.key $BITS 2>/dev/null
openssl req -new -x509 -days $DAYS -key ca.key -out ca.crt \
    -subj "/C=XX/ST=Test/L=Test/O=CHMigrationTest/CN=TestCA" 2>/dev/null
echo "  CA cert: tls/ca.crt"

echo "── Generating node certificates ───────────────────────────────"
for node in "${NODES[@]}"; do
    openssl genrsa -out "${node}.key" $BITS 2>/dev/null

    # SAN must include the container hostname so ClickHouse inter-node
    # connections (which use the service name) pass certificate validation.
    cat > "${node}.ext" <<EOT
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
subjectAltName = DNS:${node},DNS:localhost,IP:127.0.0.1
EOT

    openssl req -new -key "${node}.key" \
        -subj "/C=XX/ST=Test/L=Test/O=CHMigrationTest/CN=${node}" \
        -config "${node}.ext" \
        -out "${node}.csr" 2>/dev/null

    openssl x509 -req -days $DAYS \
        -in  "${node}.csr" \
        -CA  ca.crt -CAkey ca.key -CAcreateserial \
        -extfile "${node}.ext" -extensions v3_req \
        -out "${node}.crt" 2>/dev/null

    rm -f "${node}.csr" "${node}.ext"
    echo "  ${node}.crt / ${node}.key"
done

echo "── Generating DH parameters (this may take a moment) ──────────"
openssl dhparam -out dhparam.pem $BITS 2>/dev/null
echo "  dhparam.pem"

# Set permissions expected by ClickHouse (readable by the clickhouse user
# inside containers, which runs as uid 101).
chmod 644 ./*.crt ./*.pem
chmod 640 ./*.key

echo ""
echo "Certificate generation complete."
echo "Files are in: $(pwd)"
