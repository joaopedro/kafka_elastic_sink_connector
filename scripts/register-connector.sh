#!/usr/bin/env bash
# Registers the Elasticsearch Sink Connector via the Kafka Connect REST API.
# Run this after the stack is up and kafka-connect is healthy.

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_CONFIG="$(dirname "$0")/../connector/elasticsearch-sink.json"

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do
  sleep 3
done

echo "Registering connector..."
curl -sf -X POST \
  -H "Content-Type: application/json" \
  --data @"${CONNECTOR_CONFIG}" \
  "${CONNECT_URL}/connectors" | jq .

echo ""
echo "Connector registered. Check status with:"
echo "  curl ${CONNECT_URL}/connectors/elasticsearch-sink-orders/status | jq ."
