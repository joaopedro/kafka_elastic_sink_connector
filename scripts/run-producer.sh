#!/usr/bin/env bash
# Runs the Java producer. Builds first if the JAR is missing.
#
# Environment variables (all optional):
#   KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
#   KAFKA_TOPIC              default: orders
#   MESSAGE_COUNT            default: 20
#   DELAY_MS                 default: 500

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="${SCRIPT_DIR}/../producer/target/kafka-producer-1.0-SNAPSHOT.jar"

if [[ ! -f "${JAR}" ]]; then
  echo "JAR not found — building first..."
  "${SCRIPT_DIR}/build-producer.sh"
fi

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-orders}"
export MESSAGE_COUNT="${MESSAGE_COUNT:-20}"
export DELAY_MS="${DELAY_MS:-500}"

echo "Producing ${MESSAGE_COUNT} messages to topic '${KAFKA_TOPIC}' on ${KAFKA_BOOTSTRAP_SERVERS}..."
java -jar "${JAR}"
