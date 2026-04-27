#!/usr/bin/env bash
# Builds the Java producer fat JAR.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PRODUCER_DIR="${SCRIPT_DIR}/../producer"

echo "Building producer..."
cd "${PRODUCER_DIR}"
mvn -q clean package -DskipTests

echo "Build complete: producer/target/kafka-producer-1.0-SNAPSHOT.jar"
