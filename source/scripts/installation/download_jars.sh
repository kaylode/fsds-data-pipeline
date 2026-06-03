#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
TMP_DIR="$( cd "$PROJECT_ROOT/.." && pwd )/.tmp"
FLINK_LIB_DIR="$TMP_DIR/flink-lib"

echo "📂 Creating flink-lib directory at $FLINK_LIB_DIR if it does not exist..."
mkdir -p "$FLINK_LIB_DIR"

# List of JAR files to download from Maven Central
JARS=(
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar"
)

echo "📥 Downloading Flink SQL Connector Kafka JARs..."

for url in "${JARS[@]}"; do
    filename=$(basename "$url")
    target_path="$FLINK_LIB_DIR/$filename"
    
    if [ -f "$target_path" ]; then
        echo "✅ $filename already exists, skipping."
    else
        echo "⬇️  Downloading $filename..."
        curl -s -L -o "$target_path" "$url"
        echo "   Saved to $target_path"
    fi
done

echo "🎉 All supplementary JAR files successfully downloaded!"
