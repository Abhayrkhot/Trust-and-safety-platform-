#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

if [[ -x /opt/homebrew/opt/openjdk@17/bin/java ]]; then
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17
  export PATH="$JAVA_HOME/bin:$PATH"
fi

samples="${1:-300}"
warmups="${2:-50}"
revision="$(git rev-parse HEAD)"
mvn --batch-mode --no-transfer-progress test-compile failsafe:integration-test failsafe:verify \
  -Dit.test=ServingQueryBenchmarkIT \
  -DservingBenchmarkSamples="$samples" \
  -DservingBenchmarkWarmups="$warmups" \
  -DservingBenchmarkRevision="$revision"
printf 'result: %s\n' "$repo_dir/target/benchmark-results/serving-query-latency.json"
