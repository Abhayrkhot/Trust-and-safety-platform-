#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

if [[ -x /opt/homebrew/opt/openjdk@17/bin/java ]]; then
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17
  export PATH="$JAVA_HOME/bin:$PATH"
fi

events="${1:-60000}"
measured_pairs="${2:-10}"
actors="${3:-6000}"
warmup_pairs="${4:-2}"
revision="$(git rev-parse HEAD)"
mvn --batch-mode --no-transfer-progress test-compile failsafe:integration-test failsafe:verify \
  -Dit.test='EndToEndPipelineIT#benchmarksBackloggedKafkaThroughFlinkIntoBothServingStores' \
  -DendToEndBenchmarkEvents="$events" \
  -DendToEndBenchmarkMeasuredPairs="$measured_pairs" \
  -DendToEndBenchmarkWarmupPairs="$warmup_pairs" \
  -DendToEndBenchmarkActors="$actors" \
  -DendToEndBenchmarkRevision="$revision"
printf 'result: %s\n' "$repo_dir/target/benchmark-results/end-to-end-load.json"
