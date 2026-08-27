#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
count="${1:-50000}"
classpath_file="$(mktemp "${TMPDIR:-/tmp}/safety-classpath.XXXXXX")"
trap 'rm -f "$classpath_file"' EXIT
mvn --batch-mode --no-transfer-progress -Dmaven.repo.local="$repo_dir/.m2" -DskipTests test-compile \
  org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile="$classpath_file"
maven_java_home="$(mvn --quiet --no-transfer-progress -Dmaven.repo.local="$repo_dir/.m2" help:evaluate -Dexpression=java.home -DforceStdout)"
"$maven_java_home/bin/java" --add-opens=java.base/java.util=ALL-UNNAMED \
  -cp "target/test-classes:target/classes:$(<"$classpath_file")" \
  dev.trustsafety.benchmark.LocalLoadBenchmark "$count"
