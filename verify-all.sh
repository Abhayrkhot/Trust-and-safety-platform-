#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_dir"

if [[ -x /opt/homebrew/opt/openjdk@17/bin/java ]]; then
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17
  export PATH="$JAVA_HOME/bin:$PATH"
fi

bash -n verify-all.sh scripts/run-local-load-benchmark.sh scripts/run-serving-query-benchmark.sh
mvn --batch-mode --no-transfer-progress -Dmaven.repo.local="$repo_dir/.m2" clean verify
test -f docs/claims-and-evidence.md
test -f target/site/jacoco/index.html
test -f target/bom.json
grep -q '"bomFormat"' target/bom.json
grep -q 'jackson-databind' target/bom.json
grep -q '2.21.5' target/bom.json
app_jar=target/safety-stream-0.1.0-SNAPSHOT-app.jar
unzip -t "$app_jar" >/dev/null
unzip -p "$app_jar" META-INF/MANIFEST.MF | grep -q 'Main-Class: dev.trustsafety.SafetyStreamJob'
java -jar "$app_jar" --help | grep -q '^usage:'
echo "verify-all: PASS"
