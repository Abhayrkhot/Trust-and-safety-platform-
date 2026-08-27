#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_dir"

bash -n verify-all.sh scripts/run-local-load-benchmark.sh
mvn --batch-mode --no-transfer-progress -Dmaven.repo.local="$repo_dir/.m2" clean verify
test -f docs/claims-and-evidence.md
echo "verify-all: PASS"
