#!/usr/bin/env bash
# Lints spec/openapi.yaml: Redocly CLI's recommended rules (pinned, through npx: nothing is
# installed globally), then openapi-spec-validator's OpenAPI 3.1 schema check with the
# conformance suite's environment. Exits non-zero on any error or warning.
#   spec/scripts/lint-openapi.sh            (needs node and npx, and uv)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$HERE"
npx -y @redocly/cli@2.57.0 lint --config spec/redocly.yaml --max-problems 200 spec/openapi.yaml
# Redocly exits 0 on warnings; this file has none, so a new one is a failure.
summary="$(npx -y @redocly/cli@2.57.0 lint --config spec/redocly.yaml --format summary spec/openapi.yaml 2>&1)"
if grep -q '^warning' <<<"$summary"; then
  echo "lint-openapi: Redocly reported warnings" >&2
  exit 1
fi
uv run --quiet --no-project --with pyyaml==6.0.3 --with openapi-spec-validator==0.9.0 python -c '
import sys, yaml
from openapi_spec_validator import validate
validate(yaml.safe_load(open("spec/openapi.yaml", encoding="utf-8")))
print("openapi-spec-validator: spec/openapi.yaml is valid OpenAPI 3.1")'
