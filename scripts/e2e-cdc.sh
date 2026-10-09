#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# E2E test for CDC (DPFMD-33): INSERT a row in PostgreSQL and assert that the
# matching change event shows up on the Debezium Kafka topic, Avro-decoded.
#
#   1. analytics.order_items -> cdc_order_items       (orderitems-postgres)
#   2. public.products       -> cdc_product_changes   (productcatalog-postgres)
#
# It is an end-to-end test, not a unit test: it needs a live dev cluster where
# ArgoCD has applied the dev overlay, so that the kafka / postgres /
# schema-registry / debezium-connect pods exist and the connectors are RUNNING.
#
# It proves the full path PostgreSQL WAL -> Debezium -> Kafka topic -> Avro, by
# reading back the event that carries the value we inserted.
#
# Avro decoding runs in short-lived per-partition consumer pods, NOT inside the
# schema-registry pod: that pod is memory-limited (512Mi) and running extra
# consumer JVMs there OOMKills it.
#
# Usage:
#   scripts/e2e-cdc.sh [--cluster <kube-context>] [--namespace <ns>]
#
# Env overrides:
#   CLUSTER         (kube context)                 default kind-ci-local
#   NAMESPACE                                      default onlineboutique-dev
#   TIMEOUT         (seconds to wait for an event) default 60
#   CONSUMER_IMAGE  default: the schema-registry Deployment image

set -euo pipefail

CLUSTER="${CLUSTER:-kind-ci-local}"
NAMESPACE="${NAMESPACE:-onlineboutique-dev}"
TIMEOUT="${TIMEOUT:-60}"
RUN_ID="$(date +%s)-$RANDOM"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster)   CLUSTER="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    -h|--help)
      sed -n '17,41p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

log()  { echo "[e2e-cdc] $*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

kubectl() { command kubectl --context "$CLUSTER" -n "$NAMESPACE" "$@"; }
kexec()   { command kubectl --context "$CLUSTER" -n "$NAMESPACE" exec "$@"; }

cleanup() {
  command kubectl --context "$CLUSTER" -n "$NAMESPACE" \
    delete pods -l "e2e-cdc-run=$RUN_ID" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- preconditions ---------------------------------------------------------
check_pods() {
  local res
  for res in deploy/kafka deploy/postgres deploy/schema-registry deploy/debezium-connect; do
    kubectl get "$res" >/dev/null 2>&1 || fail "$res not found in $NAMESPACE (is the dev overlay applied?)"
  done
  log "waiting for pods to be ready ..."
  kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s >/dev/null
  kubectl wait --for=condition=ready pod -l app=postgres --timeout=300s >/dev/null
  kubectl wait --for=condition=ready pod -l app=schema-registry --timeout=300s >/dev/null
}

check_connectors() {
  log "checking connectors are RUNNING ..."
  local name status state
  for name in productcatalog-postgres orderitems-postgres; do
    status=$(kexec deploy/debezium-connect -- \
      curl -s "localhost:8083/connectors/$name/status" 2>/dev/null || true)
    state=$(echo "$status" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("MISSING"); raise SystemExit
tasks = [t.get("state") for t in d.get("tasks", [])]
print(d.get("connector", {}).get("state") if tasks and all(s == "RUNNING" for s in tasks) else "NOT_RUNNING")
' 2>/dev/null || echo "MISSING")
    [[ "$state" == "RUNNING" ]] || fail "connector $name is $state"
    log "  $name: RUNNING"
  done
}

# --- offset helpers --------------------------------------------------------
# end_offsets <topic> -> end offset per partition, comma-separated, by partition.
end_offsets() {
  kexec deploy/kafka -- \
    kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:9092 --topic "$1" --time -1 2>/dev/null \
  | awk -F: '{print $2":"$3}' | sort -t: -k1,1n | awk -F: '{print $2}' | paste -sd, -
}

# consume_from <topic> <partition> <offset> <outfile>
# Avro-decoded read from an exact offset, in a disposable per-partition pod.
consume_from() {
  local pod="e2e-cdc-${RUN_ID}-${1//[._]/-}-$2"
  kubectl run "$pod" --rm -i --restart=Never \
    --image="$CONSUMER_IMAGE" --labels="e2e-cdc-run=$RUN_ID" --command -- \
    kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic "$1" --partition "$2" --offset "$3" \
      --property schema.registry.url=http://schema-registry:8081 \
      --property print.key=true \
      --max-messages 500 --timeout-ms $((TIMEOUT * 1000)) \
    >"$4" 2>/dev/null || true
}

# expect_event <topic> <expected-after-json> <label> <offsets-csv> <marker>
# `marker` is the unique id we inserted: as soon as it appears in any partition
# we stop waiting, so TIMEOUT is only an upper bound, not a fixed cost.
expect_event() {
  local topic="$1" expected="$2" label="$3" offsets_csv="$4" marker="$5"
  local -a offsets
  IFS=',' read -r -a offsets <<<"$offsets_csv"
  local nparts=${#offsets[@]}

  local tmp; tmp="$(mktemp -d)"
  local -a pids=()
  local p
  for ((p = 0; p < nparts; p++)); do
    consume_from "$topic" "$p" "${offsets[$p]}" "$tmp/part.$p" &
    pids+=("$!")
  done

  local deadline=$((SECONDS + TIMEOUT))
  while ((SECONDS < deadline)); do
    if grep -qsF "$marker" "$tmp"/part.* 2>/dev/null; then
      break
    fi
    sleep 2
  done

  local pid
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  cat "$tmp"/part.* >"$tmp/all" 2>/dev/null || true

  local rc=0
  assert_event_py "$tmp/all" "$expected" "$topic" "$label" || rc=$?
  rm -rf "$tmp" 2>/dev/null || true
  return "$rc"
}

assert_event_py() {
  python3 - "$@" <<'PY'
import json, sys

path, expected_raw, topic, label = sys.argv[1:5]
expected = json.loads(expected_raw)
SCALAR = ("string", "int", "long", "boolean", "float", "double", "bytes")


def unwrap(node):
    """Avro union/nullable wrappers: {"string": "x"} -> "x"."""
    if isinstance(node, dict) and len(node) == 1 and next(iter(node)) in SCALAR:
        return unwrap(next(iter(node.values())))
    return node


def row(after):
    """Debezium emits `after` nested under "<db>.<schema>.<table>.Value"."""
    if isinstance(after, dict) and len(after) == 1:
        inner = next(iter(after.values()))
        if isinstance(inner, dict):
            return inner
    return after or {}


found = None
with open(path, encoding="utf-8", errors="replace") as handle:
    for line in handle:
        line = line.rstrip("\n")
        if not line:
            continue
        value = line.split("\t")[-1]            # print.key=true -> key \t value
        try:
            event = json.loads(value)
        except ValueError:
            continue
        if event.get("op") != "c":
            continue
        fields = row(event.get("after"))
        if all(str(unwrap(fields.get(k))) == str(v) for k, v in expected.items()):
            found = fields
            break

if found is None:
    print(f"FAIL {label}: no op=c event on {topic} with {expected}", file=sys.stderr)
    sys.exit(1)
print(f"PASS {label}: {topic} <- op=c { {k: unwrap(found.get(k)) for k in expected} }",
      file=sys.stderr)
PY
}

# --- test bodies -----------------------------------------------------------
test_order_items() {
  local id="E2E-$(date +%s)-$RANDOM"
  local product="e2e-probe"
  log "test 1: analytics.order_items -> cdc_order_items (order_id=$id)"

  local offsets; offsets="$(end_offsets cdc_order_items)"

  kexec deploy/postgres -- psql -U boutique -d product_catalog -v ON_ERROR_STOP=1 -c \
    "INSERT INTO analytics.order_items
       (order_id, product_id, quantity, currency_code, unit_price, total_price, event_timestamp, status)
     VALUES ('$id', '$product', 1, 'USD', 1.23, 1.23, now(), 'SUCCEED');" >/dev/null

  expect_event cdc_order_items \
    "{\"order_id\": \"$id\", \"product_id\": \"$product\", \"status\": \"SUCCEED\"}" \
    "order_items" "$offsets" "$id"

  kexec deploy/postgres -- psql -U boutique -d product_catalog -c \
    "DELETE FROM analytics.order_items WHERE order_id = '$id';" >/dev/null
  log "  cleaned up $id"
}

test_products() {
  local id="e2e-probe-$(date +%s)-$RANDOM"
  log "test 2: public.products -> cdc_product_changes (id=$id)"

  local offsets; offsets="$(end_offsets cdc_product_changes)"

  kexec deploy/postgres -- psql -U boutique -d product_catalog -v ON_ERROR_STOP=1 -c \
    "INSERT INTO public.products
       (id, name, description, picture_url, price_units, price_nanos, currency_code)
     VALUES ('$id', 'E2E Probe', 'e2e', '', 1, 0, 'USD')
     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;" >/dev/null

  expect_event cdc_product_changes \
    "{\"id\": \"$id\", \"name\": \"E2E Probe\"}" "products" "$offsets" "$id"

  kexec deploy/postgres -- psql -U boutique -d product_catalog -c \
    "DELETE FROM public.products WHERE id = '$id';" >/dev/null
  log "  cleaned up $id"
}

main() {
  check_pods
  CONSUMER_IMAGE="${CONSUMER_IMAGE:-$(kubectl get deploy/schema-registry -o jsonpath='{.spec.template.spec.containers[0].image}')}"
  log "cluster=$CLUSTER namespace=$NAMESPACE timeout=${TIMEOUT}s image=$CONSUMER_IMAGE"
  check_connectors
  test_order_items
  test_products
  log "ALL CDC E2E TESTS PASSED"
}

main "$@"
