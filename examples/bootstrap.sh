#!/bin/sh
# Lakekeeper bootstrap script.
#
# Performs the two HTTP calls required after Lakekeeper starts for the first
# time:
#   1. POST /management/v1/bootstrap   - accept terms of use (NOT idempotent;
#                                        Lakekeeper v0.12 returns 400 with
#                                        body type "CatalogAlreadyBootstrapped"
#                                        on subsequent invocations)
#   2. POST /management/v1/warehouse   - create the warehouse pointing at
#                                        rustfs (Lakekeeper v0.12 returns 400
#                                        with CreateWarehouseStorageProfileOverlap
#                                        or WarehouseNameAlreadyTaken on
#                                        replay; older / future versions
#                                        may use 409)
#
# Both calls tolerate "already done" responses so this container is safe to
# re-run against an already-bootstrapped Lakekeeper. Exits non-zero only
# on a persistent failure (network problems, 5xx, unrecognised 4xx).

set -eu

: "${LAKEKEEPER_URL:?LAKEKEEPER_URL required}"
: "${WAREHOUSE_NAME:?WAREHOUSE_NAME required}"
: "${RUSTFS_ENDPOINT:?RUSTFS_ENDPOINT required}"
: "${RUSTFS_BUCKET:?RUSTFS_BUCKET required}"
: "${RUSTFS_ACCESS_KEY:?RUSTFS_ACCESS_KEY required}"
: "${RUSTFS_SECRET_KEY:?RUSTFS_SECRET_KEY required}"
: "${RUSTFS_REGION:=local}"
# The endpoint we hand to Lakekeeper as the *client* endpoint - what
# clients (PyIceberg / Spark) will use to talk to S3 directly. We default
# to the host-routable URL so tests can run from the docker host; when
# the tests-in-container use case shows up later we can pass through
# a different value here.
: "${CLIENT_RUSTFS_ENDPOINT:=$RUSTFS_ENDPOINT}"

log() {
    printf '[bootstrap] %s\n' "$*"
}

# curl_with_retry METHOD URL [DATA]
# Retries on transport failures with exponential-ish backoff.
# Prints "<http_code>\n<body>" on stdout.
curl_with_retry() {
    method="$1"
    url="$2"
    data="${3:-}"

    attempt=0
    max_attempts=10
    while [ "$attempt" -lt "$max_attempts" ]; do
        attempt=$((attempt + 1))
        if [ -n "$data" ]; then
            resp=$(curl -sS -o /tmp/body -w '%{http_code}' \
                -X "$method" \
                -H 'Content-Type: application/json' \
                --data "$data" \
                "$url" 2>/tmp/err) && rc=0 || rc=$?
        else
            resp=$(curl -sS -o /tmp/body -w '%{http_code}' \
                -X "$method" \
                "$url" 2>/tmp/err) && rc=0 || rc=$?
        fi

        if [ "$rc" -eq 0 ]; then
            printf '%s\n' "$resp"
            cat /tmp/body
            return 0
        fi

        log "attempt $attempt/$max_attempts: curl failed (rc=$rc) for $method $url:"
        cat /tmp/err >&2 || true
        sleep_for=$((attempt * 2))
        log "sleeping ${sleep_for}s before retry ..."
        sleep "$sleep_for"
    done

    log "gave up after $max_attempts attempts: $method $url"
    return 1
}

bootstrap_terms() {
    log "POST $LAKEKEEPER_URL/management/v1/bootstrap"
    body='{"accept-terms-of-use": true}'
    out=$(curl_with_retry POST "$LAKEKEEPER_URL/management/v1/bootstrap" "$body")
    code=$(printf '%s\n' "$out" | head -n1)
    payload=$(printf '%s\n' "$out" | tail -n +2)
    log "bootstrap response: HTTP $code"
    [ -n "$payload" ] && log "  body: $payload"

    case "$code" in
        2??)
            log "bootstrap accepted"
            return 0
            ;;
        409)
            log "bootstrap already performed (409) - OK"
            return 0
            ;;
        400)
            # Lakekeeper v0.12 returns 400 (not 409) with a structured
            # error body when /bootstrap is called on an already-bootstrapped
            # catalog. Detect by error type so we don't swallow real 400s.
            if printf '%s' "$payload" | grep -q 'CatalogAlreadyBootstrapped'; then
                log "bootstrap already performed (CatalogAlreadyBootstrapped) - OK"
                return 0
            fi
            log "bootstrap FAILED: HTTP 400 (not the already-bootstrapped marker)"
            return 1
            ;;
        *)
            log "bootstrap FAILED: HTTP $code"
            return 1
            ;;
    esac
}

create_warehouse() {
    log "POST $LAKEKEEPER_URL/management/v1/warehouse (name=$WAREHOUSE_NAME)"
    # Build the warehouse payload. rustfs is S3-compatible but does not
    # speak STS, so we disable sts and rely on static creds vended back
    # to the client by Lakekeeper.
    # NOTE on the endpoint: clients that load tables from this warehouse
    # connect to S3 directly using whatever endpoint Lakekeeper hands
    # back. We point at the host-routable URL (CLIENT_RUSTFS_ENDPOINT)
    # rather than the in-network ``http://rustfs:9000`` so that tests
    # running on the docker *host* can reach storage. Lakekeeper itself
    # does not need to talk to S3 for the read/write path - clients do.
    #
    # remote-signing is disabled so PyIceberg drives the S3 calls
    # directly with the static credentials below (and so we don't have to
    # ship s3fs as a test-only dep just to satisfy the REST-signing IO).
    body=$(cat <<EOF
{
  "warehouse-name": "$WAREHOUSE_NAME",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "$RUSTFS_BUCKET",
    "key-prefix": "warehouse",
    "endpoint": "$CLIENT_RUSTFS_ENDPOINT",
    "region": "$RUSTFS_REGION",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false,
    "remote-signing-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "$RUSTFS_ACCESS_KEY",
    "secret-access-key": "$RUSTFS_SECRET_KEY"
  }
}
EOF
)
    out=$(curl_with_retry POST "$LAKEKEEPER_URL/management/v1/warehouse" "$body")
    code=$(printf '%s\n' "$out" | head -n1)
    payload=$(printf '%s\n' "$out" | tail -n +2)
    log "warehouse response: HTTP $code"
    [ -n "$payload" ] && log "  body: $payload"

    case "$code" in
        2??)
            log "warehouse '$WAREHOUSE_NAME' created"
            return 0
            ;;
        409)
            log "warehouse '$WAREHOUSE_NAME' already exists (409) - OK"
            return 0
            ;;
        400)
            # Lakekeeper v0.12 returns 400 with structured error types when
            # the warehouse already exists (either by name or by overlapping
            # storage profile). Both are benign for an idempotent bootstrap.
            if printf '%s' "$payload" | \
                grep -qE 'CreateWarehouseStorageProfileOverlap|WarehouseNameAlreadyTaken|WarehouseAlreadyExists'; then
                log "warehouse '$WAREHOUSE_NAME' already provisioned - OK"
                return 0
            fi
            log "warehouse creation FAILED: HTTP 400 (unrecognised error type)"
            return 1
            ;;
        *)
            log "warehouse creation FAILED: HTTP $code"
            return 1
            ;;
    esac
}

log "starting Lakekeeper bootstrap"
log "  target:    $LAKEKEEPER_URL"
log "  warehouse: $WAREHOUSE_NAME"
log "  storage:   $RUSTFS_ENDPOINT (bucket=$RUSTFS_BUCKET)"

bootstrap_terms
create_warehouse

log "bootstrap complete"
