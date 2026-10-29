#!/bin/sh
# Polaris bootstrap script.
#
# The realm and root principal are auto-bootstrapped by Polaris itself
# (in-memory persistence + POLARIS_BOOTSTRAP_CREDENTIALS). This script does
# the rest:
#   1. POST /api/catalog/v1/oauth/tokens  - client_credentials token for root
#   2. GET/POST /api/management/v1/catalogs
#        - create INTERNAL catalog on rustfs (409 = already exists, OK)
#   3. PUT  /api/management/v1/catalogs/{c}/catalog-roles/catalog_admin/grants
#        - add CATALOG_MANAGE_CONTENT (catalog_admin only gets
#          CATALOG_MANAGE_ACCESS + CATALOG_MANAGE_METADATA by default)
#   4. GET  /api/catalog/v1/config?warehouse={c} - smoke test
#
# Safe to re-run against an already-provisioned Polaris.

set -eu

: "${POLARIS_URL:?POLARIS_URL required}"
: "${POLARIS_REALM:=POLARIS}"
: "${POLARIS_CLIENT_ID:?POLARIS_CLIENT_ID required}"
: "${POLARIS_CLIENT_SECRET:?POLARIS_CLIENT_SECRET required}"
: "${CATALOG_NAME:?CATALOG_NAME required}"
: "${RUSTFS_BUCKET:?RUSTFS_BUCKET required}"
: "${KEY_PREFIX:=polaris}"
: "${INTERNAL_S3_ENDPOINT:?INTERNAL_S3_ENDPOINT required}"
: "${CLIENT_S3_ENDPOINT:?CLIENT_S3_ENDPOINT required}"
: "${S3_REGION:=local}"
: "${STS_UNAVAILABLE:=true}"

BASE_LOCATION="s3://$RUSTFS_BUCKET/$KEY_PREFIX"

log() {
    printf '[bootstrap] %s\n' "$*"
}

# http METHOD URL [DATA] [CONTENT_TYPE]
# Retries transport failures. Prints "<http_code>\n<body>".
http() {
    method="$1"
    url="$2"
    data="${3:-}"
    ctype="${4:-application/json}"

    attempt=0
    max_attempts=10
    while [ "$attempt" -lt "$max_attempts" ]; do
        attempt=$((attempt + 1))
        set -- -sS -o /tmp/body -w '%{http_code}' -X "$method" \
            -H "Polaris-Realm: $POLARIS_REALM" \
            -H 'Accept: application/json'
        if [ -n "${TOKEN:-}" ]; then
            set -- "$@" -H "Authorization: Bearer $TOKEN"
        fi
        if [ -n "$data" ]; then
            set -- "$@" -H "Content-Type: $ctype" --data "$data"
        fi
        code=$(curl "$@" "$url" 2>/tmp/err) && rc=0 || rc=$?

        if [ "$rc" -eq 0 ]; then
            printf '%s\n' "$code"
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

split_response() {
    code=$(printf '%s\n' "$1" | head -n1)
    payload=$(printf '%s\n' "$1" | tail -n +2)
}

obtain_token() {
    log "POST $POLARIS_URL/api/catalog/v1/oauth/tokens (client_id=$POLARIS_CLIENT_ID)"
    attempt=0
    while :; do
        attempt=$((attempt + 1))
        out=$(http POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
            "grant_type=client_credentials&client_id=$POLARIS_CLIENT_ID&client_secret=$POLARIS_CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
            'application/x-www-form-urlencoded')
        split_response "$out"
        if [ "$code" = "200" ]; then
            break
        fi
        log "token response: HTTP $code body: $payload"
        if [ "$attempt" -ge 10 ]; then
            log "token request FAILED"
            return 1
        fi
        sleep 2
    done
    TOKEN=$(printf '%s' "$payload" | sed -n 's/.*"access_token" *: *"\([^"]*\)".*/\1/p')
    if [ -z "$TOKEN" ]; then
        log "could not parse access_token from: $payload"
        return 1
    fi
    log "token obtained"
}

create_catalog() {
    out=$(http GET "$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME")
    split_response "$out"
    if [ "$code" = "200" ]; then
        log "catalog '$CATALOG_NAME' already exists - OK"
        log "  body: $payload"
        return 0
    fi

    if [ "$STS_UNAVAILABLE" = "true" ]; then
        sts_fields='"stsUnavailable": true, "kmsUnavailable": true,'
    else
        sts_fields='"kmsUnavailable": true,'
    fi

    body=$(cat <<EOF
{
  "catalog": {
    "name": "$CATALOG_NAME",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "$BASE_LOCATION"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["$BASE_LOCATION"],
      "endpoint": "$CLIENT_S3_ENDPOINT",
      "endpointInternal": "$INTERNAL_S3_ENDPOINT",
      "pathStyleAccess": true,
      $sts_fields
      "region": "$S3_REGION"
    }
  }
}
EOF
)
    log "POST $POLARIS_URL/api/management/v1/catalogs (name=$CATALOG_NAME, base=$BASE_LOCATION)"
    out=$(http POST "$POLARIS_URL/api/management/v1/catalogs" "$body")
    split_response "$out"
    log "catalog response: HTTP $code"
    [ -n "$payload" ] && log "  body: $payload"
    case "$code" in
        2??) log "catalog '$CATALOG_NAME' created" ;;
        409) log "catalog '$CATALOG_NAME' already exists (409) - OK" ;;
        *) log "catalog creation FAILED: HTTP $code"; return 1 ;;
    esac
}

grant_manage_content() {
    url="$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles/catalog_admin/grants"
    log "PUT $url (CATALOG_MANAGE_CONTENT)"
    out=$(http PUT "$url" '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}')
    split_response "$out"
    log "grant response: HTTP $code"
    [ -n "$payload" ] && log "  body: $payload"
    case "$code" in
        2??) log "grant applied" ;;
        409) log "grant already present (409) - OK" ;;
        *) log "grant FAILED: HTTP $code"; return 1 ;;
    esac
}

smoke_test() {
    url="$POLARIS_URL/api/catalog/v1/config?warehouse=$CATALOG_NAME"
    out=$(http GET "$url")
    split_response "$out"
    log "config response: HTTP $code"
    [ -n "$payload" ] && log "  body: $payload"
    [ "$code" = "200" ] || { log "config smoke test FAILED"; return 1; }
}

log "starting Polaris bootstrap"
log "  target:   $POLARIS_URL (realm=$POLARIS_REALM)"
log "  catalog:  $CATALOG_NAME"
log "  storage:  $BASE_LOCATION internal=$INTERNAL_S3_ENDPOINT client=$CLIENT_S3_ENDPOINT sts_unavailable=$STS_UNAVAILABLE"

obtain_token
create_catalog
grant_manage_content
smoke_test

log "bootstrap complete"
