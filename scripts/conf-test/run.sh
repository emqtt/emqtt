#!/usr/bin/env bash

set -euo pipefail

cd -P -- "$(dirname -- "$0")/../.."
# shellcheck disable=SC1091
source ./env.sh

PROFILE="${PROFILE:-emqx}"
EMQX_ROOT="${EMQX_ROOT:-_build/$PROFILE/rel/emqx}"
EMQX_WAIT_FOR_START="${EMQX_WAIT_FOR_START:-30}"
export EMQX_WAIT_FOR_START

function check_dashboard_https_ssl_options_depth() {
  if [[ $1 =~ v5\.0\.25 ]]; then
    EXPECT_DEPTH=5
  else
    EXPECT_DEPTH=10
  fi
  DEPTH=$("$EMQX_ROOT"/bin/emqx eval "emqx:get_config([dashboard,listeners,https,ssl_options,depth],10)")
  if [[ "$DEPTH" != "$EXPECT_DEPTH" ]]; then
    echo "Bad Https depth $DEPTH, expect $EXPECT_DEPTH"
    exit 1
  fi
}

start_emqx_with_conf() {
    echo "Starting $PROFILE with $1"
    "$EMQX_ROOT"/bin/emqx start
    check_dashboard_https_ssl_options_depth "$1"
    "$EMQX_ROOT"/bin/emqx stop
}

PKG_VSN=${PKG_VSN:-$(./pkg-vsn.sh "$PROFILE")}
MAJOR_VSN=$(echo "$PKG_VSN" | cut -d- -f1 | cut -d. -f1)

if [ "$PROFILE" = "emqx" ]; then
  PREFIX="v"
else
  PREFIX="e"
fi

FILES=$(find ./scripts/conf-test/old-confs/ -name "${PREFIX}${MAJOR_VSN}*")

cp "$EMQX_ROOT"/etc/emqx.conf "$EMQX_ROOT"/etc/emqx.conf.bak
cleanup() {
    cp "$EMQX_ROOT"/etc/emqx.conf.bak "$EMQX_ROOT"/etc/emqx.conf
}
trap cleanup EXIT

for file in $FILES; do
    cp "$file" "$EMQX_ROOT"/etc/emqx.conf
    start_emqx_with_conf "$file"
done
