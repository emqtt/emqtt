#!/usr/bin/env bash
set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

## This script prints the relup upgrade base versions
## for the given EMQX edition (specified as first arg)
##
## The second argument is the current release version
## if not provided, it's taken from pkg-vsn.sh

usage() {
    echo "Usage: $0 <EMQX_PROFILE> [<CURRENT_VERSION>]"
    echo "e.g.   $0 enterprise 4.3.10"
    exit 1
}

parse_semver() {
    echo "$1" | tr '.|-' ' '
}

EDITION="${1:-}"
[ -z "${EDITION}" ] && usage

## Get the current release version
## e.g.
## 5.0.0                 when GA
## 5.0.0-beta.3          when pre-release
## 5.0.0-beta.3.abcdef00 when developing
CUR="${2:-}"
if [ -z "${CUR}" ]; then
    CUR="$(./pkg-vsn.sh)"
fi

# shellcheck disable=SC2207
CUR_SEMVER=($(parse_semver "$CUR"))

if [ "${#CUR_SEMVER[@]}" -lt 3 ]; then
    echo "$CUR is not Major.Minor.Patch"
    usage
fi

case "${EDITION}" in
    *enterprise*)
        GIT_TAG_PREFIX="e"
        ;;
    *)
        GIT_TAG_PREFIX="v"
        ;;
esac

# must not be empty for MacOS (bash 3.x)
TAGS=( 'dummy' )
TAGS_EXCLUDE=( 'dummy' )

while read -r vsn; do
  # shellcheck disable=SC2207
  TAGS+=($(git tag -l "${GIT_TAG_PREFIX}${vsn}"))
done < <(./scripts/relup-base-vsns.escript base-vsns "$CUR" ./scripts/relup_bases.eterm)

for tag_to_del in "${TAGS_EXCLUDE[@]}"; do
    TAGS=( "${TAGS[@]/$tag_to_del}" )
done

for tag in "${TAGS[@]}"; do
    if [ "$tag" != '' ]; then
        echo "$tag"
    fi
done
