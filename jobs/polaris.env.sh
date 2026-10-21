# Source this non-sensitive configuration in each new Polaris terminal:
#   source jobs/polaris.env.sh
# It reads current project quota but neither submits jobs nor fetches NOAA data.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    printf 'Use: source jobs/polaris.env.sh\n' >&2
    exit 2
fi

export ECORE_PROJECT_ROOT="${ECORE_PROJECT_ROOT:-/eagle/lighthouse-upr}"
export ECORE_PYTHON="${ECORE_PYTHON:-$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu/bin/python}"
export ECORE_CODE_ROOT="${ECORE_CODE_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}"
export ECORE_PBS_ACCOUNT="${ECORE_PBS_ACCOUNT:-lighthouse-upr}"
export ECORE_QUOTA_PROJECT="${ECORE_QUOTA_PROJECT:-lighthouse-upr}"
export PBS_GOES_START="${PBS_GOES_START:-2021-01-01}"
export PBS_GOES_END="${PBS_GOES_END:-2022-01-01}"
export PBS_MRMS_START="${PBS_MRMS_START:-2021-01-01}"
export PBS_MRMS_END="${PBS_MRMS_END:-2026-07-01}"
export ECORE_PBS_ENV_VARS=ECORE_PROJECT_ROOT,ECORE_CODE_ROOT,ECORE_PYTHON,ECORE_QUOTA_FREE_GIB

ecore_refresh_quota() {
    local quota_output quota_python quota_value
    unset ECORE_QUOTA_FREE_GIB  # Never retain a stale value after a failed lookup.
    if ! command -v myprojectquotas >/dev/null 2>&1; then
        printf 'myprojectquotas is unavailable; source this configuration on Polaris.\n' >&2
        return 1
    fi
    quota_python="$ECORE_PYTHON"
    if ! test -x "$quota_python"; then
        quota_python="$(command -v python3)" || {
            printf 'Python 3 is needed to parse the quota table.\n' >&2; return 1;
        }
    fi
    if ! quota_output="$(LC_ALL=C timeout 30s myprojectquotas)"; then
        printf 'Project quota lookup failed; inspect myprojectquotas before submitting.\n' >&2
        return 1
    fi
    if ! quota_value="$(printf '%s\n' "$quota_output" | "$quota_python" \
        "$ECORE_CODE_ROOT/src/ecore_weather/pbs.py" quota --project "$ECORE_QUOTA_PROJECT" --filesystem eagle)"; then
        return 1
    fi
    export ECORE_QUOTA_FREE_GIB="$quota_value"
    printf 'Available Eagle quota estimate: %s GiB (conservative; refreshed now).\n' "$ECORE_QUOTA_FREE_GIB" >&2
}

# Job mode is intentionally chosen at submission, not fixed here.
ecore_refresh_quota
