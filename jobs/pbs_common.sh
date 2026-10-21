# Shared startup checks for the PBS wrappers; source after their early traps.
# No Python imports, credentials, full environment dumps or shell tracing.

ecore_note() {
    printf '%s\n' "$*"
    if test -n "${ecore_startup_log:-}"; then
        printf '%s\n' "$*" >> "$ecore_startup_log"
    fi
}

ecore_fail() {
    printf 'E-CORE error | phase %s | %s\n' "$ecore_phase" "$*" >&2
    if test -n "${ecore_startup_log:-}"; then
        printf 'E-CORE error | phase %s | %s\n' "$ecore_phase" "$*" >> "$ecore_startup_log"
    fi
    exit 1
}

ecore_require() {
    local name
    for name in "$@"; do
        if test -z "${!name:-}"; then
            ecore_fail "Required variable $name is unset or empty; export it and include it in qsub -v."
        fi
    done
}

ecore_startup_record() {
    # One small record per source/mode, overwritten by the next attempt.
    # Application output continues through PBS's normal streamed output.
    local state="$1"
    mkdir -p "$state"
    ecore_startup_log="$state/startup.log"
    : > "$ecore_startup_log"
    ecore_note "E-CORE startup | job $PBS_JOBID | host ${HOSTNAME:-unknown} | mode $ECORE_PBS_MODE"
}

ecore_code_directory() {
    ecore_phase=code-directory
    if ! test -d "$code_root"; then
        ecore_fail "Repository directory is unavailable: $code_root"
    fi
    cd "$code_root"
    ecore_note "Using code directory | $code_root"
}

ecore_walltime() {
    local minimum="$1"
    ecore_phase=walltime
    walltime="$(qstat -f "$PBS_JOBID" | awk '$1 == "Resource_List.walltime" {print $3}')"
    if ! [[ "$walltime" =~ ^[0-9]+:[0-5][0-9]:[0-5][0-9]$ ]]; then
        ecore_fail 'qstat did not return a valid Resource_List.walltime (HH:MM:SS).'
    fi
    IFS=: read -r wall_hours wall_minutes wall_seconds <<< "$walltime"
    job_seconds=$((10#$wall_hours*3600 + 10#$wall_minutes*60 + 10#$wall_seconds))
    if test "$job_seconds" -le "$minimum"; then
        ecore_fail "Requested walltime must exceed $minimum seconds for safe shutdown."
    fi
    ecore_note "Walltime verified | $walltime"
}
