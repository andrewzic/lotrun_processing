#!/bin/bash
# Helper functtions for sbatch submission and chaining, 
# to be sourced by the various pipeline scripts in this directory.

# -------------------- GENERIC SUBMISSION + HELPERS --------------------
log(){ printf '[%s] %s\n' "$(date +'%F %T')" "$*" >&2; }

validate_stages() {
  local start_stage="${START_STAGE:-}"
  local end_stage="${END_STAGE:-}"
  
  if [[ -n "$start_stage" ]]; then
    local found=0
    for stage in "${PIPELINE_STAGES[@]}"; do
      if [[ "$stage" == "$start_stage" ]]; then
        found=1
        break
      fi
    done
    if [[ $found -eq 0 ]]; then
      echo "Error: START_STAGE '${start_stage}' is not a valid stage." >&2
      exit 1
    fi
  fi
  
  if [[ -n "$end_stage" ]]; then
    local found=0
    for stage in "${PIPELINE_STAGES[@]}"; do
      if [[ "$stage" == "$end_stage" ]]; then
        found=1
        break
      fi
    done
    if [[ $found -eq 0 ]]; then
      echo "Error: END_STAGE '${end_stage}' is not a valid stage." >&2
      exit 1
    fi
  fi
}

should_skip() {
  local target_stage="$1"
  local start_stage="${START_STAGE:-}"
  local end_stage="${END_STAGE:-}"
  
  # If START_STAGE and END_STAGE are not set or empty, never skip
  if [[ -z "$start_stage" ]] && [[ -z "$end_stage" ]]; then
    return 1 # false (do not skip)
  fi

  # Find indices
  local start_idx=-1
  local end_idx=-1
  local target_idx=-1
  local i
  for i in "${!PIPELINE_STAGES[@]}"; do
    if [[ -n "$start_stage" ]] && [[ "${PIPELINE_STAGES[$i]}" == "$start_stage" ]]; then
      start_idx=$i
    fi
    if [[ -n "$end_stage" ]] && [[ "${PIPELINE_STAGES[$i]}" == "$end_stage" ]]; then
      end_idx=$i
    fi
    if [[ "${PIPELINE_STAGES[$i]}" == "$target_stage" ]]; then
      target_idx=$i
    fi
  done
  
  # If the target stage isn't found in the list, default to not skipping
  if [[ $target_idx -eq -1 ]]; then
    return 1
  fi
  
  # If the target is BEFORE the start stage, skip it
  if [[ -n "$start_stage" ]] && [[ $start_idx -ne -1 ]] && [[ $target_idx -lt $start_idx ]]; then
    return 0 # true (skip)
  fi
  
  # If the target is AFTER the end stage, skip it
  if [[ -n "$end_stage" ]] && [[ $end_idx -ne -1 ]] && [[ $target_idx -gt $end_idx ]]; then
    return 0 # true (skip)
  fi

  return 1 # false (do not skip)
}

# sbatch_submit <name> <time> <cpus> <mem> <array_spec_or_empty> <wrapper> <dep_jid_or_empty> [KEY=VAL ...]
sbatch_submit() {
  local name="$1" time="$2" cpus="$3" mem="$4" array="$5" wrapper="$6" dep="${7:-}"; shift 7
  
  if should_skip "$name"; then
    log "Skipping stage '${name}'"
    echo "SKIPPED"
    return 0
  fi
  
  local -a exports=( "$@" )

  # Build one --export argument (no newline, no trailing comma)
  local export_arg="--export=ALL"
  if ((${#exports[@]})); then
    local joined=""
    for kv in "${exports[@]}"; do
      joined+="${joined:+,}${kv}"
    done
    export_arg="--export=ALL${joined:+,${joined}}"
  fi

  # Assemble sbatch options; append wrapper last
  local -a cmd=( sbatch
                 --job-name="$name"
                 --time="$time"
                 --cpus-per-task="$cpus"
                 --mem="$mem"
                 --output="logs/${name}_%A_%a.out"
                 --error="logs/${name}_%A_%a.err" )
  [[ -n "$array" ]] && cmd+=( "--array=$array" )
  [[ -n "$dep" && "$dep" != "SKIPPED" ]] && cmd+=( "--dependency=afterok:${dep}" )
  cmd+=( "$export_arg" "$wrapper" )

  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    # Print the would-be command to STDERR so it doesn't get captured by $( ... )
    printf 'DRY sbatch:' >&2
    local token
    for token in "${cmd[@]}"; do
      if [[ "$token" =~ [[:space:]] ]]; then
        printf ' "%s"' "$token" >&2
      else
        printf ' %s' "$token" >&2
      fi
    done
    printf '\n' >&2

    # Return a deterministic fake JID on STDOUT for chaining
    local fake_jid="${__DRY_JID_SEQ:-490000}"
    __DRY_JID_SEQ=$(( fake_jid + 1 ))
    echo "${fake_jid}"
    return 0
  fi

  # Real submission path
  local jid
  jid=$("${cmd[@]}" | awk '{print $4}')
  if [[ "${VERBOSE:-0}" == "1" ]]; then
    log "Submitted stage '${name}' -> job ${jid}"
  fi
  echo "${jid}"
}


chain() {
  local jid="$1" label="$2"
  if [[ "$jid" == "SKIPPED" ]]; then
    echo "SKIPPED"
    return 0
  fi
  
  if [[ -z "$jid" ]]; then
    echo "sbatch not successful for ${label}. exiting"
    exit 1
  fi
  if [[ "${VERBOSE:-0}" == "1" ]]; then
    log "Stage '${label}' confirmed -> job ${jid}"
  fi
  echo "$jid"
}