#!/usr/bin/env bash
set -euo pipefail

# --confirmed is an explicit per-invocation gate, not persisted authorization.
SSH_HOST="ubuntu"
REMOTE_DATA_DIR="/home/zhangdi/workspace/flink_data"
REMOTE_FLINK_HOME="/home/zhangdi/works/flink-2.3.0"
REMOTE_JAVA_HOME="/home/zhangdi/works/jdk-21.0.12.1+1"
REMOTE_SAVEPOINT_DIR="$REMOTE_DATA_DIR/savepoints"
JOB_NAME="mobile-event-common-enrichment"
LOCAL_JAR="flink_processer/target/flink-processer-1.0.0.jar"
EXECUTE=false CONFIRMED=false RESTORE_MODE="" SAVEPOINT_URI=""
OLD_JOB_ID="" NEW_JOB_ID="" OLD_STATUS="not observed"
VERSIONED_JAR="" TEMP_JAR="" STAGE="arguments"
RUNNING_IDS=()

fail() { printf 'Deployment failed: %s\n' "$*" >&2; exit 1; }

report_failure() {
  local status=$?
  if (( status != 0 )); then
    printf 'Failure stage: %s\nOld Job ID: %s\nOld job status (last observed): %s\nNew Job ID: %s\n' \
      "$STAGE" "${OLD_JOB_ID:-none}" "$OLD_STATUS" "${NEW_JOB_ID:-unknown}" >&2
    printf 'Manual rollback information: versioned JAR=%s; savepoint=%s; restore mode=%s. No automatic rollback was attempted.\n' \
      "${VERSIONED_JAR:-not uploaded}" "${SAVEPOINT_URI:-none}" "${RESTORE_MODE:-unset}" >&2
    printf 'The original JAR must be recovered from the previous deployment record; the versioned JAR above is the new artifact. Confirm its original arguments before manual rollback.\n' >&2
  fi
}
trap report_failure EXIT

validate_job_id() { [[ "$1" =~ ^[0-9a-f]{32}$ ]] || fail 'invalid Job ID'; }
validate_savepoint_uri() {
  local uri=$1 relative
  [[ "$uri" =~ ^file:///home/zhangdi/workspace/flink_data/savepoints/[A-Za-z0-9._/-]+$ ]] || fail 'invalid savepoint URI'
  relative=${uri#"file://$REMOTE_SAVEPOINT_DIR/"}
  # Lexically safe characters alone do not prevent traversal outside the allowlist.
  case "/$relative/" in *'/../'*|*'/./'*|*'//'*) fail 'invalid savepoint path segment' ;; esac
}

parse_args() {
  while (( $# )); do
    case "$1" in
      --execute) EXECUTE=true; shift ;;
      --confirmed) CONFIRMED=true; shift ;;
      --restore-mode)
        (( $# >= 2 )) || fail '--restore-mode needs a value'
        [[ -z "$RESTORE_MODE" ]] || fail 'duplicate --restore-mode'
        RESTORE_MODE=$2; shift 2 ;;
      --savepoint-path)
        (( $# >= 2 )) || fail '--savepoint-path needs a value'
        [[ -z "$SAVEPOINT_URI" ]] || fail 'duplicate --savepoint-path'
        SAVEPOINT_URI=$2; validate_savepoint_uri "$SAVEPOINT_URI"; shift 2 ;;
      *) fail "unknown argument: $1" ;;
    esac
  done
}

verify_local_build() {
  STAGE="local build"
  mvn -f flink_processer/pom.xml clean verify || fail 'mvn clean verify failed'
  [[ -s "$LOCAL_JAR" ]] || fail 'built JAR missing or empty'
  local entries expected
  entries=$(jar tf "$LOCAL_JAR") || fail 'cannot inspect built JAR'
  for expected in com/example/flinkprocesser/EventEnrichmentJob.class \
    org/apache/flink/connector/kafka/source/KafkaSource.class com/fasterxml/jackson/databind/ObjectMapper.class; do
    grep -Fx -- "$expected" <<< "$entries" >/dev/null || fail "fat JAR missing $expected"
  done
}

# Only constants or separately allowlisted values reach this remote shell string.
remote_exec() {
  exec ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 "$SSH_HOST" \
    "export JAVA_HOME=$REMOTE_JAVA_HOME; export PATH=$REMOTE_FLINK_HOME/bin:$REMOTE_JAVA_HOME/bin:\"\$PATH\"; $1"
}
remote() { (remote_exec "$1"); }

# Bound the SSH process itself, since a hung Flink CLI can outlive a loop timer.
poll_running_jobs() {
  local deadline=$1 output pid status=0
  output=$(mktemp) || return 1
  (remote_exec 'flink list --running') > "$output" &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      kill -TERM "$pid" 2>/dev/null || true
      kill -KILL "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      rm -f "$output"
      return 124
    fi
    sleep 0.1
  done
  wait "$pid" || status=$?
  cat "$output"
  rm -f "$output"
  return "$status"
}

matching_ids() {
  awk -F ' : ' -v job="$JOB_NAME" '
    {gsub(/^[ \t]+|[ \t]+$/, "", $3); gsub(/^[ \t]+|[ \t]+$/, "", $2)}
    $3 == job " (RUNNING)" {print ($2 == "" ? "invalid-empty-job-id" : $2)}
  '
}

read_running_jobs() {
  STAGE="remote read-only preflight"
  remote "test -x $REMOTE_JAVA_HOME/bin/java && test -x $REMOTE_FLINK_HOME/bin/flink && test -d $REMOTE_DATA_DIR && test -w $REMOTE_DATA_DIR && java -version && flink --version" || fail 'remote environment preflight failed'
  local listing id ids
  listing=$(remote 'flink list --running') || fail 'cannot read running jobs'
  ids=$(matching_ids <<< "$listing")
  while IFS= read -r id; do
    [[ -n "$id" ]] || continue
    validate_job_id "$id"
    RUNNING_IDS+=("$id")
  done <<< "$ids"
  if (( ${#RUNNING_IDS[@]} == 1 )); then
    OLD_JOB_ID=${RUNNING_IDS[0]}; OLD_STATUS=RUNNING
  elif (( ${#RUNNING_IDS[@]} == 0 )); then
    OLD_STATUS="no matching RUNNING job"
  else
    OLD_STATUS="multiple matching RUNNING jobs"
  fi
  printf 'Read-only preflight: %s matching RUNNING job(s) for %s\n' "${#RUNNING_IDS[@]}" "$JOB_NAME"
}

verify_execution_gates() {
  STAGE="execution gates"
  (( ${#RUNNING_IDS[@]} <= 1 )) || fail 'multiple matching running jobs; choose explicitly outside this script'
  if [[ "$EXECUTE" != true ]]; then
    [[ "$CONFIRMED" == false && -z "$RESTORE_MODE" && -z "$SAVEPOINT_URI" ]] || fail 'deployment flags require --execute'
    return
  fi
  [[ "$CONFIRMED" == true ]] || fail '--execute requires fresh confirmation via --confirmed'
  case "$RESTORE_MODE" in
    savepoint)
      if [[ -z "$OLD_JOB_ID" ]]; then
        [[ -n "$SAVEPOINT_URI" ]] || fail 'no running job: --savepoint-path is required'
        remote "test -r ${SAVEPOINT_URI#file://}/_metadata" || fail 'savepoint _metadata is not readable'
      fi ;;
    stateless) [[ -z "$SAVEPOINT_URI" ]] || fail 'stateless mode does not accept --savepoint-path' ;;
    *) fail '--execute requires --restore-mode savepoint or stateless' ;;
  esac
}

cleanup_temp_upload() {
  remote "rm -f $TEMP_JAR" || printf 'Temporary upload cleanup failed: %s\n' "$TEMP_JAR" >&2
}

upload_and_verify() {
  STAGE="upload and SHA-256 verification"
  local sha local_result local_hash remote_result remote_hash
  sha=$(git rev-parse --short=12 HEAD) || fail 'cannot determine Git revision'
  [[ "$sha" =~ ^[0-9a-f]{12,40}$ ]] || fail 'invalid Git revision'
  VERSIONED_JAR="$REMOTE_DATA_DIR/flink-processer-$sha.jar"
  TEMP_JAR="$REMOTE_DATA_DIR/.flink-processer-$sha-$$.jar.tmp"
  local_result=$(shasum -a 256 "$LOCAL_JAR") || fail 'local SHA-256 failed'
  local_hash=${local_result%% *}
  [[ "$local_hash" =~ ^[0-9a-f]{64}$ ]] || fail 'invalid local SHA-256'
  if ! scp -o BatchMode=yes -o ConnectTimeout=10 "$LOCAL_JAR" "$SSH_HOST:$TEMP_JAR"; then
    cleanup_temp_upload; fail 'upload failed'
  fi
  if ! remote_result=$(remote "sha256sum $TEMP_JAR"); then
    cleanup_temp_upload; fail 'remote SHA-256 failed'
  fi
  remote_hash=${remote_result%% *}
  if [[ ! "$remote_hash" =~ ^[0-9a-f]{64}$ || "$remote_hash" != "$local_hash" ]]; then
    cleanup_temp_upload; fail 'upload SHA-256 mismatch; old job was not stopped'
  fi
  remote "mv -f $TEMP_JAR $VERSIONED_JAR" || { cleanup_temp_upload; fail 'atomic upload move failed'; }
}

stop_with_savepoint() {
  [[ -n "$OLD_JOB_ID" ]] || return 0
  STAGE="stop with savepoint"
  local output uri
  OLD_STATUS="stop requested; terminal state unverified"
  output=$(remote "flink stop --savepointPath $REMOTE_SAVEPOINT_DIR $OLD_JOB_ID") || fail 'savepoint stop failed; inspect old job state'
  OLD_STATUS="stopped with savepoint"
  uri=$(sed -n 's/^Savepoint completed\. Path: //p' <<< "$output")
  validate_savepoint_uri "$uri"
  SAVEPOINT_URI=$uri
  remote "test -r ${SAVEPOINT_URI#file://}/_metadata" || fail 'generated savepoint _metadata is not readable'
}

cancel_without_restore() {
  [[ -n "$OLD_JOB_ID" ]] || return 0
  STAGE="cancel without restore"
  OLD_STATUS="cancel requested; terminal state unverified"
  remote "flink cancel $OLD_JOB_ID" || fail 'cancel failed; inspect old job state'
  OLD_STATUS="cancelled without savepoint"
}

submit_job() {
  STAGE="submit new job"
  local output command
  local restore_args=()
  if [[ "$RESTORE_MODE" == savepoint ]]; then
    validate_savepoint_uri "$SAVEPOINT_URI"
    restore_args=(-s "$SAVEPOINT_URI")
  fi
  command="flink run --detached"
  if (( ${#restore_args[@]} )); then command+=" ${restore_args[*]}"; fi
  command+=" -c com.example.flinkprocesser.EventEnrichmentJob $VERSIONED_JAR"
  command+=" --bootstrap-servers 192.168.250.106:9092 --input-topic flink_demo --output-topic flink_output --error-topic flink_error --group-id flink-processer --parallelism 1 --checkpoint-interval-ms 60000"
  command+=" --checkpoint-storage-uri file://$REMOTE_DATA_DIR/checkpoints --savepoint-storage-uri file://$REMOTE_SAVEPOINT_DIR"
  output=$(remote "$command") || fail 'new job submission failed'
  NEW_JOB_ID=$(sed -n 's/^Job has been submitted with JobID //p' <<< "$output")
  validate_job_id "$NEW_JOB_ID"
}

wait_until_running() {
  STAGE="wait for new RUNNING job"
  local attempt listing ids id deadline=$((SECONDS + 30))
  for (( attempt=0; attempt<30 && SECONDS<deadline; attempt++ )); do
    local poll_status=0
    listing=$(poll_running_jobs "$deadline") || poll_status=$?
    if (( poll_status == 124 )); then fail 'new job observation exceeded the 30-second polling window'; fi
    (( poll_status == 0 )) || fail 'cannot observe new job state'
    ids=$(matching_ids <<< "$listing")
    while IFS= read -r id; do
      [[ -n "$id" ]] || continue
      validate_job_id "$id"
      if [[ "$id" == "$NEW_JOB_ID" ]]; then
        printf 'Deployment successful: Job ID %s is RUNNING; JAR=%s\n' "$NEW_JOB_ID" "$VERSIONED_JAR"
        return 0
      fi
    done <<< "$ids"
    if (( attempt<29 && SECONDS<deadline )); then sleep 1; fi
  done
  fail 'new Job ID did not enter RUNNING within the 30-second polling window'
}

main() {
  parse_args "$@"
  cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
  verify_local_build
  read_running_jobs
  verify_execution_gates
  [[ "$EXECUTE" == true ]] || return 0
  upload_and_verify
  case "$RESTORE_MODE" in
    savepoint) stop_with_savepoint ;;
    stateless) cancel_without_restore ;;
  esac
  submit_job
  wait_until_running
}
main "$@"
