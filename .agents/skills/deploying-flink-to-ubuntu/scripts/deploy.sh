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
SAVEPOINT_ACTUAL="" BUILD_REV=""
OLD_JOB_ID="" NEW_JOB_ID="" OLD_STATUS="not observed"
VERSIONED_JAR="" TEMP_JAR="" STAGE="arguments"
ACTIVE_IDS=() ACTIVE_STATES=()

fail() { printf 'Deployment failed: %s\n' "$*" >&2; exit 1; }

report_failure() {
  local status=$?
  if (( status != 0 )); then
    printf 'Failure stage: %s\nOld Job ID: %s\nOld job status (last observed): %s\nNew Job ID: %s\n' \
      "$STAGE" "${OLD_JOB_ID:-none}" "$OLD_STATUS" "${NEW_JOB_ID:-unknown}" >&2
    printf 'Manual rollback information: versioned JAR=%s; savepoint=%s; restore mode=%s. No automatic rollback was attempted.\n' \
      "${VERSIONED_JAR:-not uploaded}" "${SAVEPOINT_ACTUAL:-none}" "${RESTORE_MODE:-unset}" >&2
    printf 'The original JAR must be recovered from the previous deployment record; the versioned JAR above is the new artifact. Confirm its original arguments before manual rollback.\n' >&2
  fi
}
trap report_failure EXIT

validate_job_id() { [[ "$1" =~ ^[0-9a-f]{32}$ ]] || fail 'invalid Job ID'; }
normalize_savepoint_uri() {
  local uri=$1 path relative
  # Keep the reported URI even if validation fails after the old job was stopped.
  SAVEPOINT_ACTUAL=$uri
  [[ "$uri" == file:* ]] || fail 'invalid savepoint URI'
  path=${uri#file:}
  if [[ "$path" == ///* ]]; then path=${path#//}; fi
  [[ "$path" =~ ^/home/zhangdi/workspace/flink_data/savepoints/[A-Za-z0-9._/-]+$ ]] || fail 'invalid savepoint URI'
  relative=${path#"$REMOTE_SAVEPOINT_DIR/"}
  # Lexically safe characters alone do not prevent traversal outside the allowlist.
  case "/$relative/" in *'/../'*|*'/./'*|*'//'*) fail 'invalid savepoint path segment' ;; esac
  SAVEPOINT_URI="file://$path"
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
        normalize_savepoint_uri "$2"; shift 2 ;;
      *) fail "unknown argument: $1" ;;
    esac
  done
}

verify_local_build() {
  STAGE="local build"
  verify_clean_worktree
  BUILD_REV=$(git rev-parse --short=12 HEAD) || fail 'cannot determine Git revision'
  [[ "$BUILD_REV" =~ ^[0-9a-f]{12,40}$ ]] || fail 'invalid Git revision'
  mvn -f flink_processer/pom.xml clean verify || fail 'mvn clean verify failed'
  verify_build_revision
  [[ -s "$LOCAL_JAR" ]] || fail 'built JAR missing or empty'
  local entries expected
  entries=$(jar tf "$LOCAL_JAR") || fail 'cannot inspect built JAR'
  for expected in com/example/flinkprocesser/EventEnrichmentJob.class \
    org/apache/flink/connector/kafka/source/KafkaSource.class com/fasterxml/jackson/databind/ObjectMapper.class; do
    grep -Fx -- "$expected" <<< "$entries" >/dev/null || fail "fat JAR missing $expected"
  done
}

verify_clean_worktree() {
  local changes
  changes=$(git status --porcelain --untracked-files=normal) || fail 'cannot inspect Git worktree'
  [[ -z "$changes" ]] || fail 'commit or remove local changes before building a versioned deployment'
}

verify_build_revision() {
  local revision
  verify_clean_worktree
  revision=$(git rev-parse --short=12 HEAD) || fail 'cannot determine Git revision'
  [[ "$revision" == "$BUILD_REV" ]] || fail 'Git revision changed during deployment build'
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
  (remote_exec 'flink list --running --scheduled') > "$output" &
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

matching_jobs() {
  awk -F ' : ' -v job="$JOB_NAME" '
    {gsub(/^[ \t]+|[ \t]+$/, "", $3); gsub(/^[ \t]+|[ \t]+$/, "", $2)}
    index($3, job " (") == 1 && $3 ~ /\([A-Z]+\)$/ {
      state=substr($3, length(job)+3); sub(/\)$/, "", state)
      if (state != "FINISHED" && state != "FAILED" && state != "CANCELED")
        print ($2 == "" ? "invalid-empty-job-id" : $2), state
    }
  '
}

read_running_jobs() {
  STAGE="remote read-only preflight"
  remote "test -x $REMOTE_JAVA_HOME/bin/java && test -x $REMOTE_FLINK_HOME/bin/flink && test -d $REMOTE_DATA_DIR && test -w $REMOTE_DATA_DIR && java -version && flink --version" || fail 'remote environment preflight failed'
  local listing id state jobs
  listing=$(remote 'flink list --running --scheduled') || fail 'cannot read active jobs'
  jobs=$(matching_jobs <<< "$listing")
  while read -r id state; do
    [[ -n "$id" ]] || continue
    validate_job_id "$id"
    ACTIVE_IDS+=("$id"); ACTIVE_STATES+=("$state")
  done <<< "$jobs"
  if (( ${#ACTIVE_IDS[@]} == 1 )); then
    OLD_JOB_ID=${ACTIVE_IDS[0]}; OLD_STATUS=${ACTIVE_STATES[0]}
  elif (( ${#ACTIVE_IDS[@]} == 0 )); then
    OLD_STATUS="no matching active job"
  else
    OLD_STATUS="multiple matching active jobs"
  fi
  printf 'Read-only preflight: %s matching active job(s) for %s; status=%s\n' "${#ACTIVE_IDS[@]}" "$JOB_NAME" "$OLD_STATUS"
}

verify_execution_gates() {
  STAGE="execution gates"
  (( ${#ACTIVE_IDS[@]} <= 1 )) || fail 'multiple matching active jobs; choose explicitly outside this script'
  if (( ${#ACTIVE_IDS[@]} == 1 )); then
    [[ "$OLD_STATUS" == RUNNING ]] || fail "matching job is $OLD_STATUS; wait for a stable RUNNING state before deployment"
  fi
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
  local local_result local_hash remote_result remote_hash existing
  verify_build_revision
  VERSIONED_JAR="$REMOTE_DATA_DIR/flink-processer-$BUILD_REV.jar"
  TEMP_JAR="$REMOTE_DATA_DIR/.flink-processer-$BUILD_REV-$$.jar.tmp"
  local_result=$(shasum -a 256 "$LOCAL_JAR") || fail 'local SHA-256 failed'
  local_hash=${local_result%% *}
  [[ "$local_hash" =~ ^[0-9a-f]{64}$ ]] || fail 'invalid local SHA-256'
  existing=$(remote "if test -e $VERSIONED_JAR || test -L $VERSIONED_JAR; then sha256sum $VERSIONED_JAR; else printf 'absent\\n'; fi") || fail 'cannot inspect existing versioned JAR'
  if [[ "$existing" != absent ]]; then
    remote_hash=${existing%% *}
    [[ "$remote_hash" =~ ^[0-9a-f]{64}$ && "$remote_hash" == "$local_hash" ]] || fail 'existing versioned JAR SHA-256 differs; old job was not stopped'
    printf 'Reusing verified versioned JAR: %s\n' "$VERSIONED_JAR"
    return 0
  fi
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
  # Same-directory hard link publishes atomically and fails if a concurrent deploy
  # created this name. It can never replace an artifact retained for rollback.
  remote "ln -T $TEMP_JAR $VERSIONED_JAR" || { cleanup_temp_upload; fail 'atomic upload installation failed; versioned path was not overwritten'; }
  cleanup_temp_upload
}

stop_with_savepoint() {
  [[ -n "$OLD_JOB_ID" ]] || return 0
  STAGE="stop with savepoint"
  local output uri
  OLD_STATUS="stop requested; terminal state unverified"
  output=$(remote "flink stop --savepointPath $REMOTE_SAVEPOINT_DIR $OLD_JOB_ID") || fail 'savepoint stop failed; inspect old job state'
  OLD_STATUS="stop accepted; terminal state unverified"
  uri=$(sed -n 's/^Savepoint completed\. Path: //p' <<< "$output")
  normalize_savepoint_uri "$uri"
  remote "test -r ${SAVEPOINT_URI#file://}/_metadata" || fail 'generated savepoint _metadata is not readable'
}

cancel_without_restore() {
  [[ -n "$OLD_JOB_ID" ]] || return 0
  STAGE="cancel without restore"
  OLD_STATUS="cancel requested; terminal state unverified"
  remote "flink cancel $OLD_JOB_ID" || fail 'cancel failed; inspect old job state'
  OLD_STATUS="cancel accepted; terminal state unverified"
}

submit_job() {
  STAGE="submit new job"
  local output command
  local restore_args=()
  if [[ "$RESTORE_MODE" == savepoint ]]; then
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
  local attempt listing jobs id state count new_running old_active deadline=$((SECONDS + 30))
  for (( attempt=0; attempt<30 && SECONDS<deadline; attempt++ )); do
    local poll_status=0
    listing=$(poll_running_jobs "$deadline") || poll_status=$?
    if (( poll_status == 124 )); then fail 'new job observation exceeded the 30-second polling window'; fi
    (( poll_status == 0 )) || fail 'cannot observe new job state'
    count=0 new_running=false old_active=false
    jobs=$(matching_jobs <<< "$listing")
    while read -r id state; do
      [[ -n "$id" ]] || continue
      validate_job_id "$id"
      count=$((count + 1))
      if [[ "$id" == "$OLD_JOB_ID" ]]; then
        old_active=true; OLD_STATUS=$state
      fi
      if [[ "$id" == "$NEW_JOB_ID" && "$state" == RUNNING ]]; then new_running=true; fi
    done <<< "$jobs"
    if [[ -n "$OLD_JOB_ID" && "$old_active" == false ]]; then OLD_STATUS="not active"; fi
    if [[ "$new_running" == true && "$old_active" == false && "$count" == 1 ]]; then
      printf 'Deployment successful: Job ID %s is RUNNING; JAR=%s\n' "$NEW_JOB_ID" "$VERSIONED_JAR"
      printf 'Old job status (last observed): %s\n' "$OLD_STATUS"
      printf 'Restore mode: %s\nSavepoint: %s\n' "$RESTORE_MODE" "${SAVEPOINT_URI:-not used}"
      return 0
    fi
    if (( attempt<29 && SECONDS<deadline )); then sleep 1; fi
  done
  fail 'new Job ID was not uniquely RUNNING with the old job inactive within the 30-second polling window'
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
