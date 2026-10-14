#!/usr/bin/env bash
set -euo pipefail

# Boundary fakes replace build/network commands. No command reaches real SSH/SCP.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$SCRIPT_DIR/../scripts/deploy.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
OLD=11111111111111111111111111111111
NEW=22222222222222222222222222222222
DATA=/home/zhangdi/workspace/flink_data
RESTORE="file://$DATA/savepoints/savepoint-generated"
PROVIDED="file://$DATA/savepoints/manual-recovery-20261014"
passed=0 failed=0

setup() {
  CASE="$TMP/$1"
  mkdir -p "$CASE/bin" "$CASE/repo/.agents/skills/deploying-flink-to-ubuntu/scripts" "$CASE/repo/flink_processer/target"
  if [[ -f "$SOURCE" ]]; then cp "$SOURCE" "$CASE/repo/.agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh"; fi
  export CALL_LOG="$CASE/calls" FAKE_STATE="$CASE/state"
  : > "$CALL_LOG"
  export FAKE_JOBS=one FAKE_CHECKSUM=ok FAKE_NEW_STATE=RUNNING FAKE_METADATA=ok FAKE_BUILD=ok FAKE_JAR=ok FAKE_POLL=ok
  export FAKE_STOP_URI="$RESTORE" FAKE_SUBMIT_ID="$NEW" FAKE_OLD_ID="$OLD" FAKE_SHA=abcdef123456 FAKE_SCP=ok FAKE_STOP=ok FAKE_MOVE=ok FAKE_SUBMIT=ok FAKE_CANCEL=ok FAKE_PREFLIGHT=ok
  export FAKE_DIRTY=clean FAKE_EXISTING=absent FAKE_OLD_POLL=gone FAKE_OTHER_JOB=no FAKE_HEAD_CHANGE=no
  cat > "$CASE/bin/dispatch" <<'FAKE'
#!/bin/bash
set -eu
name=${0##*/}
printf '%s %s\n' "$name" "$*" >> "$CALL_LOG"
case "$name" in
  mvn)
    [[ "$FAKE_BUILD" == ok ]] || exit 1
    : > "$FAKE_STATE.build"
    printf 'fake jar\n' > flink_processer/target/flink-processer-1.0.0.jar ;;
  jar)
    printf '%s\n' com/example/flinkprocesser/EventEnrichmentJob.class
    if [[ "$FAKE_JAR" == ok ]]; then
      printf '%s\n' org/apache/flink/connector/kafka/source/KafkaSource.class com/fasterxml/jackson/databind/ObjectMapper.class
    fi ;;
  git)
    case "$1" in
      status)
        if [[ "$FAKE_DIRTY" == before || ( "$FAKE_DIRTY" == after && -f "$FAKE_STATE.build" ) ]]; then printf ' M source.java\n'; fi ;;
      rev-parse)
        if [[ "$FAKE_HEAD_CHANGE" == yes && -f "$FAKE_STATE.build" ]]; then printf 'abcdef654321\n'; else printf '%s\n' "$FAKE_SHA"; fi ;;
      *) exit 92 ;;
    esac ;;
  shasum) printf '%064d  %s\n' 0 "${@: -1}" ;;
  scp) [[ "$FAKE_SCP" == ok ]] ;;
  sleep) : ;;
  ssh)
    remote=${@: -1}
    case "$remote" in
      *'flink list --running'*)
        if [[ -f "$FAKE_STATE" && "$FAKE_POLL" == hang ]]; then
          trap 'exit 143' TERM
          # A broken production timeout must still let the test suite terminate.
          deadline=$((SECONDS + 35))
          while (( SECONDS < deadline )); do :; done
          exit 1
        fi
        printf '%s\n' 'Waiting for response...' '------------------ Running/Restarting Jobs -------------------'
        if [[ -f "$FAKE_STATE" ]]; then
          count=0
          if [[ -f "$FAKE_STATE.poll" ]]; then count=$(cat "$FAKE_STATE.poll"); fi
          count=$((count + 1)); printf '%s\n' "$count" > "$FAKE_STATE.poll"
          if [[ "$FAKE_NEW_STATE" == RUNNING || "$FAKE_NEW_STATE" == RESTARTING ]]; then
            printf '14.10.2026 10:00:00 : 22222222222222222222222222222222 : mobile-event-common-enrichment (%s)\n' "$FAKE_NEW_STATE"
          fi
          if [[ "$FAKE_OLD_POLL" == persistent || ( "$FAKE_OLD_POLL" == delayed && "$count" -le 2 ) ]]; then
            printf '14.10.2026 10:00:00 : 11111111111111111111111111111111 : mobile-event-common-enrichment (RUNNING)\n'
          elif [[ "$FAKE_OLD_POLL" == restarting ]]; then
            printf '14.10.2026 10:00:00 : 11111111111111111111111111111111 : mobile-event-common-enrichment (RESTARTING)\n'
          fi
          if [[ "$FAKE_OTHER_JOB" == yes ]]; then printf '14.10.2026 10:00:00 : 33333333333333333333333333333333 : mobile-event-common-enrichment (RESTARTING)\n'; fi
        else
          case "$FAKE_JOBS" in
            one|two|mixed) printf '14.10.2026 10:00:00 : %s : mobile-event-common-enrichment (RUNNING)\n' "$FAKE_OLD_ID" ;;
            restarting) printf '14.10.2026 10:00:00 : %s : mobile-event-common-enrichment (RESTARTING)\n' "$FAKE_OLD_ID" ;;
            unrelated) printf '14.10.2026 10:00:00 : 33333333333333333333333333333333 : mobile-event-common-enrichment-other (RUNNING)\n' ;;
          esac
          if [[ "$FAKE_JOBS" == two ]]; then printf '14.10.2026 10:00:00 : 33333333333333333333333333333333 : mobile-event-common-enrichment (RUNNING)\n'; fi
          if [[ "$FAKE_JOBS" == mixed ]]; then printf '14.10.2026 10:00:00 : 33333333333333333333333333333333 : mobile-event-common-enrichment (RESTARTING)\n'; fi
        fi ;;
      *'if test -e '*|*'if test -L '*)
        case "$FAKE_EXISTING" in
          absent) printf 'absent\n' ;;
          same) printf '%064d  existing\n' 0 ;;
          different) printf '%064d  existing\n' 1 ;;
          error) exit 1 ;;
        esac ;;
      *'sha256sum '*)
        if [[ "$FAKE_CHECKSUM" == ok ]]; then printf '%064d  upload\n' 0; else printf '%064d  upload\n' 1; fi ;;
      *'flink stop '*)
        [[ "$FAKE_STOP" == ok ]] || exit 1
        printf 'Savepoint completed. Path: %s\n' "$FAKE_STOP_URI" ;;
      *'flink cancel '*) [[ "$FAKE_CANCEL" == ok ]] ;;
      *'flink run --detached '*)
        [[ "$FAKE_SUBMIT" == ok ]] || exit 1
        : > "$FAKE_STATE"
        printf 'Job has been submitted with JobID %s\n' "$FAKE_SUBMIT_ID" ;;
      *'test -r '*'/savepoints/'*'/_metadata'*) [[ "$FAKE_METADATA" == ok ]] ;;
      *'mv '*|*'ln '*) [[ "$FAKE_MOVE" == ok ]] ;;
      *'rm -f '*) : ;;
      *'test -x '*|*'flink --version'*) [[ "$FAKE_PREFLIGHT" == ok ]] || exit 1; printf 'Flink 2.3.0; Java 21\n' ;;
      *) printf 'Unexpected fake SSH command: %s\n' "$remote" >&2; exit 90 ;;
    esac ;;
  *) exit 91 ;;
esac
FAKE
  chmod +x "$CASE/bin/dispatch"
  local tool
  for tool in mvn jar git shasum ssh scp sleep; do ln -s dispatch "$CASE/bin/$tool"; done
  # Only explicitly allowed local utilities are reachable via PATH.
  for tool in bash dirname grep sed awk head tail tr wc mkdir rm cat mktemp; do
    ln -s "$(command -v "$tool")" "$CASE/bin/$tool"
  done
}

run_deploy() {
  local code=0
  (cd "$CASE/repo" && PATH="$CASE/bin" /bin/bash .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh "$@") > "$CASE/output" 2>&1 || code=$?
  STATUS=$code
}
assert_ok() { [[ "$STATUS" == 0 ]] || { cat "$CASE/output"; return 1; }; }
assert_failed() { [[ "$STATUS" != 0 && "$STATUS" != 127 ]] || { printf 'Expected deployment failure, got status %s\n' "$STATUS"; cat "$CASE/output"; return 1; }; }
assert_log_contains() { grep -F -- "$1" "$CALL_LOG" >/dev/null || { printf 'Missing command: %s\n' "$1"; cat "$CALL_LOG"; return 1; }; }
assert_log_not_contains() { if grep -F -- "$1" "$CALL_LOG" >/dev/null; then printf 'Forbidden command: %s\n' "$1"; return 1; fi; }
assert_no_mutation() {
  local word
  for word in 'scp ' 'flink stop ' 'flink cancel ' 'flink run ' 'mv ' 'ln ' 'rm -f ' 'mkdir '; do assert_log_not_contains "$word" || return 1; done
}
assert_before() {
  local left right
  left=$(grep -nF -- "$1" "$CALL_LOG" | head -1 | cut -d: -f1)
  right=$(grep -nF -- "$2" "$CALL_LOG" | head -1 | cut -d: -f1)
  [[ -n "$left" && -n "$right" && "$left" -lt "$right" ]] || { cat "$CALL_LOG"; return 1; }
}
execute() { run_deploy --execute --confirmed --restore-mode "$@"; }

# Each scenario catches a concrete unsafe command, missing gate, or wrong branch.
test_preflight_never_mutates_remote() { run_deploy; assert_ok; assert_log_contains 'mvn -f flink_processer/pom.xml clean verify'; assert_log_contains 'flink list --running'; assert_no_mutation; }
test_execute_requires_confirmation_flag() { run_deploy --execute --restore-mode stateless; assert_failed; assert_no_mutation; }
test_execute_requires_restore_mode() { run_deploy --execute --confirmed; assert_failed; assert_no_mutation; }
test_invalid_restore_mode_never_uploads() { execute anything; assert_failed; assert_no_mutation; }
test_multiple_matching_jobs_abort_before_upload() { export FAKE_JOBS=two; execute stateless; assert_failed; assert_no_mutation; }
test_savepoint_mode_stops_without_drain_and_restores_with_s() {
  execute savepoint; assert_ok
  assert_log_contains "flink stop --savepointPath $DATA/savepoints $OLD"
  assert_log_not_contains '--drain'
  assert_log_contains "flink run --detached -s $RESTORE -c com.example.flinkprocesser.EventEnrichmentJob $DATA/flink-processer-abcdef123456.jar"
  assert_before 'sha256sum ' 'ln '; assert_before 'ln ' 'flink stop '; assert_before 'flink stop ' 'flink run '
}
test_stateless_mode_cancels_and_submits_without_s() {
  execute stateless; assert_ok; assert_log_contains "flink cancel $OLD"; assert_log_not_contains ' -s '
  assert_log_not_contains '_metadata'; assert_log_not_contains 'flink stop '; assert_before 'ln ' 'flink cancel '; assert_before 'flink cancel ' 'flink run '
  assert_log_contains '--bootstrap-servers 192.168.250.106:9092 --input-topic flink_demo --output-topic flink_output --error-topic flink_error --group-id flink-processer --parallelism 1 --checkpoint-interval-ms 60000'
  assert_log_contains "--checkpoint-storage-uri file://$DATA/checkpoints --savepoint-storage-uri file://$DATA/savepoints"
  assert_log_contains 'export JAVA_HOME=/home/zhangdi/works/jdk-21.0.12.1+1; export PATH=/home/zhangdi/works/flink-2.3.0/bin:/home/zhangdi/works/jdk-21.0.12.1+1/bin:"$PATH"'
}
test_checksum_mismatch_keeps_old_job_running() {
  export FAKE_CHECKSUM=bad; execute stateless; assert_failed
  assert_log_contains 'scp '; assert_log_contains "rm -f $DATA/.flink-processer-abcdef123456-"
  assert_log_not_contains 'mv '; assert_log_not_contains 'ln '; assert_log_not_contains 'flink cancel '; assert_log_not_contains 'flink stop '; assert_log_not_contains 'flink run '
}
test_missing_running_job_requires_explicit_savepoint_path() { export FAKE_JOBS=none; execute savepoint; assert_failed; assert_no_mutation; }
test_explicit_savepoint_is_verified_and_restored() {
  export FAKE_JOBS=none; execute savepoint --savepoint-path "$PROVIDED"; assert_ok
  assert_log_contains "test -r $DATA/savepoints/manual-recovery-20261014/_metadata"
  assert_before 'test -r ' 'scp '; assert_log_contains "flink run --detached -s $PROVIDED"
  assert_log_not_contains 'flink stop '; assert_log_not_contains 'flink cancel '
}
test_unreadable_savepoint_aborts_before_upload() { export FAKE_JOBS=none FAKE_METADATA=bad; execute savepoint --savepoint-path "$RESTORE"; assert_failed; assert_no_mutation; }
test_stateless_without_job_does_not_read_savepoints() { export FAKE_JOBS=none; execute stateless; assert_ok; assert_log_not_contains '_metadata'; assert_log_not_contains ' -s '; assert_log_not_contains 'flink cancel '; }
test_stateless_rejects_savepoint_flag() { execute stateless --savepoint-path "$RESTORE"; assert_failed; assert_no_mutation; }
test_new_job_not_running_reports_failure_without_automatic_rollback() {
  export FAKE_NEW_STATE=FAILED; execute stateless; assert_failed
  [[ $(grep -c 'flink run ' "$CALL_LOG") == 1 ]]
  [[ $(grep -c 'flink cancel ' "$CALL_LOG") == 1 ]]
  [[ $(grep -c 'flink list --running' "$CALL_LOG") == 31 ]]
  grep -F "$OLD" "$CASE/output" >/dev/null; grep -F 'rollback' "$CASE/output" >/dev/null
}
test_hanging_poll_is_bounded_to_thirty_seconds() {
  export FAKE_POLL=hang
  local started=$SECONDS
  execute stateless; assert_failed
  (( SECONDS-started <= 32 ))
  [[ $(grep -c 'flink run ' "$CALL_LOG") == 1 ]]
  grep -F '30-second' "$CASE/output" >/dev/null
}
test_rejects_unsafe_savepoint_uris() {
  local uri
  for uri in 'file:///tmp/savepoint' "$RESTORE;touch /tmp/injected" "file://$DATA/savepoints/../outside" "file://$DATA/savepoints/x/../../outside" "$RESTORE\$(id)" "file://$DATA/savepoints/" "file:$DATA/savepoints/../outside" "file://ubuntu$DATA/savepoints/x" "file:$DATA/savepoints/%2e%2e/x" "file:$DATA/savepoints/x//y" "file:$DATA/savepoints/./x" "file:$DATA/savepoints/x?query"; do
    run_deploy --execute --confirmed --restore-mode savepoint --savepoint-path "$uri"; assert_failed; assert_no_mutation
  done
}
test_rejects_malformed_job_id_before_upload() { export FAKE_OLD_ID='1111111111111111111111111111111;'; execute stateless; assert_failed; assert_no_mutation; }
test_rejects_empty_job_id_before_upload() { export FAKE_OLD_ID=''; execute stateless; assert_failed; assert_no_mutation; }
test_rejects_malformed_git_sha_before_upload() { export FAKE_SHA='abcdef;id'; execute stateless; assert_failed; assert_no_mutation; }
test_bad_generated_savepoint_never_submits() { export FAKE_STOP_URI='file:///tmp/savepoint'; execute savepoint; assert_failed; assert_log_contains 'flink stop '; assert_log_not_contains 'flink run '; }
test_flink_single_slash_savepoint_is_normalized_and_restored() {
  export FAKE_STOP_URI="file:$DATA/savepoints/savepoint-generated"
  execute savepoint; assert_ok
  assert_log_contains "test -r $DATA/savepoints/savepoint-generated/_metadata"
  assert_log_contains "flink run --detached -s $RESTORE"
  grep -Fx "Savepoint: $RESTORE" "$CASE/output" >/dev/null
}
test_generated_uri_is_retained_in_failure_report() {
  export FAKE_STOP_URI="file:$DATA/savepoints/invalid%2fvalue"
  execute savepoint; assert_failed; assert_log_not_contains 'flink run '
  grep -F "savepoint=$FAKE_STOP_URI" "$CASE/output" >/dev/null
}
test_single_slash_metadata_failure_retains_actual_uri() {
  export FAKE_STOP_URI="file:$DATA/savepoints/savepoint-generated" FAKE_METADATA=bad
  execute savepoint; assert_failed; assert_log_not_contains 'flink run '
  assert_log_contains "test -r $DATA/savepoints/savepoint-generated/_metadata"
  grep -F "savepoint=$FAKE_STOP_URI" "$CASE/output" >/dev/null
}
test_restarting_job_aborts_before_upload() { export FAKE_JOBS=restarting; execute stateless; assert_failed; assert_no_mutation; grep -F RESTARTING "$CASE/output" >/dev/null; }
test_running_and_restarting_jobs_abort_before_upload() { export FAKE_JOBS=mixed; execute stateless; assert_failed; assert_no_mutation; }
test_old_job_still_running_cannot_report_success() {
  export FAKE_OLD_POLL=persistent; execute stateless; assert_failed
  [[ $(grep -c 'flink list --running' "$CALL_LOG") == 31 ]]
  grep -Fx 'Old job status (last observed): RUNNING' "$CASE/output" >/dev/null
}
test_old_job_restarting_cannot_report_success() { export FAKE_OLD_POLL=restarting; execute savepoint; assert_failed; grep -Fx 'Old job status (last observed): RESTARTING' "$CASE/output" >/dev/null; }
test_delayed_old_job_exit_is_observed_before_success() {
  export FAKE_OLD_POLL=delayed; execute stateless; assert_ok
  [[ $(grep -c 'flink list --running' "$CALL_LOG") == 4 ]]
  grep -Fx 'Old job status (last observed): not active' "$CASE/output" >/dev/null
}
test_other_active_same_name_job_cannot_report_success() { export FAKE_OTHER_JOB=yes; execute stateless; assert_failed; grep -Fx 'Old job status (last observed): not active' "$CASE/output" >/dev/null; }
test_new_job_restarting_cannot_report_success() { export FAKE_NEW_STATE=RESTARTING; execute stateless; assert_failed; }
test_existing_identical_version_is_reused_without_upload_or_overwrite() {
  export FAKE_EXISTING=same; execute stateless; assert_ok
  assert_log_contains "sha256sum $DATA/flink-processer-abcdef123456.jar"
  assert_log_not_contains 'scp '; assert_log_not_contains 'mv '; assert_log_not_contains 'ln '
}
test_existing_conflicting_version_aborts_before_stopping_old_job() { export FAKE_EXISTING=different; execute stateless; assert_failed; assert_no_mutation; }
test_existing_version_checksum_read_failure_aborts_before_stopping() { export FAKE_EXISTING=error; execute stateless; assert_failed; assert_no_mutation; }
test_version_install_never_overwrites_existing_artifact() { execute stateless; assert_ok; assert_log_contains "ln -T $DATA/.flink-processer-abcdef123456-"; assert_log_not_contains 'mv '; }
test_dirty_worktree_aborts_before_build_or_remote() { export FAKE_DIRTY=before; execute stateless; assert_failed; assert_log_not_contains 'mvn '; assert_log_not_contains 'ssh '; assert_no_mutation; }
test_worktree_changed_during_build_aborts_before_remote() { export FAKE_DIRTY=after; execute stateless; assert_failed; assert_log_contains 'mvn '; assert_log_not_contains 'ssh '; assert_no_mutation; }
test_revision_changed_during_build_aborts_before_remote() { export FAKE_HEAD_CHANGE=yes; execute stateless; assert_failed; assert_log_contains 'mvn '; assert_log_not_contains 'ssh '; assert_no_mutation; }
test_malformed_new_job_id_never_reaches_poll_command() { export FAKE_SUBMIT_ID='2222222222222222222222222222222;id'; execute stateless; assert_failed; [[ $(grep -c 'flink list --running' "$CALL_LOG") == 1 ]]; }
test_build_failure_never_contacts_remote() { export FAKE_BUILD=bad; execute stateless; assert_failed; assert_log_not_contains 'ssh '; assert_log_not_contains 'scp '; }
test_thin_jar_never_contacts_remote() { export FAKE_JAR=thin; execute stateless; assert_failed; assert_log_not_contains 'ssh '; assert_log_not_contains 'scp '; }
test_scp_failure_keeps_old_job_running() { export FAKE_SCP=bad; execute stateless; assert_failed; assert_log_not_contains 'flink cancel '; assert_log_not_contains 'flink stop '; assert_log_not_contains 'flink run '; }
test_atomic_move_failure_keeps_old_job_running() { export FAKE_MOVE=bad; execute stateless; assert_failed; assert_log_not_contains 'flink cancel '; assert_log_not_contains 'flink stop '; assert_log_not_contains 'flink run '; }
test_stop_failure_never_submits() { export FAKE_STOP=bad; execute savepoint; assert_failed; assert_log_not_contains 'flink run '; assert_log_not_contains 'flink cancel '; }
test_cancel_failure_never_submits() { export FAKE_CANCEL=bad; execute stateless; assert_failed; assert_log_not_contains 'flink run '; }
test_submit_failure_reports_manual_rollback_after_savepoint() {
  export FAKE_SUBMIT=bad; execute savepoint; assert_failed
  [[ $(grep -c 'flink run ' "$CALL_LOG") == 1 ]]; assert_log_not_contains 'flink cancel '
  grep -F "$RESTORE" "$CASE/output" >/dev/null; grep -F 'rollback' "$CASE/output" >/dev/null
  grep -F 'original JAR must be recovered' "$CASE/output" >/dev/null
}
test_remote_preflight_failure_never_mutates() { export FAKE_PREFLIGHT=bad; execute stateless; assert_failed; assert_no_mutation; }
test_exact_job_name_does_not_cancel_unrelated_job() { export FAKE_JOBS=unrelated; execute stateless; assert_ok; assert_log_not_contains 'flink cancel '; }
test_unknown_argument_never_contacts_remote() { run_deploy --wat; assert_failed; assert_log_not_contains 'ssh '; assert_no_mutation; }

# These output contracts let the Skill report actual recovery choices and paths.
test_success_reports_generated_savepoint_and_restore_mode() {
  execute savepoint; assert_ok
  grep -Fx 'Restore mode: savepoint' "$CASE/output" >/dev/null
  grep -Fx "Savepoint: $RESTORE" "$CASE/output" >/dev/null
}
test_success_reports_provided_savepoint_and_restore_mode() {
  export FAKE_JOBS=none
  execute savepoint --savepoint-path "$PROVIDED"; assert_ok
  assert_log_contains "test -r $DATA/savepoints/manual-recovery-20261014/_metadata"
  assert_log_contains "flink run --detached -s $PROVIDED"
  grep -Fx 'Restore mode: savepoint' "$CASE/output" >/dev/null
  grep -Fx "Savepoint: $PROVIDED" "$CASE/output" >/dev/null
}
test_success_reports_stateless_without_savepoint() {
  execute stateless; assert_ok
  grep -Fx 'Restore mode: stateless' "$CASE/output" >/dev/null
  grep -Fx 'Savepoint: not used' "$CASE/output" >/dev/null
}

for test in $(declare -F | awk '{print $3}' | grep '^test_'); do
  setup "$test"
  # Failures must stop the test immediately, even though the parent collects all results.
  set +e
  (set -e; "$test") > "$CASE/assertions" 2>&1
  result=$?
  set -e
  if [[ "$result" == 0 ]]; then
    printf 'PASS %s\n' "$test"; passed=$((passed + 1))
  else
    printf 'FAIL %s\n' "$test"; cat "$CASE/assertions"; failed=$((failed + 1))
  fi
done
printf '%s passed; %s failed\n' "$passed" "$failed"
[[ "$failed" == 0 ]]
