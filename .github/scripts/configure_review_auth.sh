#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Shared read-only selection and locked-worker auth configuration.
set -euo pipefail
umask 077

install -m 700 -d "$RUNNER_TEMP/codex-home"

auth_prefix='oss://doris-community-ci/codex/auth.json.'
auth_candidates="$(mktemp "$RUNNER_TEMP/codex-auth-candidates.XXXXXX")"
if [ -n "${CODEX_AUTH_CANDIDATE:-}" ]; then
  # Once the job owns an auth concurrency group it must never switch slots.
  if [[ ! "$CODEX_AUTH_CANDIDATE" =~ ^oss://doris-community-ci/codex/auth\.json\.[0-9]+$ ]]; then
    echo "::error::Invalid queued Codex auth object."
    exit 1
  fi
  printf '%s\n' "$CODEX_AUTH_CANDIDATE" > "$auth_candidates"
else
  auth_listing="$(mktemp "$RUNNER_TEMP/codex-auth-objects.XXXXXX")"
  if ! ossutil -i "$OSS_AK" -k "$OSS_SK" -e "$OSS_ENDPOINT" ls "$auth_prefix" -s > "$auth_listing"; then
    auth_failure_reason="Failed to list Codex review auth objects from OSS."
    echo "failure_reason=$auth_failure_reason" >> "$GITHUB_OUTPUT"
    echo "::error::$auth_failure_reason"
    exit 1
  fi

  awk '$0 ~ /^oss:\/\/doris-community-ci\/codex\/auth\.json\.[0-9]+$/ { print }' \
    "$auth_listing" > "$auth_candidates"
fi
if [ ! -s "$auth_candidates" ]; then
  auth_failure_reason="No numbered Codex review auth objects were found in OSS."
  echo "failure_reason=$auth_failure_reason" >> "$GITHUB_OUTPUT"
  echo "::error::$auth_failure_reason"
  exit 1
fi

auth_object=""
earliest_retry_after_epoch=""
now_epoch="$(date +%s)"
while IFS= read -r candidate; do
  context_file="$(mktemp "$RUNNER_TEMP/codex-auth-context.XXXXXX")"
  if ossutil -i "$OSS_AK" -k "$OSS_SK" -e "$OSS_ENDPOINT" \
    cp -f "${candidate}.context" "$context_file" >/dev/null 2>&1; then
    if raw_retry_after_epoch="$(jq -ser '
      select(length == 1)
      | .[0]
      | select(type == "object"
          and .version == 1
          and .state == "usage_limited"
          and (.retry_after_epoch | type == "number" and floor == .))
      | .retry_after_epoch
    ' "$context_file" 2>/dev/null)" && \
       retry_after_epoch="$(date -u -d "@$raw_retry_after_epoch" +%s 2>/dev/null)"; then
      if [ "$retry_after_epoch" -gt "$now_epoch" ]; then
        retry_at="$(date -u -d "@$retry_after_epoch" +%Y-%m-%dT%H:%M:%SZ)"
        echo "Skipping usage-limited Codex auth ${candidate##*/} until $retry_at."
        if [ -z "$earliest_retry_after_epoch" ] || \
           [ "$retry_after_epoch" -lt "$earliest_retry_after_epoch" ]; then
          earliest_retry_after_epoch="$retry_after_epoch"
        fi
        continue
      fi
    else
      echo "::warning::Ignoring unusable Codex auth context for ${candidate##*/}; the account remains eligible."
    fi
  fi

  candidate_auth="$(mktemp "$RUNNER_TEMP/codex-home/candidate-auth.XXXXXX")"
  if ! ossutil -i "$OSS_AK" -k "$OSS_SK" -e "$OSS_ENDPOINT" cp -f "$candidate" "$candidate_auth"; then
    auth_failure_reason="Failed to download Codex auth ${candidate##*/}. Re-run all jobs in the workflow to select another slot."
    echo "failure_reason=$auth_failure_reason" >> "$GITHUB_OUTPUT"
    echo "::error::$auth_failure_reason"
    exit 1
  fi
  jq -e '
    .auth_mode == "chatgpt"
    and (.tokens.access_token | type == "string" and length > 0)
    and (.tokens.refresh_token | type == "string" and length > 0)
  ' "$candidate_auth" >/dev/null
  refresh_token_hash="$(jq -rj '.tokens.refresh_token' "$candidate_auth" | sha256sum | awk '{print $1}')"

  # Quarantine the refresh token, not the slot or the entire auth file:
  # replacing the token restores eligibility; changing metadata does not.
  invalid_object="${candidate}.invalid.${refresh_token_hash}"
  invalid_listing="$(mktemp "$RUNNER_TEMP/codex-auth-invalid.XXXXXX")"
  if ! ossutil -i "$OSS_AK" -k "$OSS_SK" -e "$OSS_ENDPOINT" \
    ls "$invalid_object" -s > "$invalid_listing"; then
    auth_failure_reason="Failed to check invalid-token marker for ${candidate##*/}."
    echo "failure_reason=$auth_failure_reason" >> "$GITHUB_OUTPUT"
    echo "::error::$auth_failure_reason"
    exit 1
  fi
  if grep -Fxq "$invalid_object" "$invalid_listing"; then
    echo "Skipping Codex auth ${candidate##*/}: refresh token is marked invalid."
    rm -f "$candidate_auth"
    continue
  fi

  auth_object="$candidate"
  if [ "${CODEX_AUTH_SELECT_ONLY:-false}" = true ]; then
    # Selection does not run Codex or pass a credential snapshot between jobs.
    # The queued worker must download and validate this slot again under its lock.
    rm -f "$candidate_auth"
    printf 'auth_object=%s\nauth_name=%s\n' "$auth_object" "${auth_object##*/}" >> "$GITHUB_OUTPUT"
    echo "Queued Codex auth object: ${auth_object##*/}"
    exit 0
  fi
  # Use the checked snapshot; downloading it again could select a different token.
  mv "$candidate_auth" "$RUNNER_TEMP/codex-home/auth.json"
  printf '%s\n' "$refresh_token_hash" > "$RUNNER_TEMP/codex-auth-refresh-token.sha256"
  break
done < <(shuf "$auth_candidates")

if [ -z "$auth_object" ]; then
  auth_failure_reason="No eligible Codex review credentials: all are usage-limited or have an invalid refresh token."
  if [ -n "${CODEX_AUTH_CANDIDATE:-}" ]; then
    auth_failure_reason="Queued Codex auth ${CODEX_AUTH_CANDIDATE##*/} is no longer eligible; refusing to use an unlocked auth slot. Re-run all jobs in the workflow to select another slot."
  fi
  if [ -n "$earliest_retry_after_epoch" ]; then
    retry_at="$(date -u -d "@$earliest_retry_after_epoch" +%Y-%m-%dT%H:%M:%SZ)"
    auth_failure_reason="$auth_failure_reason Earliest usage-limit retry is $retry_at."
  fi
  auth_failure_reason="$auth_failure_reason Credentials with invalid refresh tokens must be replaced."
  echo "failure_reason=$auth_failure_reason" >> "$GITHUB_OUTPUT"
  echo "::error::$auth_failure_reason"
  exit 1
fi

printf 'CODEX_HOME=%s\n' "$RUNNER_TEMP/codex-home" >> "$GITHUB_ENV"
printf 'CODEX_AUTH_OSS_OBJECT=%s\n' "$auth_object" >> "$GITHUB_ENV"
echo "Selected Codex auth object: ${auth_object##*/}"
chmod 600 "$RUNNER_TEMP/codex-home/auth.json"
sha256sum "$RUNNER_TEMP/codex-home/auth.json" \
  | awk '{print $1}' \
  > "$RUNNER_TEMP/codex-auth-original.sha256"

cat > "$RUNNER_TEMP/codex-home/config.toml" <<EOF
cli_auth_credentials_store = "file"
approval_policy = "never"

[features]
memories = true

[memories]
use_memories = true
generate_memories = true

[shell_environment_policy]
inherit = "all"

[otel]
environment = "github-actions"
exporter = "none"
metrics_exporter = "none"
trace_exporter = "none"
EOF
chmod 600 "$RUNNER_TEMP/codex-home/config.toml"
