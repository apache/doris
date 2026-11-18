# Label-triggered reviewer assignment

The workflow is [reviewer-assignment.yml](../../.github/workflows/reviewer-assignment.yml).
It uses this tool's guides and historical evidence to select reviewer candidates.

## Behavior

1. A `pull_request_target` event with action `labeled` and label `need-reviewers`
   starts a run for an open PR, including a draft explicitly labeled for routing.
2. The read-only selection job reads trusted default-branch tools and obtains
   complete live PR metadata, file pages, submitted review pages and the diff.
   It never checks out or executes the PR's code.
3. Python precomputes `candidate-summary.json` with the affected modules' qualified
   candidates, preference weights, historical counts, dates and example PRs. It
   uses the same policy and planner as the offline tool, with an independent
   two-assignee budget. Codex reads `CI_GUIDE.md`, the summary and the complete
   PR context instead of the long operator guides. It outputs only
   a JSON object such as `{"reviewers":["eldenmoon","csun5285"]}`. The schema
   permits at most two IDs from qualified historical candidates or explicit
   maintainer overrides. The helper rejects
   the author, duplicate IDs, unqualified people and an empty result.
4. A separate write job verifies the selection again, checks that both accounts
   are human and assignable, and preserves every existing assignee.
5. The helper creates a checkpoint comment, adds the selected assignees, reads
   the actual assignments back and completes the same comment with:

   ```text
   @eldenmoon, @csun5285, please review this PR.
   ```

6. After the comment and assignments succeed, it adds `reviewers-assigned` and
   removes `need-reviewers`, preserving unrelated labels. It creates the
   `reviewers-assigned` repository label if necessary.

The existing `reviewers-assigned` label never suppresses a new request. Removing
and reapplying `need-reviewers` creates a new event and run ID, so Codex selects
again and a new mention comment is posted. A queued label event is processed
even if an earlier run has already removed the visible request label.

Rerunning the same workflow run reuses its bot-authored checkpoint and selected
pair. It repairs incomplete assignments, comments or label updates without
selecting additional people or posting another mention. The temporary checkpoint
comment says assignment is in progress until all selected assignees are confirmed.

An empty or invalid AI result fails without marking assignment complete. PR
head/base changes, API file truncation, expired mapping, unavailable assignees
and API failures also stop completion. Inspect the run and retry it, or reapply
the label when a fresh selection is needed.

The path suggestion is advisory. The summary includes the first five eligible
historical candidates per affected module or the complete override list, and
two example PRs per person. Codex still reads the complete diff and can query
other modules or more candidates without loading the full mapping:

```bash
python3 tools/reviewer-routing/ci_context.py --list-modules
python3 tools/reviewer-routing/ci_context.py pr.json --modules runtime.memory
python3 tools/reviewer-routing/ci_context.py pr.json --modules search --all-candidates
```

## Usage and timing

Every selection job attempts to publish an Actions Summary table and a separate
`reviewer-assignment-usage` artifact containing `usage.json`, retained for seven
days. It records the model, reasoning effort, outcome, Codex wall time, exit code,
completed turns, input tokens, cached input tokens, uncached input tokens and
output tokens. Reasoning output tokens appear when the CLI reports them.
The existing validated plan artifact remains limited to context and selected IDs.

Counts come from `turn.completed.usage` in the local Codex JSONL events and sum
across completed turns. Cached input is already part of input tokens; reasoning
output is not added to output tokens. These counters do not calculate a price
or a ChatGPT plan's allowance. Codex wall time excludes installation, queuing,
auth sync and GitHub writes; Actions shows those as separate step durations.

Reporting and artifact upload run after model success, failure or timeout, and
also when the model is skipped. A checkpoint reuse or a model that never started
records known zero model usage. Missing completion events produce unavailable
counts rather than zero; partial counters are marked `usage_complete: false`
because interrupted requests can consume tokens without a completion event.
Only numeric metadata and fixed status fields are retained. Raw events, PR text,
prompts, command output, sessions and credentials are not uploaded. A telemetry
failure is visible in its step but does not block an otherwise valid assignment.

## GitHub token and permissions

The workflow uses the same automatic `GITHUB_TOKEN` as `code-review-runner.yml`,
passed to `gh` through `GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}`. No additional token
or personal access token is required. The jobs request:

| Job | Permission | Purpose |
|---|---|---|
| `select` | `contents: read` | Check out the trusted tool and helper |
| `select` | `pull-requests: read` | Read PR metadata, files, reviews, diffs and comments |
| `assign` | `contents: read` | Check out the trusted assignment helper and evidence |
| `assign` | `pull-requests: read` | Recheck the live PR before applying the plan |
| `assign` | `issues: write` | Add assignees, create/update the mention comment, create/add/remove labels |

GitHub exposes PR assignees, conversation comments and labels through its Issues
API. `Issues: write` is sufficient for these writes; `pull-requests: write` and
`contents: write` are not requested. The token's actor must be permitted to add
assignees; the helper checks the resulting state because GitHub can silently
ignore assignment attempts. See the official APIs for
[assignees](https://docs.github.com/en/rest/issues/assignees),
[comments](https://docs.github.com/en/rest/issues/comments), and
[labels](https://docs.github.com/en/rest/issues/labels).

The reference runner already declares `statuses: write`, `pull-requests: write`,
`contents: read` and `issues: write`. This workflow requests a subset: it does not
publish formal reviews or commit statuses. The checkbox allowing Actions to
create or approve pull requests is not needed. This implementation trusts
`github-actions[bot]` checkpoints, matching the existing token's actor.

Apply `need-reviewers` through the GitHub UI, a maintainer's authenticated CLI,
or an approved GitHub App/PAT. A label added by another workflow's own
`GITHUB_TOKEN` does not trigger this workflow. If a label-producing workflow
must trigger assignment, that producer needs an approved GitHub App installation
token or PAT with `Issues: write`, and the user/app needs repository access.
See [GitHub's workflow-trigger rules](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

## Codex credentials and OSS access

The workflow installs exactly the pinned Codex package used by
`code-review-runner.yml`, version `0.166.0` from its OSS release object. It verifies
the archive, package manifest and binary SHA256 values before execution. It uses
`gpt-6.1-sol` with medium reasoning and a read-only sandbox, with an eight-minute
Codex timeout. Structured output uses Codex's
[`--output-schema` option](https://learn.chatgpt.com/docs/non-interactive-mode).

Reuse the existing repository or organization Secrets:

| Secret | Required access |
|---|---|
| `OSS_AK` | OSS access-key ID, using the existing CI secret name |
| `OSS_SK` | Matching OSS secret access key |

Authentication follows the reference runner's existing numbered credential pool:

```text
oss://doris-community-ci/codex/auth.json.<number>
```

No new login, credential object, repository variable or `OPENAI_API_KEY` is
required. The endpoint remains `oss-cn-hongkong.aliyuncs.com`. With the same OSS
access used by Code Review Runner, the workflow:

1. Lists the pool and selects only object names ending in a numeric slot.
2. Randomizes the candidates, skipping future `usage_limited` cooldowns from
   `<object>.context` and exact `<object>.invalid.<refresh-token-sha256>` markers.
3. Validates the chosen ChatGPT access/refresh tokens and uses that checked
   snapshot, without downloading it a second time.
4. Records its original hash and selected `CODEX_AUTH_OSS_OBJECT` locally.
5. Writes refreshed credentials back to that same selected pool object, using
   the reference runner's retry/timeouts and stale-snapshot check.

The existing OSS policy already provides the required pool listing, credential
and marker reads, package download and credential writeback. Memory and session
archives are not needed for reviewer selection.

Workflow-level concurrency serializes reviewer-assignment runs. Code Review
Runner continues to use the same pool independently. As in the reference runner,
auth sync rereads the selected remote object and skips a stale upload if it
observes that another run has replaced the snapshot. It does not claim an atomic
cross-workflow credential lock. Auth sync runs after AI failures as well.

GitHub's `queue: max` retains up to 100 pending runs instead of replacing the
single pending run; more than 100 pending runs are subject to GitHub's queue
limit. See
[the concurrency documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency).

Ubuntu setup installs Bubblewrap and loads its AppArmor user-namespace profile
to support the read-only Codex sandbox. It does not fall back to an unrestricted
model execution. See [OpenAI's sandbox setup](https://learn.chatgpt.com/docs/sandboxing).

## Setup and first run

The existing runner's tokens and login pool can be reused directly. Deployment
only needs the workflow and its trusted source files on the default branch:

1. Reuse the existing `GITHUB_TOKEN`, `OSS_AK`, `OSS_SK` and numbered Codex auth
   objects configured for Code Review Runner.
2. The repository has a `need-reviewers` label. The completion label can be
   created in advance or by the workflow.
3. The workflow, helper scripts and `tools/reviewer-routing/` are present on the
   default branch. A PR containing this new workflow does not activate it before
   it is merged into the trusted default branch.

The offline planner and the workflow validator share `reviewer_candidates.py`.
Both honor ordered `reviewer_override` lists before historical thresholds or
individual inactivity checks. The frozen historical records remain available
for explanation, and the global mapping-expiry guard still applies.

Test with a maintainer-labeled PR, inspect the selected IDs, confirmed assignees,
mention comment and labels, then remove/reapply `need-reviewers` to test a new
selection. A successful selection artifact contains only `context.json` and
`selection.json`, retained for seven days. Auth files, Codex logs, prompts and
session files are not uploaded as artifacts.

Local tests require no credentials and send no requests:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s .github/scripts -p test_reviewer_assignment.py -v
```

The snapshot expiry and reviewer freshness rules described in `README.md` still
apply. Refresh the historical mapping when necessary rather than altering its
timestamp to bypass expiry.
