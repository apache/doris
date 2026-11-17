# Label-triggered reviewer assignment

The workflow is [reviewer-assignment.yml](../../.github/workflows/reviewer-assignment.yml).
It uses this tool's guides and historical evidence to select reviewer candidates.

## Behavior

1. A `pull_request_target` event with action `labeled` and label `need-reviewers`
   starts a run for an open PR, including a draft explicitly labeled for routing.
2. The read-only selection job reads trusted default-branch tools and obtains
   complete live PR metadata, file pages, submitted review pages and the diff.
   It never checks out or executes the PR's code.
3. Codex reads `README.md`, `REVIEWER_RULES.md` and the PR context. It outputs only
   a JSON object such as `{"reviewers":["eldenmoon","csun5285"]}`. The schema
   permits at most two IDs from active qualified candidates. The helper rejects
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

## GitHub token and permissions

The workflow uses the automatic `GITHUB_TOKEN`, exposed as `${{ github.token }}`.
No personal access token is required for the implemented path. The jobs request:

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

The repository/organization must allow this workflow to obtain `issues: write`.
The checkbox allowing Actions to create or approve pull requests is not needed.
No status, check-run, branch, pull-request creation or approval permission is used.
If organization policy forbids these token permissions, an administrator must
approve a suitable approach before deployment. This implementation trusts
`github-actions[bot]` checkpoints; changing to another token actor also requires
updating that identity check.

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

Required repository or organization Secrets:

| Secret | Required access |
|---|---|
| `OSS_AK` | OSS access-key ID, using the existing CI secret name |
| `OSS_SK` | Matching OSS secret access key |

Their OSS policy needs `GetObject` on the pinned release archive, plus
`GetObject` and `PutObject` on the dedicated reviewer-assignment auth object.
This workflow does not list credential pools or write memory/session archives.

Prepare a valid Codex `auth.json` from a **separate login session with its own
refresh token**. Do not share a refresh token with Code Review Runner or another
active client. Upload it to the default dedicated object:

```text
oss://doris-community-ci/codex/reviewer-assignment/auth.json
```

Optionally override the object using repository variable
`REVIEWER_ASSIGNMENT_CODEX_AUTH_OBJECT`. The endpoint is
`oss-cn-hongkong.aliyuncs.com`. The auth file must use `auth_mode: chatgpt` and
contain nonempty access and refresh tokens, matching the existing runner's
authentication format. No `OPENAI_API_KEY` is used in this implementation.

Workflow-level concurrency serializes access to this dedicated session. Refreshed
auth is synchronized even after an AI failure, and upload is refused if the remote
object changed since download. GitHub's `queue: max` retains up to 100 pending
runs instead of replacing the single pending run; more than 100 pending runs are
subject to GitHub's queue limit. See
[the concurrency documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency).

Ubuntu setup installs Bubblewrap and loads its AppArmor user-namespace profile
to support the read-only Codex sandbox. It does not fall back to an unrestricted
model execution. See [OpenAI's sandbox setup](https://learn.chatgpt.com/docs/sandboxing).

## Setup and first run

Before enabling the workflow, confirm:

1. Actions may run on the repository with `contents: read`,
   `pull-requests: read` and `issues: write` for the indicated jobs.
2. `OSS_AK` and `OSS_SK` are available and have the object permissions above.
3. The dedicated Codex auth object exists and its session can use the selected
   model. Set the optional auth-object variable if a different path is used.
4. The repository has a `need-reviewers` label. The completion label can be
   created in advance or by the workflow.
5. The workflow, helper scripts and `tools/reviewer-routing/` are present on the
   default branch. A PR containing this new workflow does not activate it before
   it is merged into the trusted default branch.

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
