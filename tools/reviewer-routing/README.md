# Reviewer routing

The GitHub workflow reads this directory to choose at most two reviewer
candidates when an open PR receives `need-reviewers`. It adds assignees, posts a
mention and replaces the label with `reviewers-assigned`. Reapplying the request
label selects again; retrying the same run preserves its saved decision.

## Maintain one file

Edit **`reviewer-routing.json`** for every routing change. Python code and this
README do not contain a second reviewer table.

| Field | How to maintain it |
|---|---|
| `modules.<id>.reviewers` | Add, remove or reorder GitHub logins. After eligibility filtering, the first five form an equal-priority sampling pool. An empty list leaves the module without candidates. |
| `modules.<id>.patterns` | Regexes for normalized source paths. Higher `priority` wins; equal priorities follow file order. |
| `modules.<id>.label` | A short description used in the model context. |
| `disabled_reviewers` | Exclude an account from every module, case insensitively, without editing each list. |
| `fallback_reviewer` | The person who handles manual assignment when no suitable module or eligible candidate is found. |
| `test_routes` | Ordered regex-to-module rules for normalized test paths. First match wins. Ordinary unit tests also use source-path normalization. |

Names have no `@` prefix. Lists remain authoritative until edited. New reviewers
can be configured immediately. To disable the fallback account, first choose
another fallback.
The author remains excluded from normal review choices; a fallback is a triage
assignment and may go to the author.

For a new module, add one entry under `modules`, using a unique ID:

```json
{
  "connector.example": {
    "label": "Example connector",
    "priority": 350,
    "patterns": ["^fe/fe-connector/fe-connector-example/"],
    "reviewers": ["ExampleMaintainer", "AnotherMaintainer"]
  }
}
```

Add a `test_routes` entry if regression-suite paths need a specific module.
Keep `other` at the lowest priority and retain the `other` and `test.other`
fallback categories. Adding or changing a module requires no generated file.

After editing, validate and inspect the effective lists:

```bash
python3 tools/reviewer-routing/routing_config.py --check
python3 tools/reviewer-routing/routing_config.py --list
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s .github/scripts -p 'test_reviewer_assignment*.py'
```

Validation rejects duplicate keys/logins, invalid login syntax or regexes,
unknown fields, missing fallback categories and invalid test-route references.
The workflow runs the same validation before selection. Merged changes apply to
new runs through the trusted default-branch checkout.

## Runtime

Python 3.9+ and the standard library are sufficient for local helpers. The
model runner and replay command support Linux and macOS. The
workflow also uses Git, GitHub CLI and the Codex runtime/auth pool already used
by Code Review Runner. It reuses `GITHUB_TOKEN`, `OSS_AK` and `OSS_SK`; selection
has read permissions, while a separate assignment job has `issues: write` and
`pull-requests: write`.

Python collects authoritative PR metadata, changed files, reviews and the
complete diff, computes the affected modules, change statistics and candidate
pools, and places all of them directly in the model's stdin. The supplied
catalog includes semantic alternatives outside the path matches. Codex returns
at most two module IDs and a short classification reason. It does not gather
input, invoke helpers or draw people. Shell, apps, plugins, web search and
subagents are disabled; local user configuration and project instructions are
excluded from this data-only classification. Python excludes the author,
unavailable and globally disabled people before taking each module's first
five eligible names. For one module, it draws two distinct people uniformly,
or one when only one is available. For two modules, it draws uniformly from
pairs covering every nonempty pool, with shared people counted toward both.
Names inside each pool have no preference weights. A new label event permits
a fresh random draw; the same pair can occur again by chance. Saved selections
and same-run assignment checkpoints are reused without another draw.
No PR code is checked out or executed. API-limited diffs are reconstructed from
verified pinned Git snapshots. Invalid output or infrastructure errors fail;
an empty classification is rejected when path-matched modules have eligible
candidates. Modules without an eligible pool cannot be selected. When no
eligible path module is available, an empty valid selection assigns
`fallback_reviewer` with this comment:

```text
No suitable reviewer was found. @<fallback_reviewer>, please help assign reviewers for this PR.
```

Assignments and comments are read back before labels change. Actions Summary
and the seven-day `reviewer-assignment-usage` artifact report Codex time and token
counts. Cached input is included in input tokens; unavailable usage after a
failure is not zero. Raw sessions, credentials and prompts are not uploaded.

The separate seven-day diagnostics artifacts contain input byte counts and
SHA256 fingerprints, implementation hashes, CLI/platform versions, the model's
decision and reason, validation status, selected pools, event/item counts,
filtered command/MCP errors and GitHub API failure metadata. Missing or partial
model output remains visible. Diagnostic collection runs after failures and
does not turn a failed classification into a successful assignment.

## Local replay and checks before merge

Run the same production prepare, model runner, validation and sampling code
locally, without posting comments or changing assignees/labels:

```bash
python3 .github/scripts/replay_reviewer_assignment.py \
  --repository apache/doris --pr 70665 \
  --codex /path/to/pinned-codex/bin/codex \
  --workspace /tmp/reviewer-70665-first
```

Use the same pinned CLI package as the workflow for an environment comparison.
The command uses the CLI's saved authentication (or an explicit `CODEX_HOME`)
but ignores user configuration. GitHub reads use `gh`; the replay wrapper
rejects every non-GET request. The model receives no GitHub or OSS credentials.
The workspace preserves the exact PR input, prompt, schema and fingerprints,
plus diagnostics, validation, usage and the final selected reviewers. Use a new
workspace for each run. To replay the captured PR data with the checked-out
routing policy and implementation, without further GitHub reads:

```bash
python3 .github/scripts/replay_reviewer_assignment.py \
  --snapshot /tmp/reviewer-70665-first \
  --codex /path/to/pinned-codex/bin/codex \
  --workspace /tmp/reviewer-70665-second
```

The `Reviewer Assignment Checks` workflow runs offline fixtures and the shared
executable paths on Ubuntu 24.04 for PRs and pushes affecting this workflow.
It uses a fake model binary, local Git/HTTP fixtures and no repository secrets;
it verifies the input, validation, error handling and mutation boundaries.

For a real hosted model run, manually dispatch the assignment workflow on a
trusted branch. This runs selection only; both the job condition and Python
entry point prohibit the assignment step for manual events:

```bash
gh workflow run reviewer-assignment.yml --repo apache/doris \
  --ref <trusted-branch> -f pr_number=70665
```

GitHub must already know this workflow on the default branch before dispatch
can target another branch. The branch must belong to the target repository;
a fork's branch is not a target-repository ref. Once this entry point is
available, branch revisions can be tested on the real hosted runner before
merge, including the pinned Linux CLI, auth pool and artifact service.

Local replay is not a full emulation of GitHub Actions. An Ubuntu container or
`act` can exercise the scripts, but cannot prove hosted token permissions,
secret injection, concurrency or artifact publication. Keep those checks on
the read-only hosted dispatch path. In particular, `act` does not enforce
`job.permissions` or workflow concurrency; see its
[unsupported functionality](https://nektosact.com/not_supported.html).
