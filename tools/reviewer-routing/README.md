# Apache Doris reviewer routing

Select reviewers for an Apache Doris pull request using a frozen analysis of
4,645 PRs merged into `master` between `2025-11-17T00:00:00Z` (inclusive) and
`2026-11-17T09:26:38Z` (exclusive). The mapping covers 57 modules: 49 have
historically qualified candidates and four have explicit maintainer overrides.
The remaining four modules require manual classification.

The Python commands run offline and produce a review plan. An agent follows
[REVIEWER_RULES.md](REVIEWER_RULES.md) to inspect the diff, check current PR state
and send review requests when the task authorizes invitations.

For automatic routing after a `need-reviewers` label event, see
[the GitHub Action guide](GITHUB_ACTION.md). That mode selects at most two people,
adds them as assignees, posts a mention comment and updates labels.

## Files and reading order

| File | Purpose | Consumer |
|---|---|---|
| `REVIEWER_RULES.md` | Agent instructions and the complete module candidate table | Read this before choosing or inviting reviewers |
| `GITHUB_ACTION.md` | Label-triggered assignee workflow, permissions and credentials | Read this for the `need-reviewers` automation mode |
| `CI_GUIDE.md` | Concise model instructions for automatic assignment | The CI model reads this instead of the operator guides |
| `ci_context.py` | Compact affected-module candidate evidence and semantic module queries | CI prepares a summary before invoking the model |
| `select_reviewers.py` | Deterministic reviewer planning CLI | Execute it; it makes no network requests |
| `reviewer_candidates.py` | Shared maintainer-override and historical eligibility policy | Used by the planner and GitHub Action validator |
| `module-reviewers.json` | Ranked candidates, historical counts, activity and example PRs | The selector reads it; inspect specific modules for more evidence |
| `taxonomy/classify_paths.py` | Path normalization and module classification | Imported by the selector; can also run directly |
| `taxonomy/module-spec.json` | Module definitions, priorities and path expressions | Read by the classifier |
| `taxonomy/historical-path-aliases.json` | Historical BE path renames | Read by the classifier |

All runtime dependencies are inside this directory. The tool does not require
the research files, an installed Python package or a Doris build.

In CI, Python writes `candidate-summary.json` before the model starts. The model
reads `CI_GUIDE.md`, that summary and the complete PR context. It can query other
modules through `ci_context.py` when the diff justifies semantic reclassification.
The offline planner below retains its existing total-reviewer budget.

## Requirements

- Python 3.9 or later, with its standard library.
- GitHub CLI (`gh`) authenticated for `apache/doris` when reading a live PR or
  sending review requests. The local planner does not need credentials.
- `jq` only for the optional live-PR preparation recipe below.

Run the examples from the repository root. Absolute script paths also work
from another working directory because data files resolve relative to the tool.
Use normal Python execution; `python -O` disables input and freshness assertions.

## Quick start

Save the following as `/tmp/reviewer-pr.json`, replacing the PR number, author
and file list with the values for the PR being reviewed:

```json
{
  "number": 12345,
  "author": "example-author",
  "changed_files": 1,
  "files": [
    {
      "path": "be/src/storage/index/inverted/query/query.cpp",
      "additions": 8,
      "deletions": 2
    }
  ],
  "existing_reviewers": [],
  "unavailable_reviewers": []
}
```

Generate a plan:

```bash
python3 tools/reviewer-routing/select_reviewers.py /tmp/reviewer-pr.json
```

At the snapshot date, this example routes to `search` and recommends `eldenmoon`,
with historical review counts and example PR numbers. Candidates can expire as
time passes. To set a smaller total reviewer target, use `--limit 1` or `--limit 2`:

```bash
python3 tools/reviewer-routing/select_reviewers.py /tmp/reviewer-pr.json --limit 2
```

The default target is at most three people, including existing individual
reviewers. A single qualified person can cover multiple modules.

## Input and output

| Input field | Meaning |
|---|---|
| `number` | Positive PR number in `apache/doris` |
| `author` | Author's GitHub login |
| `files` | Complete changed-file list: paths or objects with `path`, `additions` and `deletions` |
| `changed_files` | Expected file count; always supply the live PR's count to detect truncated input |
| `existing_reviewers` | Individual logins already requested or with a submitted, non-dismissed review; default empty |
| `unavailable_reviewers` | Individual logins to exclude for this task; default empty |

Login comparisons are case insensitive. For renamed files, supply the current
path once and inspect both old and new paths while reading the diff.

Important output fields:

- `reviewers`: new candidates, covered modules and historical evidence.
- `covered_by_existing`: modules already covered by an experienced reviewer.
- `uncovered_modules`: modules still needing attention.
- `needs_manual_attention`: whether any module remains uncovered.
- `refresh_recommended`: whether the mapping is older than 30 days.
- `dry_run`: always `true`; the command has not sent an invitation.

An empty `reviewers` list can mean sufficient existing coverage, an exhausted
reviewer budget or insufficient evidence. Check `uncovered_modules` and
`covered_by_existing` before interpreting the result. A successful exit means
the plan was produced, not that every module has a reviewer.

## Prepare input from a live PR

First read the PR and its diff, check that it is open and ready for review, and
confirm that its base is `master`. Other branches were not included in training.

This Bash recipe reads metadata, every file page and every formal-review page.
Replace `12345` with the requested PR number:

```bash
set -e
pr_number=12345
work_dir=$(mktemp -d)

gh api "repos/apache/doris/pulls/$pr_number" > "$work_dir/metadata.json"
jq -e '.state == "open" and .draft == false and .base.ref == "master"' \
  "$work_dir/metadata.json" > /dev/null

gh api --paginate --slurp \
  "repos/apache/doris/pulls/$pr_number/files?per_page=100" \
  > "$work_dir/files.json"
gh api --paginate --slurp \
  "repos/apache/doris/pulls/$pr_number/reviews?per_page=100" \
  > "$work_dir/reviews.json"
gh pr diff "$pr_number" --repo apache/doris > "$work_dir/pr.diff"

jq -n \
  --slurpfile metadata "$work_dir/metadata.json" \
  --slurpfile files "$work_dir/files.json" \
  --slurpfile reviews "$work_dir/reviews.json" '
  {
    number: $metadata[0].number,
    author: $metadata[0].user.login,
    changed_files: $metadata[0].changed_files,
    files: ($files[0] | add | map({
      path: .filename, additions: .additions, deletions: .deletions
    })),
    existing_reviewers: (
      (($metadata[0].requested_reviewers // [])
        | map(select(.type != "Bot") | .login))
      + ($reviews[0] | add | map(select(
          .user != null and .user.type != "Bot"
          and (.state == "APPROVED" or .state == "COMMENTED"
               or .state == "CHANGES_REQUESTED")
        ) | .user.login))
      | unique
    ),
    unavailable_reviewers: []
  }' > "$work_dir/input.json"

python3 tools/reviewer-routing/select_reviewers.py "$work_dir/input.json" \
  > "$work_dir/plan.json"
cat "$work_dir/plan.json"
```

Read `pr.diff` before acting on `plan.json`. Team requests require separate
inspection: check whether GitHub has expanded them to individual reviewers.
Both REST and GraphQL file connections can cap at 3,000 files. The count check
rejects truncated input; reconstruct a verified complete diff before continuing.

## Invite the selected reviewers

Follow `REVIEWER_RULES.md`. Re-read the author and current requested reviewers
immediately before sending, remove duplicates from the proposed list and verify
that the task authorizes invitations. An explicit request to invite reviewers is
sufficient authorization; a recommendation-only request calls for a plan.

Create a request body from the inspected plan:

```bash
jq '{reviewers: [.reviewers[].login]}' "$work_dir/plan.json" \
  > "$work_dir/request.json"
```

Only send a nonempty, reconciled list. The following command sends notifications:

```bash
gh api --method POST \
  "repos/apache/doris/pulls/$pr_number/requested_reviewers" \
  --input "$work_dir/request.json"
gh api "repos/apache/doris/pulls/$pr_number/requested_reviewers"
```

Report the reviewers actually requested and any uncovered modules or API errors.
The invitation endpoint requests reviews; assigning a PR owner is a separate action.

## Inspect classification or historical evidence

Classify paths directly:

```bash
python3 tools/reviewer-routing/taxonomy/classify_paths.py \
  be/src/storage/index/inverted/query/query.cpp \
  regression-test/suites/nereids_p0/test_search.groovy
```

The production search path determines routing; the companion regression path
does not add an optimizer reviewer. Test-only PRs use test classification hints.

Inspect candidates for a specific module without loading the entire mapping
into an agent's context:

```bash
jq '.modules[] | select(.id == "search") | {id, label, reviewer_override, recommended, candidates}' \
  tools/reviewer-routing/module-reviewers.json
```

## Maintainer overrides

A module's `reviewer_override` list is its authoritative candidate pool, in
preference order. It replaces historical candidates for that module and bypasses
historical sample-size and individual inactivity checks. Author exclusion,
existing-reviewer deduplication, explicit unavailability, live account checks
and the global 90-day mapping-expiry guard still apply.

| Module | Ordered reviewers |
|---|---|
| Trino connector | `morningman`, `Gabriel39`, `924060929` |
| Fluss | `morningman`, `Gabriel39` |
| MongoDB | `Gabriel39` |
| Memory management and MemTracker | `yiguolei`, `mrhhsg` |

Override candidates have `confidence: maintainer` and selection evidence with
`selection_source: maintainer_override`. The original historical counts, scores,
activity dates and `auto_eligible` flags remain unchanged in the candidate JSON.
An old review date or a false historical `auto_eligible` flag does not disqualify
a maintainer-configured person. If all configured people are excluded, report
the module as uncovered instead of falling back to an unlisted historical name.

## Freshness, maintenance and limits

Refresh the mapping after 30 days when practical. The selector stops after 90
days and excludes history-derived candidates whose last review is more than
180 days old at execution time. Maintainer overrides bypass that individual
inactivity filter. Rebuild from a new complete merged-PR window; changing
`as_of` alone does not refresh the evidence.

When refreshing, preserve the documented scoring and eligibility rules, update
the candidate JSON and the table in `REVIEWER_RULES.md` together, preserve explicit
maintainer override lists and their order, and review path
rules after directory moves. `research_provenance` records hashes from the source
research; those source files are not runtime dependencies.

Historical participation is associated with PR modules, not verified for every
reviewed line. The agent must inspect shared interfaces and ambiguous paths.
Current permissions, leave and pending review workload are not inferred from
the historical score. Unknown modules and insufficient evidence remain explicit
manual decisions.

An agent task can start with:

```text
Read tools/reviewer-routing/README.md and
tools/reviewer-routing/REVIEWER_RULES.md. Inspect the requested PR and its complete
diff, prepare the selector input, and run the planner.
Explain the module choices and reviewer evidence. If the task asks for reviewer
invitations, reconcile the current reviewer state, send the review requests and
verify the result. Report unresolved modules or failed requests.
```
