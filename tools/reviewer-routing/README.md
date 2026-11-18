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
| `modules.<id>.reviewers` | Add, remove or reorder GitHub logins. Earlier names are preferred. An empty list leaves the module without candidates. |
| `modules.<id>.patterns` | Regexes for normalized source paths. Higher `priority` wins; equal priorities follow file order. |
| `modules.<id>.label` | A short description used in the model context. |
| `disabled_reviewers` | Exclude an account from every module, case insensitively, without editing each list. |
| `fallback_reviewer` | The person who handles manual assignment when Codex returns no suitable candidates. |
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
  -s .github/scripts -p test_reviewer_assignment.py
```

Validation rejects duplicate keys/logins, invalid login syntax or regexes,
unknown fields, missing fallback categories and invalid test-route references.
The workflow runs the same validation before selection. Merged changes apply to
new runs through the trusted default-branch checkout.

## Runtime

Python 3.9+ and the standard library are sufficient for local helpers. The
workflow also uses Git, GitHub CLI and the Codex runtime/auth pool already used
by Code Review Runner. It reuses `GITHUB_TOKEN`, `OSS_AK` and `OSS_SK`; selection
has read permissions, while a separate assignment job has `issues: write`.

`ci_context.py` prepares the affected modules and a path-based suggestion.
Codex reads `CI_GUIDE.md`, that summary, PR metadata and the complete diff. It
can query other configured modules when the diff shows a different specialty.
No PR code is checked out or executed. API-limited diffs are reconstructed from
verified pinned Git snapshots. Invalid output or infrastructure errors fail;
an empty valid selection assigns `fallback_reviewer` with this comment:

```text
No suitable reviewer was found. @<fallback_reviewer>, please help assign reviewers for this PR.
```

Assignments and comments are read back before labels change. Actions Summary
and the seven-day `reviewer-assignment-usage` artifact report Codex time and token
counts. Cached input is included in input tokens; unavailable usage after a
failure is not zero. Raw sessions, credentials and prompts are not uploaded.
