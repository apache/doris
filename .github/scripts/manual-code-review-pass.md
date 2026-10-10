# Manual code-review acceptance

A maintainer with repository write/admin permission can accept the current head
of an open PR by posting a new PR comment containing exactly:

```text
/force-pass code-review <full 40-character PR head SHA>
```

Copy the SHA from the PR's current head. A queued command for an older head is
rejected. This command accepts the code-review findings for this PR and commit;
it does not suppress review delivery or change other CI checks.

The Code Review workflow validates the comment creator's current repository
permission and writes `code-review/source/manual/pr-<number>/head-<sha>` on that
commit. Its target URL identifies the authorizing comment. Editing or deleting
the comment does not revoke an already recorded acceptance. Ordinary approvals,
dismissed reviews, and unscoped `code-review: success` / `force pass` statuses
are not manual acceptance records. To preserve an old force pass, an authorized
maintainer must issue the explicit command on the intended PR.

The manual source survives automated reruns and base movement, but cannot
approve a new head or a different PR. Source status history uses the latest
entry for each exact context, as with the other review sources.

## Shared commits and final status

GitHub commit statuses are shared across PRs using the same commit. The final
`code-review` status remains conservative: **every open PR using that SHA must
have its own acceptance or passing review source**. Accepting PR A cannot turn
an unreviewed or failed PR B green, even when they share a head. Conversely, B
can keep the shared check pending/failed while A's manual acceptance remains
recorded. Independent green/red merge gates for identical heads require a
separate redesign of the required-check integration; this change does not claim
that isolation or alter repository branch protection.

Only the serialized aggregate job publishes the final `code-review` context.
Starting a review or accepting a local/skip/manual source no longer blindly
resets that context to pending. The last aggregate verdict remains visible until
a new aggregate verdict is available; individual source statuses still show
pending while automated review runs. New heads must obtain their own sources.
When no open PR uses a head, aggregation leaves its final status unchanged.

## Validation and rollout

Run the focused unit tests from `.github/scripts`:

```sh
python3 -m unittest test_resolve_code_review_status test_validate_manual_review_pass
```

The trusted validator/resolver are checked out from the default branch. Merge
the workflow and scripts together before using the command. Older workflow runs
already in progress can still write the previous final-status behavior; allow
them to finish, then rerun aggregation if needed. No existing force-pass statuses
are automatically migrated because they do not identify the intended PR.
