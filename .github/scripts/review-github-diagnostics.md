# GitHub request diagnostics for code review

The runner, status aggregator and result-sync jobs load review_github_api.py
from the immutable workflow commit before their first GitHub API request. The
copy in RUNNER_TEMP survives the subsequent PR checkout and is shared by the
submission helper and head watchdog.

Use python3 "$REVIEW_GH_HELPER" api ... in these workflow steps. Python helpers
use run_gh with captured text output, check=True and their existing timeout;
use decode_json when decoding its result. Request arguments, normal stdout,
write deduplication, permission decisions and retry policy remain unchanged.

On a failure, the helper prints a short diagnostic and saves one JSON record
under REVIEW_GITHUB_DIAGNOSTICS_DIR. Records include the command exit status or
timeout, the last HTTP method/path/status, GitHub request ID, rate-limit and
retry headers, gh version, and bounded response/stdout/stderr excerpts. A
successful gh exit followed by a Python JSON decode error is also recorded.
GraphQL errors returned with HTTP 200 are recorded as failures as well.
A transport failure without a response has status: null; it is not inferred
to be an HTTP error.

The helper captures GH_DEBUG=api in memory because gh can discard a malformed
error body and its HTTP status while parsing it. Outgoing headers, request
bodies and argv are never archived. Response headers use an allowlist.
Credential values and common credential fields are redacted before excerpts
are truncated. The response body is gh's formatted debug excerpt, not exact
wire bytes; gh may itself limit or omit it. Debug format changes may leave
fields unavailable. Hard termination of the helper can prevent recording.

Each failure uses a separate file so later cleanup errors do not overwrite it.
An always() artifact step runs after cleanup, including when an API failure
is caught by an optional step. Artifacts are named
github-api-errors-<job>-<run>-<attempt>; reusable aggregation jobs include the
head SHA to distinguish current-head and previous-head executions. Diagnostic
upload/storage failures do not replace the original request failure.

The existing failed-code-review-* submission artifacts remain available.
These additions do not change the failure notification's content or posting
arguments, or its existing failure classification.

Local validation:

~~~sh
RUN_GH_HTTP_TESTS=1 python3 -m unittest discover -s .github/scripts -p 'test_*.py'
~~~

The opt-in HTTP test uses real gh only against a loopback server, with fake
tokens and a temporary config; it never sends a request to GitHub. Linux-only
process-supervision tests require a Linux host.

## Staged final review submission

When a review has inline findings, submit_review.py creates one PENDING review,
adds each finding with a separate addPullRequestReviewThread mutation, verifies
the full draft comment set, then submits the review once. This avoids asking
the bulk REST create-review endpoint to resolve all targeted file diffs in one
request. The input still uses path/position/body; line and side are derived
from the authoritative local pr.diff before any write. A summary-only review
keeps the direct REST submission path.

The invocation marker, frozen head/base guards, exclusive review-submission.json
intent and complete final readback remain required. A pending review, partial
comment set or local journal never proves delivery. No write is automatically
retried. A lost final-submit response may be recovered by exact readback of the
already submitted review, without another submission event.

failed-code-review-* now also includes review-draft.json: the current stage,
owned review ID, confirmed thread IDs and current finding index/location. This
is diagnostic progress, not a resume instruction. Before publication, an error
triggers best-effort deletion only of the uniquely identified pending draft
with this invocation's exact body, bot author and head. Foreign or modified
drafts and submitted reviews are never deleted.

An ambiguous publish attempt is not deleted or repeated. If it remains PENDING,
or cleanup cannot finish because of an API outage, exhausted budget or process
termination, inspect the journal and GitHub state before manually clearing the
owned draft and starting a new run. Never remove the submission intent to
force a retry. The pipeline must fail rather than report partial delivery.

The motivating #68499 experiment accepted all five original findings in one
draft via sequential GraphQL mutations, while bulk REST creation rejected the
same set with 422. That experiment used a personal account and deleted the
draft; formal publication and the Actions identity still require live rollout
validation. The exact GitHub internal diff limit is not established.
