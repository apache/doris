# GitHub request diagnostics for code review

The runner, status aggregator and result-sync jobs load review_github_api.py
from the immutable workflow commit before their first GitHub API request. The
copy in RUNNER_TEMP survives the subsequent PR checkout and is shared by the
submission helper and head watchdog.

Use python3 "$REVIEW_GH_HELPER" api ... in these workflow steps. Python helpers
use run_gh with captured text output, check=True and their existing timeout;
use decode_json when decoding its result. Request arguments, normal stdout,
write deduplication and permission decisions remain unchanged.

Body-free REST GETs using the pipeline's known gh api arguments retry HTTP
502/503/504 at most twice. Each attempt is limited to 30 seconds; all attempts
and waits share 90 seconds, or the caller's shorter timeout. Retry-After
(seconds or an HTTP date) is respected; a malformed header or a wait exceeding
the remaining budget stops retries. Each failed attempt keeps its diagnostic.
Only a successful attempt's complete stdout is returned; failed pagination
output is discarded before restarting the full read. Exhaustion still fails.

Writes, GraphQL, fields/input, unknown argument forms, transport/timeouts, JSON
parse failures and other HTTP statuses retain single-attempt behavior. The
failure notification can use the authorized event/input PR number when input
resolution failed before producing outputs; this does not authorize status
writes for an unresolved head/base.

On a failure, the helper prints a short diagnostic and saves one JSON record
under REVIEW_GITHUB_DIAGNOSTICS_DIR. Records include the command exit status or
timeout, the last HTTP method/path/status, GitHub request ID, rate-limit and
retry headers, gh version, and bounded response/stdout/stderr excerpts. A
successful gh exit followed by a Python JSON decode error is also recorded.
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
