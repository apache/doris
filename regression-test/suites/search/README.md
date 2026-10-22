# SEARCH NULL semantics tests

SEARCH evaluates a two-valued matching set:

- An ordinary term does not match NULL or a field proven absent from storage.
- Internal NOT complements that set, so NULL rows can match a negated term.
- SQL NOT SEARCH complements the same non-nullable result.
- Ordinary SQL comparisons and MATCH keep three-valued logic. NOT MATCH therefore excludes UNKNOWN rows and can differ from NOT SEARCH.

Positive term combinations also compare SEARCH with MATCH. Negated queries keep separate expectations for the two semantic layers.

The `tdd_search_2vl` suites additionally cover both parser modes, cold/warm cache, complete scored results, missing VARIANT paths, and nullable ARRAY payloads.

The backend SQL suites use clauses already emitted by the master FE. Backend field-existence
semantics are checked directly with `TSearchClause` unit tests, including empty arrays, residual
NULL postings, numeric fields, absent paths, scoring, cache reads, and multiple CLucene readers.
The FE field-scope regression is maintained separately with its minimal backend binding fix.

Run this directory with the repository wrapper, passing the worktree configuration before `--run`:

```bash
./run-regression-test.sh --conf tmp/regression-conf.auto.groovy --run -d search -parallel 1
```
