# Query engine benchmarks

These disabled tests exercise production index readers and complete SEARCH expressions for CLucene (V2) and SNII. Reader queries verify complete result IDs and candidate restrictions; SEARCH queries also check finite BM25 values and top-k score cutoffs.

Prepare matching full-text and keyword indexes before running SEARCH:

```bash
BUILD_TYPE_UT=RELEASE GTEST_ALSO_RUN_DISABLED_TESTS=1 \
PHRASE_CANDIDATE_BENCH_INDEX_ROOT="$PWD/bench-corpus" \
PHRASE_CANDIDATE_BENCH_PREPARE=1 \
./run-be-ut.sh --run --filter='PhraseCandidatePushdownBench.DISABLED_RestrictedVersusFullPhrase' -j 8

BUILD_TYPE_UT=RELEASE GTEST_ALSO_RUN_DISABLED_TESTS=1 \
SEARCH_TREE_BENCH_INDEX_ROOT="$PWD/bench-corpus" \
./run-be-ut.sh --run --filter='*SearchTreeBench*' -j 8
```

Use the same document count, field ID and NULL settings when preparing and reading a corpus. A secondary field corpus uses `PHRASE_CANDIDATE_BENCH_FIELD_ID=2`.

| Reader environment variable | Default | Meaning |
| --- | --- | --- |
| `PHRASE_CANDIDATE_BENCH_DOCS` | 200000 | Rows in the short-document corpus. |
| `PHRASE_CANDIDATE_BENCH_LONG_DOCS` | 20000 | Long documents, written separately for each launch. |
| `PHRASE_CANDIDATE_BENCH_ITERATIONS` | 10 | Samples per measurement. |
| `PHRASE_CANDIDATE_BENCH_FORMATS` | Both | Comma-separated `V2` and `SNII`. |
| `PHRASE_CANDIDATE_BENCH_CASES` | All | Comma-separated query labels. |
| `PHRASE_CANDIDATE_BENCH_VARIANTS` | All | Comma-separated variants such as `full`, `scored`, `cached`, `count` or `random/0.500`. |
| `PHRASE_CANDIDATE_BENCH_INDEX_ROOT` | Temporary indexes | Directory for prepared short-document indexes. |
| `PHRASE_CANDIDATE_BENCH_PREPARE` | 0 | Write indexes under the supplied root without executing queries. |
| `PHRASE_CANDIDATE_BENCH_NULL_EVERY` | 0 | Make rows 0, n, 2n, and so on NULL when n is positive. |
| `PHRASE_CANDIDATE_BENCH_FIELD_ID` | 1 | Column ID written into the prepared index. |

A restricted phrase also runs once without candidates for its result oracle when the `full` variant is omitted. Scored, cached and count variants run only for cases that support them.

SEARCH uses `SEARCH_TREE_BENCH_INDEX_ROOT` for the prepared corpus and the corresponding `DOCS`, `ITERATIONS`, `FORMATS`, `CASES` and `NULL_EVERY` variables with the same prefix. Defaults are 200000 rows, 10 samples, both formats, all cases and no NULL rows. Cross-field cases additionally require `SEARCH_TREE_BENCH_SECONDARY_ROOT`; `SEARCH_TREE_BENCH_SECONDARY_NULL_EVERY` defaults to the primary NULL setting.

Samples measure per-query thread CPU time, with result verification outside the timed window. Scored cases use fixed collection statistics; top-k cases validate the score cutoff without requiring one ordering among equal scores.

Paired comparisons must use matching benchmark drivers and runtime support. Unrelated test registrations in a monolithic test executable can affect timing, so record the fixed driver set, source revisions, build options, binary hashes and every sample with the comparison.
