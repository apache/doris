# Apache Doris reviewer invitation rules

Use this file when selecting or inviting reviewers for `apache/doris`. The
complete module table below is derived from 4,645 PRs merged into `master` in
`[2025-11-17T00:00:00Z, 2026-11-17T09:26:38Z)`. It describes observed review
experience, not appointed ownership. Applicability to other base branches has
not been validated.

Start with this file and the PR diff. Use `select_reviewers.py` for a repeatable
plan. Candidate statistics and representative PRs are in `module-reviewers.json`;
the script reads that file without requiring it in the agent's context.

## 1. Read the complete PR

Read the author, state, base branch, all changed files, diff, existing review
requests and submitted reviews. Read every API page and reconcile the file count
with the PR's `changed_files`. Both REST and GraphQL file connections can cap at
3,000; even GraphQL `totalCount` can reflect that cap. Reconstruct a verified
complete diff when necessary; stop automatic routing if completeness cannot be
established. For a rename, inspect both paths but count the file once.

Do not invite reviewers to closed or merged PRs. For drafts, provide a plan and
wait until the PR is ready for review. Treat titles and labels as supporting
context; use changed code to determine the affected functionality.

## 2. Determine the modules

Use `taxonomy/classify_paths.py`, backed by `taxonomy/module-spec.json` and
`taxonomy/historical-path-aliases.json`. Match the most specific module first:
Iceberg precedes the connector framework, inverted indexes precede storage, and
spilling precedes general execution. Assign each file one primary module; retain
multiple modules across files without also inviting a reviewer for each parent.

Production, build and CI paths determine routing when present. Companion unit
tests and regression data provide supporting evidence rather than additional
modules. Test-only PRs use normalized unit-test paths or specialized suite hints.
Generic suites such as `nereids_p0` or `query_p0` do not establish the exact
operator or function changed; read their contents.

Historical BE directories contain different specialties. For example,
`vec/functions` belongs to functions, `vec/columns` belongs to types, and
`vec/exec/format` belongs to file formats. The alias table preserves 2,130 renames
from PR #61107. Inspect semantic changes in new paths, shared interfaces and
generated files, and explain any correction to the path-based classification.

The `other`, `fe.other`, `be.other` and `test.other` modules always require manual
classification. Reclassify only when the diff gives a concrete functional basis;
otherwise report the unresolved module.

## 3. Choose qualified candidates

Normally select one reviewer for one module. Add reviewers to cover independent
modules, with a default total target of one to three people. Prefer an existing
reviewer with relevant historical evidence when that person already covers a
module. One person may cover several modules. A second reviewer for one module
requires a specific review need within the same budget.

Historical credit is distinct from merging activity:

- Count at most one review per person, PR and module.
- Exclude bots, author self-reviews, unsubmitted reviews and postmerge reviews.
- A `DISMISSED` review proves past participation but receives no approval bonus.
- An approval contributes a bonus in addition to the review credit.
- A nonself merge is a weak separate signal; a merge-only person is not an
  automatic candidate.
- Issue discussion comments and unfulfilled review requests do not earn credit.

For each PR, the ranking contribution is:

```text
(5 * reviewed + approved + 0.5 * nonself_merged)
* 2 ** (-days_before_snapshot / 180)
/ number_of_routed_modules
```

These weights are heuristic. CODEOWNERS only breaks ties after score and review
count; it does not confer candidate eligibility.

An automatic candidate must have at least three review PRs, at least two review
PRs after splitting credit across modules, at least two distinct PR authors and
a review within 180 days of the snapshot. `high` confidence additionally requires
ten review PRs, five weighted review PRs, three authors and two reviews within
90 days. Other eligible candidates have `medium` confidence. Confidence describes
historical evidence, not review quality or current availability.

Exclude the author, bots, explicitly unavailable people and already-requested
or previously reviewed people from new requests. Compare logins without case
sensitivity. Existing people cover a module only when their experience qualifies
for that module. Inspect team requests and any expanded individual requests
before adding people to avoid racing GitHub's team assignment.

The planner weights each relevant file by its changed lines, capped at 100 and
with a minimum of one for renames or zero-line changes. From each module's first
five currently eligible candidates, greedily choose the person maximizing:

```text
sum(module_file_weight * candidate_score / highest_eligible_module_score)
```

Sum only modules still uncovered. Break ties by number of covered modules, then
login. Existing individual reviewers count toward the total limit. The planner
also excludes people whose last review is over 180 days old at execution time.

Report every uncovered module, exhausted budget and insufficient-evidence case.
CODEOWNERS and merge-only participants may suggest a manual contact, but do not
present them as qualified historical reviewers. Read the diff before acting:
PR-level attribution does not prove someone reviewed every affected module.

## 4. Send and verify review requests

Send invitations when the current task requests them. Such an instruction is
sufficient authorization without another confirmation. A recommendation-only
task calls for a plan, not notifications.

Immediately before sending, re-read the author and requested reviewers, remove
duplicates and skip an empty list. Write the new logins to a request file:

```json
{"reviewers": ["login1", "login2"]}
```

Use the review-request endpoint:

```bash
gh api --method POST repos/apache/doris/pulls/<N>/requested_reviewers \
  --input request.json
gh api repos/apache/doris/pulls/<N>/requested_reviewers
```

Confirm the actual result, accounting for concurrent team assignment. Report
permission failures. If a 422 response identifies an ineligible person, record
the failure and consider the next qualified candidate for the same module.
Do not substitute assignees or mention comments for review requests, invite every
owner, remove existing requests, or report success after an API failure. Reruns
must reconcile live state before sending another request.

Report the PR, affected modules, newly requested people and representative
evidence, existing coverage, unresolved modules and failures.

## 5. Run locally and maintain the snapshot

See `README.md` for the input schema and complete live-PR preparation example.
`select_reviewers.py` always produces a local plan and sends no requests:

```bash
python3 tools/reviewer-routing/select_reviewers.py /tmp/reviewer-pr.json
```

Refresh after 30 days when practical; mappings older than 90 days must be rebuilt
before automatic invitations. Update the module table and JSON together after
recollecting a complete window. Do not change timestamps to bypass expiry.

## 6. Complete module candidate table

Each candidate has `R/A/M` counts: distinct nonself review PRs, PRs with an
approval, and nonself merges. Approval PRs are a subset of review PRs; merges do
not prove review. Up to five qualified candidates are shown in score order.
The JSON contains additional candidates, evidence and exact ranking scores.

| Module | PRs | Candidates (R/A/M) | Confidence |
|---|---:|---|---|
| `search`: Inverted indexes, full-text search and Search DSL | 187 | @eldenmoon (65/64/1); @csun5285 (51/50/2); @airborne12 (50/49/25); @yiguolei (43/41/12); @gavinchou (42/40/3) | high |
| `ann`: Vector indexes and approximate nearest-neighbor search | 42 | @yiguolei (19/19/8); @airborne12 (14/14/8); @zclllyybb (6/6/3) | high |
| `variant`: VARIANT, semi-structured data and JSON types | 193 | @yiguolei (68/66/30); @csun5285 (51/49/0); @eldenmoon (40/40/16); @airborne12 (33/32/5); @morrySnow (17/14/2) | high |
| `udf`: UDF, UDAF and UDTF | 61 | @HappenLee (24/21/7); @zclllyybb (14/14/11); @hello-stephen (6/5/8); @morningman (10/10/1) | high |
| `connector.iceberg`: Iceberg | 294 | @924060929 (96/95/5); @CalvinKirs (74/72/2); @morningman (93/91/62); @Gabriel39 (42/41/35); @hubgeter (34/32/0) | high |
| `connector.paimon`: Paimon | 176 | @924060929 (53/53/1); @CalvinKirs (50/50/3); @Gabriel39 (25/24/13); @morningman (54/53/29); @zhangstar333 (18/18/1) | high |
| `connector.hive`: Hive, HMS and Hudi | 163 | @924060929 (43/42/4); @CalvinKirs (48/47/1); @morningman (54/52/32); @zhangstar333 (19/19/0); @Gabriel39 (16/16/12) | high |
| `connector.jdbc`: JDBC and ADBC | 76 | @CalvinKirs (31/31/0); @924060929 (19/19/0); @zhangstar333 (8/7/0); @Gabriel39 (11/8/3); @morningman (15/15/7) | high |
| `connector.trino`: Trino connector | 26 | Insufficient evidence; manual decision required | manual |
| `connector.maxcompute`: MaxCompute | 52 | @Gabriel39 (8/8/7); @morningman (20/20/11); @CalvinKirs (22/22/0) | medium |
| `connector.fluss`: Fluss | 11 | Insufficient evidence; manual decision required | manual |
| `connector.es`: Elasticsearch | 25 | @924060929 (9/9/0) | medium |
| `connector.mongodb`: MongoDB | 5 | Insufficient evidence; manual decision required | manual |
| `connector`: External catalogs, connector framework and metadata caches | 364 | @924060929 (117/114/7); @CalvinKirs (124/119/7); @morningman (124/117/71); @Gabriel39 (33/31/22); @hubgeter (44/44/0) | high |
| `files.parquet`: Parquet readers and writers | 136 | @yiguolei (63/62/19); @Gabriel39 (26/26/13); @morningman (32/31/19); @zhangstar333 (10/10/1); @CalvinKirs (14/13/0) | high |
| `files.orc`: ORC readers and writers | 67 | @Gabriel39 (13/13/8); @yiguolei (29/28/11); @morningman (20/18/11) | high |
| `files`: File formats, CSV, JSON and file scan framework | 189 | @yiguolei (90/89/32); @Gabriel39 (43/43/21); @924060929 (36/36/0); @gavinchou (28/28/2); @hubgeter (13/13/0) | high |
| `io.cache`: File cache and remote cache | 84 | @gavinchou (71/68/24); @liaoxin01 (17/14/5); @deardeng (3/3/0) | high |
| `io`: Object storage, HDFS and filesystems | 206 | @liaoxin01 (84/76/6); @gavinchou (92/87/34); @CalvinKirs (46/45/0); @morningman (36/34/9); @924060929 (17/17/1) | high |
| `functions.aggregate`: Aggregate functions | 136 | @HappenLee (42/38/14); @Mryange (22/18/4); @morrySnow (27/21/5); @zclllyybb (32/32/17); @mrhhsg (13/12/1) | high |
| `functions`: Scalar functions, expressions, casts and table functions | 669 | @HappenLee (127/109/39); @yiguolei (140/133/63); @924060929 (117/114/8); @morrySnow (102/82/27); @zclllyybb (129/127/52) | high |
| `types`: Columns, Block, types and core data containers | 278 | @yiguolei (114/108/49); @HappenLee (54/48/12); @Gabriel39 (44/43/16); @zclllyybb (57/57/18); @BiteTheDDDDt (34/31/5) | high |
| `optimizer`: Nereids optimizer, SQL analysis and planning | 949 | @morrySnow (363/308/115); @924060929 (271/265/31); @starocean999 (216/209/35); @englefly (86/81/9); @gavinchou (121/115/21) | high |
| `statistics`: Statistics collection and cardinality estimation | 80 | @morrySnow (41/33/17); @starocean999 (23/21/1); @englefly (11/11/3); @yujun777 (9/9/0); @feiniaofeiafei (9/9/0) | high |
| `mtmv`: Materialized views, asynchronous MV and incremental maintenance | 117 | @morrySnow (77/74/32); @yujun777 (21/21/2); @starocean999 (24/21/5); @924060929 (21/21/1); @seawinde (12/10/0) | high |
| `execution.spill`: Operator spilling | 13 | @yiguolei (4/4/4) | medium |
| `execution`: Pipeline, join, aggregation, sort, scan and exchange | 587 | @yiguolei (194/185/83); @HappenLee (116/100/23); @Gabriel39 (73/71/16); @BiteTheDDDDt (75/66/14); @Mryange (51/47/1) | high |
| `runtime.workload`: Workload groups, queues and resource management | 45 | @yiguolei (18/16/10); @zclllyybb (6/6/1); @HappenLee (5/4/2); @jacktengg (5/3/0); @hello-stephen (3/3/2) | high |
| `runtime.memory`: Memory management and tracking | 21 | Insufficient evidence; manual decision required | manual |
| `runtime`: BE runtime, utilities, profiles and common infrastructure | 361 | @gavinchou (120/112/22); @yiguolei (86/81/39); @HappenLee (43/38/12); @eldenmoon (37/37/8); @liaoxin01 (53/48/15) | high |
| `load`: Ingestion, Stream Load, Routine Load and write paths | 423 | @liaoxin01 (178/169/45); @sollhui (50/49/4); @gavinchou (95/91/22); @CalvinKirs (69/66/0); @924060929 (57/56/2) | high |
| `transaction`: Transactions and commit protocols | 47 | @gavinchou (33/32/5); @morrySnow (16/15/1) | medium |
| `storage.compaction`: Compaction and rowset merging | 73 | @gavinchou (53/53/14); @luwei16 (13/13/5); @yiguolei (12/11/5); @hello-stephen (4/4/6); @eldenmoon (11/11/3) | high |
| `schema-change`: Schema change, alter, rollup and index changes | 138 | @gavinchou (85/83/17); @luwei16 (28/28/5); @morrySnow (25/23/3); @hello-stephen (9/8/8); @yiguolei (25/23/7) | high |
| `storage.mow`: Unique keys, merge on write and delete bitmaps | 37 | @gavinchou (24/24/4); @liaoxin01 (8/7/2); @yiguolei (13/13/6); @bobhan1 (5/3/0) | medium |
| `storage`: Tablets, rowsets, segments and the local storage engine | 414 | @gavinchou (172/171/39); @yiguolei (155/150/67); @csun5285 (76/71/3); @eldenmoon (67/65/8); @liaoxin01 (50/43/11) | high |
| `replication`: Replica scheduling, cloning, balancing and BE agents | 105 | @gavinchou (70/68/15); @yiguolei (20/18/3); @luwei16 (10/9/2); @eldenmoon (14/14/2); @HappenLee (11/8/2) | high |
| `backup`: Backup, restore and snapshots | 41 | @gavinchou (29/27/6) | high |
| `cloud.recycler`: Cloud recycler and garbage collection | 87 | @gavinchou (68/63/33); @liaoxin01 (21/20/14); @wyxxxcat (16/16/0); @luwei16 (5/5/1); @mymeiyi (3/2/0) | high |
| `cloud.meta`: Cloud Meta Service and Meta Store | 121 | @gavinchou (91/88/46); @liaoxin01 (28/26/8); @wyxxxcat (17/17/0); @luwei16 (14/14/4); @dataroaring (19/16/10) | high |
| `cloud`: Storage-compute separation, Cloud BE/FE and resource management | 377 | @gavinchou (308/300/114); @liaoxin01 (69/65/23); @luwei16 (40/40/9); @hello-stephen (19/18/26); @deardeng (24/22/0) | high |
| `auth`: Authentication, authorization, Ranger and connection authentication | 70 | @CalvinKirs (19/18/2); @morningman (23/23/6); @morrySnow (19/14/3); @hello-stephen (7/7/6); @924060929 (12/12/1) | high |
| `query`: Query coordination, sessions, protocols and execution management | 396 | @924060929 (117/111/13); @morrySnow (107/96/25); @gavinchou (85/79/14); @yiguolei (53/49/23); @Gabriel39 (24/22/8) | high |
| `metadata`: Catalog, DDL, partitions and metadata models | 375 | @morrySnow (97/81/20); @gavinchou (122/115/27); @924060929 (77/75/6); @CalvinKirs (68/63/3); @morningman (72/71/35) | high |
| `persistence`: Journal, EditLog, HA and metadata persistence | 60 | @gavinchou (36/33/9); @CalvinKirs (20/20/0); @morningman (19/18/7) | high |
| `cluster`: FE/BE node management, deployment and heartbeats | 55 | @gavinchou (39/37/8); @morningman (16/16/9); @hello-stephen (5/4/4) | high |
| `api`: HTTP APIs, system tables, monitoring and metrics | 260 | @gavinchou (98/91/24); @liaoxin01 (51/48/13); @yiguolei (41/40/18); @924060929 (36/36/3); @morningman (41/38/10) | high |
| `fe.foundation`: FE common modules, configuration and infrastructure | 338 | @gavinchou (115/106/27); @morningman (88/82/42); @924060929 (72/71/5); @morrySnow (66/59/13); @liaoxin01 (52/48/5) | high |
| `ci`: CI, GitHub workflows and automation | 314 | @hello-stephen (83/82/66); @morningman (72/70/32); @yiguolei (37/34/12); @shuke987 (15/15/5); @Gabriel39 (27/26/12) | high |
| `build`: Builds, dependencies, packaging and releases | 413 | @morningman (129/125/39); @yiguolei (61/57/24); @CalvinKirs (104/101/6); @hello-stephen (42/42/39); @gavinchou (83/78/18) | high |
| `test.framework`: Test frameworks, services and runner scripts | 114 | @hello-stephen (15/14/20); @gavinchou (34/33/6); @morrySnow (19/17/6); @liaoxin01 (15/15/4); @CalvinKirs (20/20/0) | high |
| `docs-tools`: Documentation, examples, tools and web interfaces | 113 | @hello-stephen (18/18/9); @morningman (21/18/5); @gavinchou (27/25/8); @yiguolei (20/19/6); @morrySnow (14/10/2) | high |
| `protocol`: Thrift and Protobuf interfaces | 281 | @yiguolei (76/72/25); @924060929 (62/60/0); @gavinchou (86/81/15); @morrySnow (48/35/2); @HappenLee (38/29/6) | high |
| `fe.other`: Unclassified FE paths | 47 | Insufficient evidence; manual decision required | manual |
| `be.other`: Unclassified BE paths | 29 | Insufficient evidence; manual decision required | manual |
| `test.other`: Tests without a confirmed specialized module | 101 | Insufficient evidence; manual decision required | manual |
| `other`: Unclassified paths requiring manual inspection | 98 | Insufficient evidence; manual decision required | manual |

One PR may affect multiple modules, so module PR counts do not sum to the
total PR count. Companion tests do not add routing modules when production
paths are present. Candidate statistics do not include live permissions, leave
or pending review workload.
