# P5 Paimon — Parity Baseline (before/after cutover)

> Task: **P5-T02** (batch B0). Companion to the FE→BE round-trip smoke + the 3-way
> `paimon.version` pin (R-007). This doc is the **parity contract** for the paimon
> connector-SPI migration: it enumerates the existing regression coverage firsthand, states
> WHY no new "after" suite needs to be authored, and flags the real gaps that DO need new
> coverage or a live-e2e hard gate.
>
> Grounding date: 2026-06-09. All suite paths verified firsthand under
> `regression-test/suites/`. Numbers below are from direct enumeration, not estimates.

---

## 1. Cutover-gate model — why the SAME suites are the before/after parity gate

The paimon read path today runs through the **legacy `PaimonScanNode`**
(`fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonScanNode.java`).

Cutover (batch **B7**) is a single switch: add `"paimon"` to `SPI_READY_TYPES` in
`fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java:51-52` (today
`{"jdbc","es","trino-connector","max_compute"}`). Once paimon is in that set,
`createCatalog` builds a `PluginDrivenExternalCatalog` backed by the SPI connector instead
of the legacy `PaimonExternalCatalog`, and the read path flows through the connector's
`PaimonScanPlanProvider` instead of `PaimonScanNode`.

**Consequence:** every regression suite below runs unchanged. Before B7 it exercises the
legacy path; after B7 the IDENTICAL suite exercises the connector-SPI path. The suites are
therefore the **before/after parity gate by construction** — run them on master (legacy),
then on the cutover branch (SPI), and diff the `.out` results. No separate "connector-path"
regression suite needs to be authored; authoring one would just duplicate these.

This mirrors how P4 maxcompute was gated (see `tasks/P5-paimon-migration.md` §当前阻塞项
"翻闸前置硬门 ③ B0 parity baseline 绿").

---

## 2. Parity dimension → covering suite(s) → status

Legend — **Status**: ✅ covered by automated regression that flips at cutover; ⚠️ partial
(only one side of a toggle, or behind an env that CI skips); ❌ no automated coverage (real
gap, see §3). All suites are tagged `p0,external` (the `external_table_p0/paimon/**` group)
unless noted P2.

| # | Parity dimension | Covering suite(s) | Status |
|---|---|---|---|
| 1 | SELECT * no-predicate | `test_paimon_catalog`, `test_paimon_table`, `paimon_base_filesystem` | ✅ |
| 2 | Predicate pushdown (explain + row correctness) | `test_paimon_predict` (per-column `explain{}` + qt row checks), `paimon_base_filesystem` | ✅ (legacy converter) / ❌ (connector `PaimonPredicateConverter` UT — see §3.1) |
| 3 | Partition pruning | `test_paimon_partition_table`, `paimon_partition_legacy` | ✅ |
| 4 | Runtime-filter partition pruning | `test_paimon_runtime_filter_partition_pruning` | ✅ |
| 5 | Native (ORC/Parquet) vs JNI split classification | `test_paimon_cpp_reader` (toggles `enable_paimon_cpp_reader` false→true), `paimon_tb_mix_format`, `test_paimon_deletion_vector` (loops `force_jni_scanner` **false AND true**) | ⚠️ — native path IS exercised, but only via the *legacy* split classifier; the **connector** native/JNI classification has no dedicated assertion (see §3.2) |
| 6 | Deletion vector | `test_paimon_deletion_vector` (orc+parquet, `force_jni` false+true), `test_paimon_deletion_vector_oss` (P2/env) | ✅ for legacy; ⚠️ connector deletion-file plumbing un-asserted (see §3.2) |
| 7 | COUNT pushdown | `test_paimon_count` (append + merge-on-read), `test_paimon_deletion_vector` count-pushdown block | ✅ |
| 8 | Incremental read | `paimon_incr_read` (parameterized by `force_jni`) | ✅ |
| 9 | Time-travel / snapshot pin | `paimon_time_travel` (snapshot-id + commit-time + tag) | ✅ |
| 10 | Sys-tables `$snapshots/$files/$manifests/$schemas/$options` | `paimon_system_table`, `paimon_data_system_table` | ✅ |
| 11 | Sys-tables `$binlog` / `$audit_log` (**forced-JNI** override) | `paimon_data_system_table` (`$audit_log`, `$binlog` rowkind queries) | ⚠️ — exercised on legacy; the **forced-JNI override for these sys-tables** is a connector-side behavior with no parity assertion (see §3.3) |
| 12 | Sys-table auth | `test_paimon_system_table_auth` | ✅ |
| 13 | Session-TZ datetime predicate | `test_paimon_catalog_timestamp_tz`, `test_paimon_timestamp_with_time_zone`, `paimon_time_travel` (uses `time_zone`) | ✅ |
| 14 | Schema evolution | `test_paimon_schema_change`, `test_paimon_full_schema_change` | ✅ |
| 15 | The 6 flavors (filesystem / HMS / DLF / DLF-REST / JDBC / S3-storage) | filesystem: `paimon_base_filesystem`, `test_paimon_catalog`; HMS: `test_paimon_hms_catalog` (P2); DLF: `test_paimon_dlf_catalog`, `test_paimon_dlf_catalog_new_param`, `test_paimon_dlf_catalog_miss_dlf_param` (P2); DLF-REST: `test_paimon_dlf_rest_catalog` (P2); JDBC: `test_paimon_jdbc_catalog`; storage: `test_paimon_s3`, `test_paimon_minio`, `test_paimon_gcs` | ⚠️ — filesystem+JDBC run in CI; **HMS/DLF/DLF-REST/S3/OSS/MinIO/GCS are env-gated** (real remote creds), CI skips → live-e2e only (see §3.4) |
| 16 | Types (char/varchar, binary→varbinary, timestamp-tz, timestamp-types) | `test_paimon_char_varchar_type`, `test_paimon_catalog_varbinary`, `test_paimon_timestamp_with_time_zone`, `paimon_timestamp_types` | ✅ |
| 17 | MTMV staleness keying (snapshot-pin) | `test_paimon_mtmv`, `test_paimon_rewrite_mtmv`, `test_paimon_olap_rewrite_mtmv` (`mtmv_p0/**`) | ✅ legacy; ⚠️ connector single-snapshot-pin invariant lands in B5 (P5-T25) and needs its own UT |
| — | Misc legacy coverage that also flips at cutover | `test_paimon_statistics`, `test_paimon_table_stats`, `test_paimon_table_properties`, `test_paimon_table_meta_cache`, `test_paimon_sql_block_rule` | ✅ |
| UT | FE planning unit test | fe-core `PaimonScanNodeTest` (`fe/fe-core/src/test/java/org/apache/doris/datasource/paimon/source/PaimonScanNodeTest.java`) | ⚠️ — pins the **legacy** scan node; does NOT cover the connector `PaimonScanPlanProvider` (see §3.1) |

**Firsthand counts:** 33 groovy suites under `external_table_p0/paimon/`, 6 under
`external_table_p2/paimon/` (HMS/DLF flavors), 3 MTMV suites under `mtmv_p0/`, plus the
fe-core UT `PaimonScanNodeTest`. (~41 P0 in the original estimate = the p0 paimon suites +
their inter-suite cases; the P2 flavor suites are additional.)

> **Correction to the T02 brief (Rule 12 — fail loud):** the brief stated "existing tests
> force_jni only". Firsthand this is **inaccurate** — `test_paimon_deletion_vector` calls
> `test_cases("false"); test_cases("true")` and `test_paimon_cpp_reader` toggles
> `enable_paimon_cpp_reader` false→true, so the **native (non-JNI) reader path IS exercised**
> on the legacy side. The real native gap is connector-side assertion, not "no native test at
> all" — see §3.2.

---

## 3. Real gaps with NO automated parity coverage

These are the holes the regression suites in §2 do **not** close. They are the substance of
the cutover risk and must be addressed by new UTs and/or the live-e2e hard gate.

### 3.1 Connector-path predicate-conversion UT (❌ no coverage)
`fe-connector-paimon` has `PaimonPredicateConverter` but **no unit test** for it — contrast
`fe-connector-trino/.../TrinoPredicateConverterTest.java` and
`fe-connector-maxcompute/.../MaxComputePredicateConverterTest.java`, which both exist. Note the
**legacy** converter (`datasource.paimon.source.PaimonPredicateConverter`) *does* have a direct
fe-core unit test (`fe/fe-core/src/test/java/org/apache/doris/planner/PaimonPredicateConverterTest.java`)
on top of the `test_paimon_predict` row checks; the gap is specifically that the **connector**
converter (`ConnectorExpression` → `org.apache.paimon.predicate.Predicate`) has no
direct test. **Recommended:** a connector-side `PaimonPredicateConverterTest` (offline,
no fe-core import) covering each op (eq/ne/lt/le/gt/ge/in/isNull/and/or) + the datetime/TZ
literal-format edge (the source-TZ session gotcha that bit maxcompute — see MEMORY
"连接器 session TZ"). This is the highest-value missing UT.

### 3.2 Native-reader + deletion-vector connector parity (⚠️ partial)
The native ORC/Parquet path and deletion-vector merge are exercised on the **legacy** split
classifier (`test_paimon_cpp_reader`, `test_paimon_deletion_vector`). After cutover the
classification + deletion-file plumbing run through the **connector** `PaimonScanPlanProvider`
(`buildJniScanRange` / `RawFile` / `DeletionFile` handling, ~`PaimonScanPlanProvider.java`).
There is no assertion that the connector emits the same native-vs-JNI split decision or the
same deletion-file list. The before/after run (§4) covers row correctness, but a focused
**connector split-classification UT** would localize a regression instead of surfacing it as
a wrong row count three suites away.

### 3.3 Sys-table forced-JNI override (⚠️ partial)
`$binlog` / `$audit_log` (and other non-data sys-tables) must be **forced to the JNI reader**
even when native is otherwise preferred. `paimon_data_system_table` exercises the queries but
nothing asserts the *forced-JNI override decision itself* on the connector path. After cutover
this override lives in the connector; a regression here would silently route a sys-table to a
native reader that cannot read it. Needs an explicit connector-side assertion (or a
before/after explain diff on `$binlog`).

### 3.4 Live-e2e hard gate (CI skips — env-gated)
The flavor suites for **HMS / DLF / DLF-REST / S3 / OSS / MinIO / GCS** (§2 row 15) require
real remote catalogs/credentials and are **skipped in CI**. The connector's per-flavor
catalog assembly (`PaimonConnector.createCatalog` switch + per-flavor
`ExecutionAuthenticator`, P5-T03) is therefore **not** validated by CI at all. This is the
single biggest before/after risk and must be a **user-run live-e2e hard gate** before
cutover (consistent with `tasks/P5-paimon-migration.md` §翻闸前置硬门 ①).

---

## 4. Live-e2e run plan (user-run, pre-cutover hard gate)

CI proves only the offline + filesystem/JDBC slice. The live gate proves the env-gated
flavors and the full read path against a real paimon deployment. Run this **twice** — once on
`master` (legacy) and once on the cutover branch (paimon in `SPI_READY_TYPES`) — and diff the
`.out` files. Identical output = parity.

1. **Per-flavor connectivity + read** (one warehouse per flavor):
   - filesystem (local/HDFS), HMS, DLF, DLF-REST, JDBC, plus S3/OSS/MinIO/GCS storage.
   - For each: `SHOW DATABASES`, `SHOW TABLES`, `SELECT *` (no predicate), one predicate
     query, one partition-pruned query → diff vs legacy.
2. **Read-path matrix on at least one keyed+partitioned table per flavor:**
   predicate pushdown, partition pruning, runtime-filter pruning, COUNT pushdown,
   deletion-vector (orc+parquet), native vs `set force_jni_scanner=true`, incremental read,
   time-travel (snapshot-id + tag), `$snapshots/$files/$binlog/$audit_log`, session-TZ
   datetime predicate.
3. **MTMV staleness:** create an MV over a paimon source, mutate the source, confirm the MV
   detects staleness via the snapshot-pin key (P5-T25 invariant) → diff vs legacy.
4. **Gate criterion:** every diff empty. Any non-empty diff blocks cutover (B7).

The offline FE→BE round-trip smoke (`PaimonTableSerdeRoundTripTest`, this task) is the CI-side
companion to step 2's serialization — it catches version-drift / non-serializable / base64
breaks that step 2 would otherwise only surface as a runtime BE failure.

---

## 5. What this task (T02) delivers vs what remains

- **Delivered (T02):** this parity-baseline doc; the offline FE→BE serialized-`Table`
  round-trip smoke (CI, not env-gated); the R-007 3-way `paimon.version` pin comment in
  `fe/pom.xml`.
- **Remaining (other tasks):** the connector `PaimonPredicateConverterTest` (§3.1), the
  connector split-classification / deletion-vector / forced-JNI assertions (§3.2–3.3), the
  MTMV snapshot-pin UT (P5-T25), and the user-run live-e2e hard gate (§4). These are tracked
  in `tasks/P5-paimon-migration.md`.
