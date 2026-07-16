# FIX-R2 — Native hive SHOW CREATE TABLE on the flipped plugin path (Option X)

**Suites fixed:** `test_hive_show_create_table`, `test_hive_ddl_text_format` (+ acceptance guards
`test_hive_meta_cache`, `test_multi_delimit_serde`, `test_hive_ddl`).
**Scope:** lazy connector method + fresh HMS fetch + connector-side renderer; fe-core stays connector-agnostic.
**Status:** code DONE (`17764d03665`). e2e awaits user self-run.

---

## 1. Symptom

`SHOW CREATE TABLE` on a flipped (plugin) hive table emitted generic Doris-style DDL (double-quoted
`PROPERTIES ("k" = "v")`) instead of native hive DDL — no `ROW FORMAT SERDE`, `WITH SERDEPROPERTIES`,
`STORED AS INPUTFORMAT/OUTPUTFORMAT`. The suites substring-assert the native clauses and their exact quoting.

## 2. Root cause

Legacy rendered native hive DDL in `ShowCreateTableCommand`'s `HMS_EXTERNAL_TABLE` arm
(`HiveMetaStoreClientHelper.showCreateTable`). Post-cutover a hive table is `PLUGIN_EXTERNAL_TABLE`, so that arm
is dead and it falls to the generic `Env.getDdlStmt` assembler, which structurally cannot emit hive serde/format
clauses and double-quotes property keys.

## 3. Two earlier designs red-teamed OUT (why Option X)

- **Eager reserved-key rendered in `getTableSchema`** — REFUTED: `test_hive_meta_cache` (L354-357) requires SHOW
  CREATE to fetch **fresh** from HMS at command time (after an external `ADD COLUMNS`, `DESC` shows the cached
  5-col schema but SHOW CREATE must show the fresh 6). An eager render baked into the schema-cache value is as
  stale as DESC.
- **Declaring `SUPPORTS_SHOW_CREATE_DDL` + rendering in `Env.getDdlStmt` (Option Y)** — REFUTED: the capability
  gate is connector-wide, so it would flip for **delegated hudi/iceberg-on-HMS** tables and every other
  `Env.getDdlStmt` caller (the HTTP `StmtExecutionAction.getSchema` endpoint) — coupling hive can't honor.

**Decision: Option X — user signed off 2026-07-12.** Lazy connector render + fresh fetch, intercepted only in
`ShowCreateTableCommand`, capability NOT declared. (Trino renders hive SHOW CREATE engine-side in Trino syntax,
not native hive DDL, so neither X nor Y mirrors Trino; native parity is a Doris-legacy requirement best served by
a connector-returned verbatim string.)

## 4. The change (7 files)

1. **`ConnectorTableOps.renderShowCreateTableDdl(session, handle)`** — new `default Optional<String>` = empty.
   iceberg/paimon/es/jdbc inherit it → return empty → fall through to `Env.getDdlStmt` byte-inert.
2. **`HmsClient.getTableFresh` + `CachingHmsClient` override** — cache-bypassing table read (exact mirror of the
   `listPartitionNamesFresh` precedent; neither reads nor writes `tableCache`). This is what makes SHOW CREATE
   fresh while DESC stays cached.
3. **`HiveShowCreateTableRenderer`** (fe-connector-hms, new) — byte-for-byte port of legacy
   `HiveMetaStoreClientHelper.showCreateTable` base-table branch. Load-bearing details: **two quoting
   conventions** (`WITH SERDEPROPERTIES ('k' = 'v')` spaces vs `TBLPROPERTIES ('k'='v')` none), 2-space data /
   1-space partition indents, the `comment` param lifted to a top-level `COMMENT` clause, and the **null-comment
   guard** (an empty `COMMENT ''` would break meta_cache's exact column substring). Types via
   `HmsTypeMapping.toHiveTypeString`.
4. **`HiveConnectorMetadata.renderShowCreateTableDdl`** — guards `!(handle instanceof HiveTableHandle)` → empty
   (a delegated iceberg/hudi-on-HMS table routes through THIS hive gateway metadata and carries the sibling's
   foreign handle; mirror `getTableSchema`'s guard, whose comment already notes "show-create ... is inert here").
   Fresh `getTableFresh` + render.
5. **`PluginDrivenExternalTable.getShowCreateTableDdl()`** — mirrors `getViewText` (safe under the command's
   read-lock; no `makeSureInitialized`); returns empty on unresolved handle (red-team soften, keeps iceberg/paimon
   at today's behavior even in edge cases).
6. **`ShowCreateTableCommand`** — new arm after the view arm, before the `Env.getDdlStmt` fallthrough. Guard is
   the method returning present (NOT `instanceof`/source name), so iceberg/paimon (empty) and flipped views (view
   arm fires first) are untouched. `Env.getDdlStmt` is not modified.

**Iron rule:** fe-core branches only on the generic `PluginDrivenExternalTable` type + the method result, never on
"hive". All serde/format/DDL formatting lives in `fe-connector-hms`. **TCCL:** the fetch pins inside
`ThriftHmsClient.doAs`; no fe-core pin (matches the `getMetadataTableRows`/`getChunkSizes` precedent). The renderer
is reflection-free, so it is CL-inert on the fe-core thread — noted in its javadoc for future maintainers.

## 5. Tests — RED/GREEN

- `HiveShowCreateTableRendererTest` (fe-connector-hms, 5): byte-parity — column block indent/types + null-comment
  guard, comment-lift + 1-space partition indent, serde/STORED-AS/LOCATION blocks, the two quoting conventions,
  and comment-not-leaked-into-TBLPROPERTIES. RED before the class existed / against any wrong-quoting impl.
- `CachingHmsClientTest` +3: `getTableFresh` always hits the delegate, does not populate the cache, and the
  non-caching default equals a plain `getTable`. RED without the `CachingHmsClient` override (would serve cached).
- Regression: fe-core `PluginDrivenExternalTableTest` 36/36 + `EnvShowCreatePluginTableTest` 3/3 (capability arm
  unchanged — hive still does not declare it); fe-connector-hive 307/307; 0 checkstyle.

## 6. Residual / fidelity notes

- **`toHiveTypeString` fidelity**: a raw HMS `varchar(n)` renders as `string` (length dropped) — harmless here
  because such columns are stored as `string` in HMS. `char(n)`/`decimal(p,s)`/date/timestamp/nested round-trip
  cleanly. An unmappable type throws `IllegalArgumentException` (legacy echoed the raw string and could not hit
  this) — left to fail loud rather than guessed (not fed by any suite).
- **TBLPROPERTIES** emits all non-`comment` params verbatim (incl. volatile `transient_lastDdlTime` etc.), matching
  legacy. Suites assert substrings only, so no `.out` golden — do not add one (it would be flaky).

## 7. Acceptance (e2e — user self-run)

`external_table_p0/hive/`: `test_hive_show_create_table` (hive2), `test_hive_ddl_text_format` (hive2+hive3),
`test_hive_meta_cache` (the fresh-fetch contract: DESC=5 cached / SHOW CREATE=6 fresh), `test_multi_delimit_serde`,
`test_hive_ddl`. All are inline `contains`/`assertTrue` (no `.out` goldens).
