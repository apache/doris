# FIX-R12 — OpenCSV serde schema flattened to all-STRING (connector-side, Trino-aligned)

**Suites fixed:** `test_open_csv_serde`, `test_hive_serde_prop` (OpenCSV half).
**Scope:** `fe-connector-hive` only. fe-core untouched; shared `fe-connector-hms` untouched.
**Status:** code DONE (`HiveConnectorMetadata` + unit tests, 306 module UT + 4 new green, 0 checkstyle). e2e awaits user self-run.

---

## 1. Symptom

Two hive regression suites read wrong values from `OpenCSVSerde` (comma/quote-delimited plain-text) tables:
- `test_open_csv_serde` — 3 pure-OpenCSV tables (`run76.hql`), including a complex-type table whose `array/map/struct`
  columns render as flat string leaves in the golden.
- `test_hive_serde_prop` — mixed: an **OpenCSV** half (`open_csv_default_prop`: `is_active` renders `TRUE` as raw
  uppercase text, declared-typed columns render **empty string, not `\N`**) plus a **LazySimpleSerDe** half
  (`serde_test8` → `\N`, `test_empty_null_format_text3`) that must stay typed + null-format-aware.

If the FE declares these columns with their raw types (`int/date/boolean/…`), BE parses them → `TRUE` vs `'true'`,
raw datetime vs ISO, empty-string vs `NULL` (`IS NULL` vs `= ''`), complex columns mis-shaped.

## 2. Root cause (HEAD-verified)

Legacy `HMSExternalTable.initHiveSchema` (`HMSExternalTable.java:765`) resolved a table's columns **by default**
through the metastore **`get_schema` RPC** (`ThriftHMSCachedClient.getSchema:427` → `get_schema_with_environment_context`).
That RPC runs the table's SerDe **on the HMS server**: for serdes Hive does *not* keep columns in the metastore for
(OpenCSVSerde, RegexSerDe, …) it returns the deserializer's schema — and `OpenCSVSerde`'s ObjectInspector reports
**every top-level column as `string`**. Raw `sd.getCols()` was only the opt-in `get_schema_from_table=true` arm.

The new connector `ThriftHmsClient.convertTable` (`ThriftHmsClient.java:653`) always reads raw `sd.getCols()` — i.e. it
behaves like legacy's *non-default* arm, so OpenCSV declared types (int/date/bool/complex) reach BE unflattened. That is
the regression.

For the common serdes (LazySimple text, ORC, Parquet) `get_schema` == `sd.getCols()`, so those were never affected —
this is an OpenCSV-shaped problem, not an all-tables problem.

## 3. Options considered

| | **A — restore `get_schema` RPC (full legacy parity)** | **B — connector STRING-force on OpenCSV (chosen)** |
|---|---|---|
| Mechanism | add `getSchema` to `HmsClient` + `ThriftHmsClient`, call `get_schema` by default, wire a `get_schema_from_table` opt-out | in the hive metadata layer, when serde == OpenCSVSerde, flatten every DATA column to `STRING` |
| Fixes both suites | yes | yes |
| Extra RPC / table load | +1 | 0 |
| Partition-key re-split hazard | **yes** (`get_schema` concatenates partition keys; connector re-appends them → must strip) | none (partition keys never touched) |
| Server-side serde-jar dependency | **yes** (OpenX-JSON / contrib-MultiDelimit jar absent on HMS ⇒ `MetaException` breaks whole table — the exact failure `get_schema_from_table=true` exists to bypass) | none |
| Touches hudi-on-HMS schema path | **yes** (shared `HmsTableInfo.getColumns` feeds `HudiConnectorMetadata.getSchemaFromHms:902`) | no |
| Avro schema-url self-describing tables | resolves them (only A) | does not (see §7 — pre-existing gap, not worsened) |
| Trino alignment | against (Trino avoids `get_fields`/`get_schema`) | with (Trino forces CSV=all-VARCHAR in `HiveMetadata`, not the metastore client) |

**Decision: Option B — user signed off 2026-07-11.** The end-state (OpenCSV → all-STRING, encoded in the goldens) is
already agreed; A and B differ only on Avro-schema-url, a table shape absent from the failing suites and unread by the
connector today. B is smaller, RPC-free, and does not fight the SPI grain.

## 4. Placement (architecture red-team refinement)

The first draft put the branch in `ThriftHmsClient.convertTable` (**fe-connector-hms**) — the shared raw-metastore
module that also feeds the **hudi** connector, and which has zero OpenCSV knowledge today (it would have needed a
mirrored FQCN constant). Relocated to **`HiveConnectorMetadata.buildColumns`** (fe-connector-hive), where:
- `HmsTableInfo.getSerializationLib()` is already in hand (already consumed at `HiveConnectorMetadata.java:383`);
- the sibling same-category op lives (`coercePartitionKeyStringToVarchar`, string→varchar coercion);
- the canonical OpenCSV constant already exists (`HiveTextProperties.HIVE_OPEN_CSV_SERDE`, same package — **no duplicate**);
- `buildColumns` is the single choke point for both consumers (`getTableSchema:415`, `getViewDefinition:613`).

This keeps serde-specific typing off the hms module hudi shares, and mirrors the layer Trino applies CSV typing in.

## 5. The change (`HiveConnectorMetadata.java`)

`buildColumns` ends with `return coerceOpenCsvColumnsToString(tableInfo, columns);` (applied after default-value
enrichment). New helper:

```java
private static List<ConnectorColumn> coerceOpenCsvColumnsToString(
        HmsTableInfo tableInfo, List<ConnectorColumn> columns) {
    if (isView(tableInfo)
            || !HiveTextProperties.HIVE_OPEN_CSV_SERDE.equals(tableInfo.getSerializationLib())) {
        return columns;                              // non-OpenCSV / view → byte-identical to sd.getCols()
    }
    ConnectorType stringType = ConnectorType.of("STRING");
    List<ConnectorColumn> forced = new ArrayList<>(columns.size());
    for (ConnectorColumn col : columns) {
        forced.add(new ConnectorColumn(col.getName(), stringType, col.getComment(),
                col.isNullable(), col.getDefaultValue(),
                col.isKey(), col.isAutoInc(), col.isAggregated()));   // mirrors coercePartitionKey rebuild
    }
    return forced;
}
```

- **Whole-type replacement, not primitive widening**: OpenCSV serves `array/map/struct` as one flat string too; the
  golden `test_open_csv_serde.out` complex table confirms. Force to flat `STRING` (= what a declared `string` maps to,
  `HmsTypeMapping` `"string"→ConnectorType.of("STRING")`), discarding the declared type entirely.
- **Partition keys untouched** — they come from `buildPartitionKeys` + `coercePartitionKeyStringToVarchar`, never through
  this helper (hive appends partition keys after the deserializer, so legacy kept them typed too). No double-count.
- **`typeMappingOptions` (varbinary/timestamptz) moot for OpenCSV data columns** — every one becomes STRING before any
  mapping flag could apply, matching legacy (OpenCSV all-string is produced before Doris type-mapping). Partition keys
  still honor the options via `convertFieldSchemas`.
- **`firstColumnIsString(tableInfo)` (handle stamp, `:388`) reads raw columns — left as is**: its only consumer is the
  OpenX-**JSON** one-column read gate; OpenCSV routes to `FORMAT_CSV_PLAIN` (`HiveFileFormat:160`) and never consults it,
  so raw-vs-forced is inert here. (Noted so a future reader doesn't "fix" it.)
- **`isView` guard**: `buildColumns` also serves `getViewDefinition`; a view is never an OpenCSV data payload, guard
  prevents flattening a view's logical columns in the pathological OpenCSV-view case.

## 6. Tests — RED/GREEN

`HiveConnectorMetadataSchemaTest` (+4): `testOpenCsvSerdeFlattensEveryDataColumnToString` (int/datetime/boolean/**array**
→ STRING), `testOpenCsvSerdePartitionKeyKeepsDeclaredType` (DATE partition key stays date), `testNonOpenCsvSerdeKeepsDeclaredDataTypes`
(LazySimple gate — id INT/ts DATETIMEV2 intact), `testOpenCsvViewColumnsAreNotFlattened` (isView guard).

- **RED verified**: with the coerce call bypassed, `testOpenCsvSerdeFlattensEveryDataColumnToString` fails
  (`expected <STRING> but was <INT>`); the LazySimple gate test stays green (proves the flatten test targets the fix).
- **GREEN**: fe-connector-hive **306/306 module UT** (17→21 in this class), 0 checkstyle, BUILD SUCCESS.

## 7. Residual risk / notes

1. **Avro schema-url self-describing tables** — the sole A-vs-B divergence. The connector has zero
   `avro.schema.url/literal` handling and resolves from `sd.getCols()`; an Avro table carrying only a schema URL with
   empty/stale `sd.cols` reads its columns wrong. This gap is **pre-existing** (B neither creates nor worsens it) and
   absent from these suites. If ever needed, it is a separate Avro-scoped connector change — not a reason to route all
   loads through `get_schema`.
2. **Latent order-fragility (informational)** — `test_utf8_check` qt_4 (`invalid_utf8_data2`, OpenCSV, `id INT`, `order by id`)
   is now string-ordered on `id`; it stays green only because all ids are single-digit (string order == int order).
   Not a current failure; noted so a future multi-digit id there isn't a surprise.

## 8. Acceptance (e2e — user self-run)

Gate suites (blast red-team widened 2 → 4):
- `external_table_p0/hive/test_open_csv_serde` — target, hive3.
- `external_table_p0/hive/test_hive_serde_prop` — target, hive2 + hive3.
- `external_table_p0/hive/test_drop_expired_table_stats` — runs `analyze`/`show table stats` on OpenCSV `employee_gz`
  (already all-string → flatten is a no-op); must stay green.
- `external_table_p0/trino_connector/hive/test_trino_hive_serde_prop` — untouched control (trino-connector path, own
  metastore client); confirms it is unaffected.

All must pass against existing `.out` goldens with **no golden edits** (a needed golden edit signals divergence from legacy).
