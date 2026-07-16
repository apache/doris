# FIX-R10 — OpenX JSON `ignore.malformed.json` not honored

Suite: `test_hive_openx_json` (order_qt_q1). Effort: M. Scope: fe-connector-hive + fe-core (1 mirror line).

## Problem

A Hive JSON table declared with OpenX serde property `ignore.malformed.json=true` should skip malformed
rows; a query over it returns the well-formed rows. On the plugin-driven path the flag is dropped, so BE
treats malformed rows as an error and the query fails:

```
select * from json_table                    -> DATA_QUALITY_ERROR  (expected — no ignore flag)
select * from json_table_ignore_malformed   -> DATA_QUALITY_ERROR  (WRONG — flag=true dropped; expected: rows)
```

`test_hive_openx_json` `order_qt_q1` (`select * from json_table_ignore_malformed`) errors instead of
returning golden rows.

## Root Cause (HEAD-verified)

Legacy `HiveScanNode:646-647` set the BE Thrift attribute — inside the **`OPENX_JSON_SERDE` branch only**
(HiveScanNode:632 `else if serDeLib == OPENX_JSON_SERDE`), and only on the **non-one-column** sub-path
(`if (!isReadHiveJsonInOneColumn())`):
```java
fileAttributes.setOpenxJsonIgnoreMalformed(
    Boolean.parseBoolean(HiveProperties.getOpenxJsonIgnoreMalformed(table)));
```
`HiveProperties.getOpenxJsonIgnoreMalformed` reads serde/table property `ignore.malformed.json` via
`HiveMetaStoreClientHelper.getSerdeProperty` = `firstNonNullable(tableParam, sdParam)` (table-param
precedence), default `"false"`. The plain `HIVE_JSON_SERDE`/`LEGACY_HIVE_JSON_SERDE` branches never set it.

On the plugin path:
- The connector's `HiveTextProperties.extractJsonSerDeProps` (:162) receives only `serDeLib` — it emits
  `is_json` / `json_serde_lib` but **never reads `ignore.malformed.json`**, even though the call site
  (`extract`, :111) already holds `sdParams` and `tableParams`.
- fe-core `PluginDrivenScanNode.getFileAttributes` (:647-651) sets `readJsonByLine`/`readByColumnDef` from
  `hive.text.is_json` but **never calls `setOpenxJsonIgnoreMalformed`**.

So `TFileAttributes.openx_json_ignore_malformed` stays at its Thrift default `false` for every plugin hive
JSON table → malformed rows always error.

## Design

Mirror legacy exactly, using the existing `hive.text.*` property channel (the established connector→fe-core
scan-attribute transport; not a new mechanism).

### 1. `HiveTextProperties` (fe-connector-hive)

- New key constant `IGNORE_MALFORMED_JSON = "ignore.malformed.json"` and `DEFAULT_IGNORE_MALFORMED_JSON =
  "false"` (mirrors `HiveProperties.PROP_OPENX_IGNORE_MALFORMED_JSON` / default).
- Thread `sdParams` + `tableParams` into `extractJsonSerDeProps` (call site :111 already has them), and emit
  the flag **only for the OpenX serde** (legacy fidelity — the other JSON branches never set it):
  ```java
  private static void extractJsonSerDeProps(String serDeLib, Map<String,String> sdParams,
          Map<String,String> tableParams, Map<String,String> result) {
      result.put(PROP_PREFIX + "column_separator", "\t");
      result.put(PROP_PREFIX + "line_delimiter", "\n");
      result.put(PROP_PREFIX + "is_json", "true");
      result.put(PROP_PREFIX + "json_serde_lib", serDeLib);
      // OpenX-only: ignore.malformed.json (table-param over sd-param, default false) — mirrors legacy
      // HiveScanNode's OPENX_JSON_SERDE branch. hcatalog/hive2 JSON serdes never carried this flag.
      if (OPENX_JSON_SERDE.equals(serDeLib)) {
          String ignoreMalformed = serdeVal(sdParams, tableParams, IGNORE_MALFORMED_JSON);
          result.put(PROP_PREFIX + "openx_ignore_malformed",
                  ignoreMalformed != null ? ignoreMalformed : DEFAULT_IGNORE_MALFORMED_JSON);
      }
  }
  ```

**One-column mode (`read_hive_json_in_one_column=true`) is a non-issue.** For OpenX in one-column mode
`HiveFileFormat.detect` resolves the format to **CSV** (single-column line read), so BE uses the CSV reader,
which never consults `openx_json_ignore_malformed` (a JSON-reader-only field). Legacy likewise did not set it
on that sub-path. So even though the connector still emits the key (extract dispatches on serde), setting the
Thrift field has no BE effect in one-column mode — no explicit fe-core one-column gate needed. The failing
assertion (`order_qt_q1`) runs in the default (non-one-column) JSON mode.

### 2. `PluginDrivenScanNode.getFileAttributes` (fe-core)

Inside the existing `if ("true".equals(isJson))` block (:648-651), add the mirror of legacy :646-647:
```java
String ignoreMalformed = props.get(PROP_HIVE_TEXT_PREFIX + "openx_ignore_malformed");
if (ignoreMalformed != null) {
    attrs.setOpenxJsonIgnoreMalformed(Boolean.parseBoolean(ignoreMalformed));
}
```
Placed with the other JSON attrs, keyed off the connector-emitted `hive.text.*` prop — the same
connector-agnostic transport fe-core already uses for `serde_lib` / `is_json` / delimiters. No new
source-specific branch (it conforms to the pre-existing `hive.text.*` reader; iron-rule status unchanged).

## Risk Analysis

- **Blast radius**: `HiveTextProperties` (private method signature — only caller is `extract:111`, updated in
  the same edit), 1 new `PluginDrivenScanNode` read. No SPI/interface change.
- **`json_table` regression guard**: without `ignore.malformed.json`, the emitted value is `"false"` →
  `setOpenxJsonIgnoreMalformed(false)` → BE still raises DATA_QUALITY_ERROR on malformed rows (the test's
  first assertion stays satisfied). The fix flips behavior ONLY when the serde/table property is truthy.
- **Non-openx JSON serdes** (hcatalog / hive2): the key is not emitted at all (openx-only gate) → fe-core never
  calls the setter → Thrift default false → no behavior change (matches legacy, which set it only in the openx
  branch).
- **Boolean parse**: `Boolean.parseBoolean` — any non-"true" (case-insensitive) → false, exactly legacy.
- **fe-core iron rule**: reads an existing `hive.text.*`-prefixed key via the established transport; no new
  `if(hive)`/source-name branch. Conforms to the current `getFileAttributes` design.

## Test Plan

### Unit Tests (fe-connector-hive — `HiveTextPropertiesTest`)
1. `testOpenxJsonIgnoreMalformedTrueEmitted`: OpenX serde + sdParam `ignore.malformed.json=true` →
   result `hive.text.openx_ignore_malformed == "true"`, `is_json == "true"`.
2. `testOpenxJsonIgnoreMalformedDefaultsFalse`: OpenX serde, no property → `"false"`.
3. `testOpenxJsonIgnoreMalformedTableParamPrecedence`: table-param `true` beats sdParam `false` → `"true"`
   (mirrors serdeVal precedence, matching legacy getSerdeProperty).
4. `testHcatalogJsonSerdeOmitsOpenxKey`: hcatalog JSON serde → result has NO `openx_ignore_malformed` key
   (openx-only gate), but still `is_json == "true"`.

### E2E
`test_hive_openx_json` `order_qt_q1` returns golden rows; `json_table` still throws DATA_QUALITY_ERROR.
Live-gated (external hive docker); user reruns. No golden change (goldens already encode the correct rows).

fe-core plumbing (prop → `setOpenxJsonIgnoreMalformed`) is a 3-line mirror of the untested-in-isolation
`is_json` block (node not bare-constructable per test-infra notes); covered by e2e + the connector UT.

## Verification
- `mvn -o -f fe/pom.xml -pl :fe-connector-hive -am test` `-Dtest=HiveTextPropertiesTest`.
- `mvn -o -f fe/pom.xml -pl fe-core -am test-compile` (getFileAttributes change compiles).
- 0 checkstyle; import gate clean.

## Red-team result (wf_64f26946-e33, 3 lenses)
- **C1 legacy parity — caught + folded.** The reviewer refuted the *original* draft (which emitted the flag for
  all three JSON serdes) as UNSOUND: legacy sets `setOpenxJsonIgnoreMalformed` ONLY in the `OPENX_JSON_SERDE`
  branch (HiveScanNode:631-647), not hcatalog/hive2. **This design now gates emission on `OPENX_JSON_SERDE`
  only** (folded before implementation). The other four dimensions confirmed exact against HEAD: key
  `ignore.malformed.json` (HiveProperties:65), default `false` (:66), table-over-sd precedence
  (getSerdeProperty `firstNonNullable(tbl,sd)` == `serdeVal`), `Boolean.parseBoolean`→`setOpenxJsonIgnoreMalformed`
  (Thrift default false, PlanNodes.thrift:309; setter exists TFileAttributes:550). One-column `!isReadHiveJsonInOneColumn`
  gate: not replicated, but harmless — one-column openx resolves to CSV format so BE's CSV reader never reads the
  JSON-only field (see design note above).
- **C2 test/regression — SOUND.** All 5 `openx_json` tables use the OpenX serde; only `json_table_ignore_malformed`
  carries `ignore.malformed.json=true` (SERDEPROPERTIES → sdParams; `run76.hql:94`). So `serdeVal` emits `true`
  only for q1's table, `false` for the rest → they keep raising DATA_QUALITY_ERROR (explicit-false == current
  never-set, Thrift default false); `read_hive_json_in_one_column` order_qt_2/3 unaffected. Private
  `extractJsonSerDeProps` has exactly one caller (extract:111), public signature unchanged, no test asserts on the
  JSON prop-map. q1 golden (11 all-null + 2 data rows) is BE-rendered, live-gated.
- **C3 iron-rule/scope — SOUND.** `getFileAttributes` already reads ~10 source-named `hive.text.*` keys
  (serde_lib/is_json/delimiters/…); one more read is the identical established connector→fe-core channel, same
  posture as `is_json`. No new source-name branch; property resolution stays connector-side. Iron rule unchanged.
