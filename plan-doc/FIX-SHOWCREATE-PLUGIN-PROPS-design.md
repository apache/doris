# FIX-SHOWCREATE-PLUGIN-PROPS — design

## Problem
`test_nereids_refresh_catalog` fails: for a **JDBC** external-catalog table, `SHOW CREATE TABLE` now renders
```
ENGINE=JDBC_EXTERNAL_TABLE
LOCATION ''
PROPERTIES (
  "password" = "...", "driver_class" = "...", "driver_url" = "...", "jdbc_url" = "...", "type" = "jdbc", ...
)
```
but the committed expected output is just `ENGINE=JDBC_EXTERNAL_TABLE;` (no LOCATION, no PROPERTIES). Two problems:
a correctness regression (output diff) **and** a credential leak (the JDBC `password` is now printed by SHOW CREATE TABLE).

## Root Cause
JDBC/ES/Trino/MaxCompute/Paimon catalogs are all plugin-driven on this branch, so their tables have
`TableType.PLUGIN_EXTERNAL_TABLE` and render through the single shared branch in `Env.getDdlStmt`
(`fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java:4929-4959`).

Branch commit `98a73bf7692` ([P5-B7 paimon cutover], D-046 paimon parity) added LOCATION+PROPERTIES emission to that
shared branch, gated **only** on `!properties.isEmpty()`:
```java
Map<String, String> properties = pluginExternalTable.getTableProperties();
if (!properties.isEmpty()) {
    sb.append("\nLOCATION '").append(properties.getOrDefault("path", "")).append("'");
    sb.append("\nPROPERTIES ( ... )");
}
```
The intent was: connectors that surface table properties (paimon coreOptions: path/file.format) render LOCATION+
PROPERTIES; connectors that don't (MaxCompute) return an empty map and stay comment-only. But JDBC/ES/Trino tables
return **non-empty** `getTableProperties()` (connection props, incl. credentials), so they wrongly get the paimon
treatment. At merge-base `11a038d` this branch was `addTableComment(table, sb)` only — i.e. legacy JDBC/ES/Trino
SHOW CREATE TABLE was comment-only (`ENGINE=...;`). The first `getDdlStmt` overload (`Env.java:4509-4511`) is still
comment-only; only the second overload regressed. Not fixed by any af2037..HEAD commit.

## Design
Restore legacy behavior by **scoping the LOCATION+PROPERTIES emission to the paimon engine type** — the only
plugin-driven connector that legacy rendered LOCATION/PROPERTIES — instead of "any plugin table with non-empty props".
`PluginDrivenExternalTable.getEngineTableTypeName()` already returns the per-catalog-type engine name
(`PAIMON_EXTERNAL_TABLE` for paimon; `JDBC/ES/TRINO_CONNECTOR/MAX_COMPUTE_EXTERNAL_TABLE` otherwise). Gate on it:
```java
boolean rendersLocation = TableType.PAIMON_EXTERNAL_TABLE.name().equals(pluginExternalTable.getEngineTableTypeName());
if (rendersLocation && !properties.isEmpty()) { ... LOCATION/PROPERTIES ... }
```
This is single-site, deterministic, restores exact legacy `ENGINE=...;` for JDBC/ES/Trino/MaxCompute, and fixes the
credential leak (their props are never printed). When hive/iceberg/hudi later migrate to plugin-driven and need
LOCATION, the gate is the obvious extension point.

Rejected alternative: rebaseline `test_nereids_refresh_catalog.out` — it would bake leaked JDBC credentials into the
committed expected output and entrench the regression. Rejected alternative: make each non-file connector's
`getTableProperties()` return empty — touches multiple connector modules; the render-side gate is more surgical and
removes the leak regardless of connector hygiene.

## Implementation Plan
- `fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java` (second `getDdlStmt`, ~4946): add the engine-type gate
  before the `if (!properties.isEmpty())` block, with a comment referencing D-046 + this fix.

## Risk Analysis
- Paimon SHOW CREATE TABLE is unchanged (engine type == paimon still renders).
- MaxCompute already comment-only (empty props) — unchanged.
- JDBC/ES/Trino revert to legacy comment-only — matches the committed `.out` and pre-`98a73bf7692` behavior.
- No SPI/connector change; no BE change.

## Test Plan
### Unit Tests
- If an `Env.getDdlStmt`/SHOW CREATE FE unit test exists for plugin tables, assert JDBC renders `ENGINE=JDBC_EXTERNAL_TABLE;`
  with no LOCATION/PROPERTIES and paimon still renders LOCATION/PROPERTIES. (Otherwise covered by e2e below.)
### E2E Tests
- `external_table_p0/nereids_commands/test_nereids_refresh_catalog` (existing) must pass unchanged against its
  committed `.out` (CI external pipeline). Paimon SHOW CREATE TABLE suites (e.g. `test_paimon_catalog`) continue to
  render LOCATION/PROPERTIES — covered once FIX-PAIMON-HADOOP-CLASSLOADER unblocks them.
