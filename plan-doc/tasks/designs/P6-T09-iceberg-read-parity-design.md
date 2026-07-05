# P6-T09 — Iceberg read-path parity (column construction + format-version + nested-namespace + view filtering)

> Branch `catalog-spi-10-iceberg`. Depends on T03/T08 (DONE). Iceberg stays OUT of `SPI_READY_TYPES` (flip is P6.6).
> Scope = the four "silent read-path" gaps the skeleton left (HANDOFF 🔴 #3/#4). Surgical; mirror paimon; byte-faithful to legacy.

## Legacy parity contract (code-grounded)

### G1 — format-version (`IcebergConnectorMetadata.getTableSchema`)
- **Current (wrong):** `iceberg.format-version = spec().specId() >= 0 ? 2 : 1` → ALWAYS "2" (unpartitioned specId==0 is also `>=0`).
- **Legacy:** `IcebergUtils.getFormatVersion(Table)` (`IcebergUtils.java:1791`):
  ```java
  int formatVersion = 2; // default 2
  if (table instanceof BaseTable) {
      formatVersion = ((BaseTable) table).operations().current().formatVersion();
  } else if (table != null && table.properties() != null) {
      String version = table.properties().get(TableProperties.FORMAT_VERSION);   // "format-version"
      if (version != null) { try { formatVersion = Integer.parseInt(version); } catch (NumberFormatException ignored) {} }
  }
  return formatVersion;
  ```
- **Fix:** port `getFormatVersion` verbatim; stamp `iceberg.format-version = String.valueOf(getFormatVersion(table))`.
- Imports: `org.apache.iceberg.BaseTable`, `org.apache.iceberg.TableProperties`.

### G2 — column construction (`IcebergConnectorMetadata.parseSchema`)
- **Legacy** (`IcebergUtils.parseSchema` ~1116): `new Column(field.name().toLowerCase(Locale.ROOT), type, isKey=true, null, isAllowNull=true, field.doc(), visible=true, -1)` then `setUniqueId(field.fieldId())` and, when source is TIMESTAMP-with-zone, `setWithTZExtraInfo()`.
- Confirmed `ConnectorColumnConverter.convertColumn` re-applies isKey / nullable / comment / `withTimeZone()→setWithTZExtraInfo()` onto the final `Column`. So the connector must set them on `ConnectorColumn`.
- **Four fixes** (mirror `PaimonConnectorMetadata:1141-1155`):
  1. **name** → `field.name().toLowerCase(java.util.Locale.ROOT)` (was verbatim). Legacy unconditionally lowercases; the SPI bridge's `fromRemoteColumnName` only layers *user* identifier-mapping on top, so the connector must lowercase itself.
  2. **isKey** → `true` (6-arg ctor; was 5-arg default `false`).
  3. **nullable** → `true` always (was `field.isOptional()`). Legacy forces `isAllowNull=true` regardless of Iceberg required/optional.
  4. **withTimeZone marker** → `.withTimeZone()` when `field.type().isPrimitiveType() && field.type().typeId()==TIMESTAMP && ((Types.TimestampType) field.type()).shouldAdjustToUTC()`. INDEPENDENT of the `enable.mapping.timestamp_tz` flag (mark from SOURCE type root).
- **comment** unchanged (`field.doc()!=null?field.doc():""` — Column normalizes blank→null, already parity).
- **field-id / uniqueId is OUT of T09 scope** — `ConnectorColumn` has no uniqueId carrier; DESCRIBE doesn't show it; field-id re-injection for the scan path is P6.2+ (HANDOFF 🔴 cross-cutting).

### G3 — listing recursion + G4 — view filtering (`CatalogBackedIcebergCatalogOps`, INTERNAL to the seam)
Legacy `IcebergMetadataOps`:
- `getNamespace(dbName)` = split `dbName` on `.` (omitEmpty/trim) then append `externalCatalogName` if present. Root `getNamespace()` = `externalCatalogName.map(Namespace::of).orElse(Namespace.empty())`.
- `listDatabaseNames()` = `listNestedNamespaces(root)`:
  - If REST flavor **and** `iceberg.rest.nested-namespace-enabled` (default false): recurse — `flatMap(child -> concat(child.toString(), recurse(child)))` (dotted names).
  - Else: `n.level(n.length()-1)` (last level).
- `listTableNames(dbName)` = `catalog.listTables(ns)` names, minus `((ViewCatalog) catalog).listViews(ns)` names when `isViewCatalogEnabled()`.
  - `isViewCatalogEnabled()` = `catalog instanceof ViewCatalog && (REST ? iceberg.rest.view-enabled(default true) : true)`.
- `databaseExists`/`tableExists` use the same `getNamespace(dbName)` split (was single-level `Namespace.of(dbName)`).

**Connector gating** (no fe-core classes available): derive from `properties` + flavor at `getMetadata` time and thread into `CatalogBackedIcebergCatalogOps`:
- `restFlavor = TYPE_REST.equals(flavor)`
- `nestedNamespaceEnabled = bool(properties["iceberg.rest.nested-namespace-enabled"], false)`
- `viewEnabled = bool(properties["iceberg.rest.view-enabled"], true)`
- `externalCatalogName = Optional.ofNullable(properties["external_catalog.name"])`

Seam interface UNCHANGED (`listDatabaseNames()`, `listTableNames(db)`, `databaseExists`, `tableExists`); only the default impl's internals get richer + 4 config fields. Keeps the `SupportsNamespaces`/`ViewCatalog` branch INTERNAL (per seam javadoc).

## Test plan (no Mockito; fail-loud fakes)
- `IcebergConnectorMetadataTest` (existing `FakeIcebergTable` + `RecordingIcebergCatalogOps`):
  - flip `getTableSchemaParsesColumnsFromLoadedTable`: required field now nullable=true + isKey=true.
  - rework `getTableSchemaStampsFormatVersionTwoForAnyValidSpec` → format-version read from `format-version` prop (present→that value; absent→"2").
  - new: lowercases name; withTimeZone marker set for TIMESTAMP-with-zone (flag on AND off), NOT set for without-zone / non-timestamp.
- new `FakeIcebergCatalog` (impl `Catalog, SupportsNamespaces, ViewCatalog`; fail-loud stubs) + `CatalogBackedIcebergCatalogOpsTest`:
  - non-nested last-level; nested-recursion dotted (REST+flag); view subtraction; view-filter off when not ViewCatalog / viewEnabled=false; dotted-namespace + externalCatalogName threaded into `listTables`/`namespaceExists`/`tableExists`.

### G5 — auth-wrapping (added to T09 per user decision; surfaced by adversarial review)
- **Legacy** `IcebergMetadataOps` wraps EVERY remote read in `executionAuthenticator.execute(...)`: `tableExist:146`, `databaseExist:154`, `listDatabaseNames:162`, `listTableNames:197` (`catch RuntimeException → rethrow; catch Exception → wrap`), `loadTable:1236`. The paimon mirror `PaimonConnectorMetadata` wraps the equivalent 5 reads via `context.executeAuthenticated(...)` (holds a `ConnectorContext` field). The skeleton wrapped NONE (no `ConnectorContext` field).
- **Materiality**: `ConnectorContext.executeAuthenticated` default is pass-through, so simple-auth catalogs are unaffected; only a Kerberized HMS/REST iceberg catalog (post-cutover) would run reads outside the FE-injected `UGI.doAs` → metastore auth failure. UT-invisible.
- **Fix** (in `IcebergConnectorMetadata`, mirroring paimon): ctor takes `ConnectorContext context` (threaded from `IcebergConnector.getMetadata`); each of the 5 reads wraps its seam call in `context.executeAuthenticated(...)` with legacy iceberg's exact per-method exception handling (warn+rethrow for listDatabaseNames; rethrow for db/tableExists+loadTable; `catch RuntimeException → rethrow` then wrap for listTableNames — iceberg `NoSuch*` are unchecked so `UGI.doAs` does not wrap them, unlike paimon's checked exceptions which it catches inside the lambda). The seam (`CatalogBackedIcebergCatalogOps`) stays auth-agnostic.
- **Test**: `RecordingConnectorContext` (`authCount` + `failAuth`) — assert 5 wraps per read sweep, and that `failAuth` (throws before invoking task) leaves `ops.log` empty (seam call sits INSIDE the wrap).

### G2.1 — namespace split byte-exactness (refined after adversarial review)
- The plain-Java `split + trim` first used diverged from legacy's Guava `Splitter.on('.').omitEmptyStrings().trimResults()` on Unicode whitespace above U+0020 (e.g. U+3000), which `String.trim()` keeps but Guava's `CharMatcher.whitespace()` strips. (NBSP U+00A0 is NOT a divergence — Guava's whitespace() excludes it.) **Fix**: use the SAME Guava `Splitter` legacy uses → byte-exact, simpler. Guava 33.2.1 is a compile-scope dep (bundled via hadoop).

## Acceptance gate
UT green (assert concrete column flags / prop values / recorded namespaces / authCount, not class names) + checkstyle 0 + `tools/check-connector-imports.sh` clean + iceberg NOT in `SPI_READY_TYPES`.

## Surfaced (verified — NOT a T09 bug)
- **`iceberg.format-version`/`iceberg.partition-spec` synthetic keys** have no fe-core reader and are NOT legacy SHOW-CREATE-TABLE parity. Adversarial verify CONFIRMED they cannot leak: `Env.java:4936-4939` gates the PluginDriven LOCATION+PROPERTIES rendering on `PAIMON_EXTERNAL_TABLE`, and iceberg's `getEngineTableTypeName()` returns `PLUGIN_EXTERNAL_TABLE` → the block is skipped. A future iceberg SHOW-CREATE-rendering branch (P6.6) would own these keys; nothing to do in T09.
- **field-id / uniqueId** still dropped (`ConnectorColumn` has no carrier); not surfaced by DESCRIBE; scan-path re-injection is P6.2+.
