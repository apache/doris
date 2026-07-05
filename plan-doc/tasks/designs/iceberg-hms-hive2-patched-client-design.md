# Design: iceberg-HMS Hive-1/2 compat — vendor Doris patched HiveMetaStoreClient into the plugin

## Problem (proven, live)
iceberg-HMS on a Hive 1/2 metastore returns EMPTY metadata (list/get databases) →
`test_iceberg_show_create` fails ("Unknown database" / "Namespace already exists").

Root cause = **regression from the SPI migration**:
- iceberg `HiveCatalog.getAllDatabases()/getDatabase()` → `MetaStoreUtils.prependCatalogToDbName` →
  unconditionally prepends the Hive-3 catalog marker `@hive#`.
- A Hive-2 metastore doesn't understand that marker → `get_databases("@hive#")` = [] ,
  `get_database("@hive#!db")` = NoSuchObject.
- Doris ships a PATCHED `org.apache.hadoop.hive.metastore.HiveMetaStoreClient` (fe-core, and a copy in
  be-java-extensions) that reads `hive.version` (default **2.3**) and uses
  `prependCatalogToDbNameByVersion` → for Hive 1/2/2.3 returns the raw name WITHOUT the marker.
- LEGACY iceberg ran in fe-core on the **app** classloader, where the patched client shadows the shaded
  jar → hive2 worked. The NEW plugin connector runs on a **child-first plugin** classloader that loads the
  UNPATCHED `hive-catalog-shade` client → hive2 broken.

Live proof: iceberg-HMS@hive2 FAIL / iceberg-HMS@hive3 OK / native-HMS@hive2 OK. `hive.version` property
does NOT help the shaded client (it ignores it).

## Goal / Non-goals
- GOAL: restore legacy parity — iceberg-HMS works on Hive 1/2 (and keeps working on Hive 3 + REST).
- NON-GOAL: fixing the other plugin HMS connectors (hive/hms/paimon-hms) that share the latent bug — they
  can copy the same file later, or a shared module can consolidate. Out of scope for this test fix.

## Approach (user-chosen: copy patched client into the plugin)
Copy Doris's patched client into `fe-connector-iceberg` so the plugin's child-first classloader loads OUR
copy, shadowing the shaded jar's. Mirrors Doris's existing "one copy per isolated classloader context"
pattern (fe-core + be-java-extensions each carry one).

### Why it works (all verified against code)
- Plugin CL is child-first except `org.apache.doris.connector.*` / `org.apache.doris.filesystem.*`
  (ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES). `org.apache.hadoop.hive.metastore.*` and
  `org.apache.doris.datasource.hive.*` are child-first → plugin jars preferred.
- DirectoryPluginRuntimeManager sorts lib jars alphabetically by path (`Path::toString`, line ~321).
  `doris-fe-connector-iceberg.jar` < `hive-catalog-shade-*.jar` ('d' < 'h') → our jar is searched FIRST →
  our `HiveMetaStoreClient.class` shadows the shaded jar's.
- iceberg `HiveClientPool` does `new HiveMetaStoreClient(conf)` → transparently resolves to our patched copy.
- fe-core's patched client already imports the RELOCATED thrift `shade.doris.hive.org.apache.thrift.*`
  (same `hive-catalog-shade` artifact the connector uses) → compiles cleanly, no thrift surgery.
- Default path (no `hive.version`, as in the test): `HiveVersionUtil.getVersion(null)=V2_3` →
  `prependCatalogToDbNameByVersion` returns raw name (no marker) → works on Hive 1/2/2.3 AND Hive 3
  (unmarked == default catalog). REST path uses RESTCatalog (no HiveMetaStoreClient) → unaffected.

## Files
Placed in the SHARED `fe-connector-hms` module (user request) so every HMS-backed connector plugin reuses
them. fe-connector-hms already has `hive-catalog-shade` (compiles the client) and is already a dependency of
fe-connector-hive; its jar `fe-connector-hms-<version>.jar` sorts before `hive-catalog-shade-*.jar` ('f'<'h')
in each consumer plugin's lib/, so the shadowing holds.
1. NEW `fe-connector-hms/.../org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java`
   - Verbatim copy of fe-core's (3621 lines). ONLY deviation: drop the heavy fe-core
     `HMSBaseProperties` import; inline `private static final String HIVE_VERSION = "hive.version";`
     (the client used HMSBaseProperties solely for that constant, 2 call sites).
   - checkstyle: already globally suppressed by `suppress files="HiveMetaStoreClient\.java"` (line 63).
2. NEW `fe-connector-hms/.../org/apache/doris/datasource/hive/HiveVersionUtil.java`
   - Verbatim copy (84 lines; self-contained: guava Strings + log4j).
3. EDIT `fe-connector-iceberg/pom.xml` — add dependency on fe-connector-hms.
4. EDIT `IcebergCatalogOps.java` — remove the temporary `[db-visibility]` diagnostics (root cause found).

## Side effect (intended, per reuse request)
fe-connector-hms's own `ThriftHmsClient` (and thus fe-connector-hive, which depends on fe-connector-hms)
will also resolve `org.apache.hadoop.hive.metastore.HiveMetaStoreClient` to this patched copy on their next
build — fixing their latent Hive-1/2 bug too. Strictly safer (it is Doris's native, battle-tested client),
but not runtime-verified in this task.

## Risks
- Alphabetical jar-order dependency (d < h). Deterministic + documented; revisit if finalName/shade renamed.
- 3rd maintenance copy of the vendored client (Doris already keeps 2). Note at file top.
- Must confirm every non-shaded import of the client resolves on the connector classpath (audit before build).

## TODO
1. [DONE] Remove `[db-visibility]` logging from IcebergCatalogOps (createDatabase + listDatabaseNames). Net-zero (file back to original).
2. [DONE] Audit all imports of fe-core's patched client → only 3 doris imports (HiveVersionUtil, its enum, HMSBaseProperties). Rest guava/hadoop/log4j/JDK, all on connector classpath.
3. [DONE] Add vendored `HiveMetaStoreClient.java` (inline HIVE_VERSION; only deviation from fe-core copy).
4. [DONE] Add `HiveVersionUtil.java` (verbatim + header note).
5. [DONE] Compile fe-connector-iceberg (-am) = BUILD SUCCESS; checkstyle (no -am) = 0 violations.
6. [DONE] Connector unit tests: 359 run, 0 fail (CatalogBackedIcebergCatalogOps*, IcebergConnectorMetadata*, IcebergScanRange/PlanProvider).
7. [DONE] Packaging verified: my classes compile into doris-fe-connector-iceberg.jar, which sorts before hive-catalog-shade-*.jar in plugin lib/ (alphabetical, d<h) → shadows the shaded client.
8. [PENDING USER] redeploy + rerun test_iceberg_show_create (hive2) + spot-check hive3 still lists.

## Runtime verification (after redeploy)
- `test_iceberg_show_create` (hive2) should pass.
- Spot-check via mysql (127.0.0.1:9033): create an iceberg-hms catalog on hive2 (thrift://172.20.32.136:9083) → `show databases` should list all real DBs (not just information_schema/mysql). Same on hive3 (9383) must still work.
- NOTE: a leftover `test_db1` from the previous failed run still exists in the hive2 metastore; once listing works, the test's `drop database if exists test_db1 force` will clean it. (If needed, drop manually.)
