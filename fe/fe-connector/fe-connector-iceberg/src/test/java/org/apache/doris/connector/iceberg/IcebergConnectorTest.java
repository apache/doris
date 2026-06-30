// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Connector-level tests for {@link IcebergConnector} that can run offline (no live catalog / no AWS call). The
 * live s3tables catalog construction (hand-built {@code S3TablesClient} + {@code S3TablesCatalog.initialize}) is
 * exercised only at the P6.6 docker plugin-zip gate; here we lock the FAIL-LOUD routing invariants that guard it.
 * No Mockito — the {@link RecordingConnectorContext} fail-loud fake is used.
 */
public class IcebergConnectorTest {

    @Test
    public void s3TablesWithoutStorageFailsLoud() {
        // WHY: legacy IcebergS3TablesMetaStoreProperties always derives from S3Properties.of(origProps); the
        // connector needs a bound S3-compatible storage to derive the region + credentials for the control-plane
        // S3TablesClient. Missing storage must fail loud (a clear DorisConnectorException), NOT silently route
        // s3tables through the generic CatalogUtil path or fall back to an anonymous client. MUTATION: dropping
        // the chosenS3 presence check -> a NullPointer / wrong-path error instead of this message -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("S3-compatible storage"),
                "expected a fail-loud message naming the missing S3-compatible storage, got: " + ex.getMessage());
    }

    @Test
    public void s3TablesWithoutRegionFailsLoud() {
        // WHY: Region.of("") would yield an invalid AWS region that only blows up deep in the SDK; the connector
        // must reject a region-less s3tables storage up front (legacy getRegion() is validated non-blank at
        // property-binding time). MUTATION: passing a blank region straight to Region.of -> a cryptic SDK error
        // instead of this message -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        List<StorageProperties> storages = Collections.singletonList(
                new FakeS3CompatibleStorageProperties("S3").accessKey("AK").secretKey("SK"));
        ctx.storageProperties = storages;
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("region"),
                "expected a fail-loud message naming the missing region, got: " + ex.getMessage());
    }

    @Test
    public void dlfWithoutStorageFailsLoud() {
        // WHY: legacy IcebergAliyunDLFMetaStoreProperties always selected an OSS StorageProperties; the connector
        // needs a bound OSS storage to back the DLFCatalog's S3FileIO. A missing one must fail loud BEFORE any
        // metastore call, NOT route dlf through the generic CatalogUtil path (which would ClassNotFound on the
        // dlf impl). MUTATION: dropping the chosenS3 presence check -> a different/cryptic error -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "dlf", "warehouse", "oss://b/wh"), ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("OSS storage"),
                "expected a fail-loud message naming the missing OSS storage, got: " + ex.getMessage());
    }

    @Test
    public void declaresMvccAndTimeTravelCapabilities() {
        // WHY: SUPPORTS_MVCC_SNAPSHOT is the gate PluginDrivenExternalDatabase checks to build the MVCC/MTMV
        // table subclass (so beginQuerySnapshot/resolveTimeTravel/applySnapshot fire); SUPPORTS_TIME_TRAVEL
        // mirrors paimon. MUTATION: leaving the default empty capability set -> iceberg tables build as plain
        // non-MVCC tables, time-travel silently reads latest -> red. (Inert pre-cutover; iceberg is not yet in
        // SPI_READY_TYPES, so getCapabilities does not touch the catalog and needs no live connection.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Set<ConnectorCapability> caps = connector.getCapabilities();
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT));
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_TIME_TRAVEL));
    }

    @Test
    public void declaresFullSchemaWriteOrderCapability() {
        // WHY (T06): legacy bindIcebergTableSink ALWAYS projects the write child to table.getFullSchema()
        // order (BindSink:797-803), regardless of the INSERT column list. The generic
        // bindConnectorTableSink reproduces that full-schema projection ONLY when the connector declares
        // SINK_REQUIRE_FULL_SCHEMA_ORDER (BindSink:892-905); otherwise it keeps user-column order, which
        // (a) writes values into the wrong remote columns and (b) makes the write-sort columnIndex
        // (a full-schema position) resolve against a misaligned sink output. MUTATION: omitting the
        // capability -> `INSERT INTO t (name, id) ...` orders/writes by the wrong column -> red.
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                .contains(ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER));
    }

    @Test
    public void declaresStaticPartitionMaterializationCapability() {
        // WHY: legacy bindIcebergTableSink re-projects each static PARTITION(col='v') literal into its data
        // column (BindSink:783-795); the iceberg BE writer keeps partition columns in the data file (does NOT
        // strip them). The generic bindConnectorTableSink reproduces that projection ONLY when the connector
        // declares SINK_MATERIALIZE_STATIC_PARTITION_VALUES; otherwise the static-partition column is
        // NULL-filled and iceberg's InclusiveMetricsEvaluator prunes the file on read-back (e.g.
        // `INSERT OVERWRITE ... PARTITION(par='a') SELECT ...` then `WHERE par='a'` reads empty). MaxCompute
        // must NOT declare it (it strips partition columns + refills from static_partition_values). MUTATION:
        // omitting the capability -> static-partition overwrite writes par=NULL -> read-back empty -> red.
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                .contains(ConnectorCapability.SINK_MATERIALIZE_STATIC_PARTITION_VALUES));
    }

    @Test
    public void declaresParallelWriteCapability() {
        // WHY (C3b ④b): legacy iceberg INSERT distributes via PhysicalIcebergTableSink, whose partition-hash
        // branch is DEAD (it reads getPartitionNames(), which IcebergExternalTable never overrides -> empty),
        // so at RUNTIME every iceberg INSERT (partitioned or not) returns SINK_RANDOM_PARTITIONED (parallel
        // writers). Post-cutover the generic PhysicalConnectorTableSink reproduces SINK_RANDOM_PARTITIONED
        // ONLY when the connector declares SUPPORTS_PARALLEL_WRITE (else it falls through to GATHER, a single
        // writer = a parallelism regression vs legacy). MUTATION: omitting the capability -> iceberg INSERT
        // degrades to GATHER post-flip -> red.
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                .contains(ConnectorCapability.SUPPORTS_PARALLEL_WRITE),
                "iceberg must declare SUPPORTS_PARALLEL_WRITE so post-flip INSERT keeps legacy random "
                        + "(parallel) distribution instead of degrading to GATHER");
    }

    @Test
    public void declaresColumnAutoAnalyzeAndTopNLazyMaterializeCapabilities() {
        // WHY: legacy IcebergExternalTable is in StatisticsUtil.supportAutoAnalyze's whitelist and is forced to
        // FULL analyze, and IcebergExternalTable.class is in MaterializeProbeVisitor's lazy-top-N supported set.
        // Post-cutover the generic fe-core gates reproduce both ONLY when the connector declares these
        // capabilities; omitting them silently regresses CBO stats quality (auto-analyze stalls) and Top-N
        // latency (lazy materialization skipped). MUTATION: dropping either capability -> the corresponding
        // fe-core gate excludes flipped iceberg -> red. (Inert pre-cutover; iceberg not yet in SPI_READY_TYPES.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Set<ConnectorCapability> caps = connector.getCapabilities();
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE),
                "iceberg must declare SUPPORTS_COLUMN_AUTO_ANALYZE so post-flip background auto-analyze keeps "
                        + "collecting per-column stats");
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE),
                "iceberg must declare SUPPORTS_TOPN_LAZY_MATERIALIZE so post-flip Top-N queries keep lazy "
                        + "materialization");
    }

    @Test
    public void declaresShowCreateDdlCapability() {
        // WHY: legacy IcebergExternalTable rendered LOCATION + PROPERTIES + PARTITION BY + ORDER BY in SHOW
        // CREATE TABLE, and IcebergExternalDatabase rendered LOCATION in SHOW CREATE DATABASE. Post-cutover the
        // generic plugin-driven render arm reproduces these ONLY when the connector declares
        // SUPPORTS_SHOW_CREATE_DDL (the capability also replaces the legacy paimon-only engine-name gate that
        // doubled as the JDBC/ES credential-leak guard). MUTATION: dropping the capability -> flipped iceberg
        // SHOW CREATE TABLE degrades to a comment-only shell -> red. (Inert pre-cutover.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                        .contains(ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL),
                "iceberg must declare SUPPORTS_SHOW_CREATE_DDL so post-flip SHOW CREATE TABLE/DATABASE keeps "
                        + "rendering LOCATION/PROPERTIES/PARTITION BY/ORDER BY");
    }

    @Test
    public void declaresViewCapability() {
        // WHY: legacy IcebergExternalTable resolves isView() from catalog.viewExists and IcebergExternalCatalog
        // merges listViewNames back into SHOW TABLES (its listTableNames subtracts views). Post-cutover the
        // generic plugin path reproduces both ONLY when the connector declares SUPPORTS_VIEW
        // (PluginDrivenExternalTable.isView() consults the connector, and listTableNamesFromRemote re-merges the
        // connector's listViewNames). MUTATION: dropping the capability -> flipped iceberg views vanish from
        // SHOW TABLES and report isView()==false (scanned as tables) -> red. (Inert pre-cutover.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                        .contains(ConnectorCapability.SUPPORTS_VIEW),
                "iceberg must declare SUPPORTS_VIEW so post-flip views stay visible/queryable/droppable");
    }

    @Test
    public void declaresNestedColumnPruneCapability() {
        // WHY: legacy IcebergExternalTable returns true from LogicalFileScan.supportPruneNestedColumn and the
        // SlotTypeReplacer rewrites the nested access path to iceberg field-ids. Post-cutover the generic
        // plugin-driven path reproduces both ONLY when the connector declares SUPPORTS_NESTED_COLUMN_PRUNE; it
        // is correct only because parseSchema/IcebergTypeMapping also carry the per-field ids the BE field-id
        // scan path matches nested leaves by. MUTATION: dropping the capability -> flipped iceberg stops pruning
        // STRUCT/ARRAY/MAP sub-fields (reads the whole complex column = read amplification) -> regression.
        // (Inert pre-cutover; iceberg not yet in SPI_READY_TYPES.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                        .contains(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE),
                "iceberg must declare SUPPORTS_NESTED_COLUMN_PRUNE so post-flip nested sub-field queries keep "
                        + "reading only the accessed leaves");
    }

    // ------------------------------------------------------------------------------------------------------
    // H-2: REST 3-level namespace (external_catalog.name) must reach scan/write/procedure, not just metadata.
    //
    // Legacy IcebergMetadataOps was a SINGLE per-catalog ops carrying external_catalog.name, so ALL of
    // metadata/scan/write/procedure resolved tables under [<db>, <cat>]. The SPI split built the three
    // provider getters with the 1-arg CatalogBackedIcebergCatalogOps (external_catalog.name dropped), so
    // post-flip SELECT/INSERT/EXECUTE on a 3-level REST catalog resolved the WRONG namespace ([<db>] only).
    // Each provider resolves its table exclusively via catalogOps.loadTable, so we assert that loadTable on
    // the ops the connector hands each provider resolves the 3-level table. MUTATION: revert any one provider
    // to the 1-arg ctor -> that ops resolves [mydb].t (missing) -> NoSuchTableException -> red.
    // ------------------------------------------------------------------------------------------------------

    private static final Schema H2_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

    /**
     * Build a connector whose lazily-created catalog is replaced (reflection) with an offline in-memory
     * catalog holding {@code [mydb, cat].t}, and configured with {@code external_catalog.name=cat}.
     */
    private static IcebergConnector connectorOver3LevelCatalog() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("mydb"));
        catalog.createNamespace(Namespace.of("mydb", "cat"));
        catalog.createTable(TableIdentifier.of(Namespace.of("mydb", "cat"), "t"), H2_SCHEMA);
        return connectorWithCatalog(
                Map.of("iceberg.catalog.type", "rest", "external_catalog.name", "cat"), catalog);
    }

    private static IcebergConnector connectorWithCatalog(Map<String, String> props, Catalog catalog)
            throws Exception {
        IcebergConnector connector = new IcebergConnector(props, new RecordingConnectorContext());
        Field f = IcebergConnector.class.getDeclaredField("icebergCatalog");
        f.setAccessible(true);
        f.set(connector, catalog);
        return connector;
    }

    /** Reflect out the private {@code catalogOps} the connector built each provider with. */
    private static IcebergCatalogOps catalogOpsOf(Object provider) throws Exception {
        Field f = provider.getClass().getDeclaredField("catalogOps");
        f.setAccessible(true);
        return (IcebergCatalogOps) f.get(provider);
    }

    @Test
    public void scanProviderThreadsExternalCatalogNameInto3LevelNamespace() throws Exception {
        IcebergCatalogOps ops = catalogOpsOf(connectorOver3LevelCatalog().getScanPlanProvider());
        Assertions.assertDoesNotThrow(() -> ops.loadTable("mydb", "t"),
                "scan provider must build ops that thread external_catalog.name so the REST 3-level namespace "
                        + "[mydb, cat] resolves; the 1-arg ops drops it and loadTable hits [mydb].t -> NoSuchTable");
    }

    @Test
    public void writeProviderThreadsExternalCatalogNameInto3LevelNamespace() throws Exception {
        IcebergCatalogOps ops = catalogOpsOf(connectorOver3LevelCatalog().getWritePlanProvider());
        Assertions.assertDoesNotThrow(() -> ops.loadTable("mydb", "t"),
                "write provider must thread external_catalog.name so INSERT/DELETE/MERGE resolve [mydb, cat].t");
    }

    @Test
    public void procedureProviderThreadsExternalCatalogNameInto3LevelNamespace() throws Exception {
        IcebergCatalogOps ops = catalogOpsOf(connectorOver3LevelCatalog().getProcedureOps());
        Assertions.assertDoesNotThrow(() -> ops.loadTable("mydb", "t"),
                "procedure provider must thread external_catalog.name so ALTER TABLE ... EXECUTE resolves "
                        + "[mydb, cat].t");
    }

    @Test
    public void scanProviderResolvesTwoLevelNamespaceWithoutExternalCatalogName() throws Exception {
        // Sanity / reverse-mutation guard: without external_catalog.name the 2-level namespace [mydb] must
        // still resolve (no spurious extra level appended). MUTATION: unconditionally appending a level -> red.
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("mydb"));
        catalog.createTable(TableIdentifier.of(Namespace.of("mydb"), "t"), H2_SCHEMA);
        IcebergConnector connector = connectorWithCatalog(Map.of("iceberg.catalog.type", "rest"), catalog);
        IcebergCatalogOps ops = catalogOpsOf(connector.getScanPlanProvider());
        Assertions.assertDoesNotThrow(() -> ops.loadTable("mydb", "t"),
                "without external_catalog.name a plain 2-level namespace [mydb] must resolve unchanged");
    }
}
