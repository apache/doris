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
import org.apache.doris.connector.api.ConnectorContractValidator;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.WriteOperation;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
    public void declaresMvccSnapshotCapability() {
        // WHY: SUPPORTS_MVCC_SNAPSHOT is the gate PluginDrivenExternalDatabase checks to build the MVCC/MTMV
        // table subclass (so beginQuerySnapshot/resolveTimeTravel/applySnapshot fire). MUTATION: leaving the
        // default empty capability set -> iceberg tables build as plain non-MVCC tables, time-travel silently
        // reads latest -> red. (Inert pre-cutover; iceberg is not yet in SPI_READY_TYPES, so getCapabilities
        // does not touch the catalog and needs no live connection.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Set<ConnectorCapability> caps = connector.getCapabilities();
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT));
    }

    @Test
    public void declaresMetadataPreloadCapability() {
        // WHY (F11): legacy IcebergExternalTable.supportsExternalMetadataPreload returns true so the planner
        // async pre-warms schema/snapshot before taking the read lock. Post-cutover PluginDrivenExternalTable
        // reproduces this ONLY when the connector declares SUPPORTS_METADATA_PRELOAD (replacing the legacy
        // engine-name "jdbc" gate). MUTATION: dropping the capability -> flipped iceberg degrades to synchronous
        // bind-time metadata load (longer lock hold on slow metastores) -> red. (Inert pre-cutover; iceberg not
        // yet in SPI_READY_TYPES, so getCapabilities does not touch the catalog.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(
                connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_METADATA_PRELOAD),
                "iceberg must declare SUPPORTS_METADATA_PRELOAD so post-flip async metadata pre-load survives");
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

    /**
     * Reflect out the per-request ops the connector built each provider with. Since P6.6-C6 each provider holds a
     * {@code Function<ConnectorSession, IcebergCatalogOps>} resolver (not a bare ops); these catalogs are NOT
     * {@code iceberg.rest.session=user}, so the resolver ignores the session and yields the shared session-less
     * ops (the same object the pre-resolver provider held) — apply it with a null session to obtain it.
     */
    private static IcebergCatalogOps catalogOpsOf(Object provider) throws Exception {
        Field f = provider.getClass().getDeclaredField("catalogOpsResolver");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        Function<ConnectorSession, IcebergCatalogOps> resolver =
                (Function<ConnectorSession, IcebergCatalogOps>) f.get(provider);
        return resolver.apply(null);
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

    // ------------------------------------------------------------------------------------------------------
    // Task 6 (write-capability unification, P2): the per-connector expected-set assertion (the pragmatic
    // "declaration == implementation" check for the write-capability invariant the removed
    // ConnectorContractValidator#1 runtime probe is NOT safe to make) plus the structural contract validator,
    // exercised against a real IcebergConnector (not just IcebergWritePlanProvider in isolation, which
    // declaresFullWriteOperationSet in IcebergWritePlanProviderTest already pins) so this also proves
    // Connector's null-safe write delegators route the provider's declarations through unchanged. The catalog
    // is injected offline (reflection, same seam as the H-2 tests above) so getWritePlanProvider() never
    // attempts a live catalog connection.
    // ------------------------------------------------------------------------------------------------------

    @Test
    public void declaredWriteCapabilitiesMatchAndPassContractValidator() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        IcebergConnector connector = connectorWithCatalog(Collections.emptyMap(), catalog);

        Assertions.assertEquals(
                EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE, WriteOperation.DELETE,
                        WriteOperation.MERGE, WriteOperation.REWRITE),
                connector.supportedWriteOperations());
        Assertions.assertTrue(connector.supportsWriteBranch());
        Assertions.assertTrue(connector.requiresParallelWrite());
        Assertions.assertFalse(connector.requiresPartitionLocalSort(),
                "iceberg does NOT require partition-local sort (unlike MaxCompute)");
        Assertions.assertTrue(connector.requiresFullSchemaWriteOrder());
        Assertions.assertTrue(connector.requiresMaterializeStaticPartitionValues());

        ConnectorContractValidator.validate(connector, "iceberg");
    }
}
