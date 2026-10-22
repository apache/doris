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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.ColumnPath;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties.DelegatedTokenMode;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises the public DROP entry points against real committed Iceberg metadata. */
public class IcebergColumnDropTest {
    private InMemoryCatalog catalog;
    private IcebergMetadataOps ops;
    private RESTSessionCatalog restCatalog;
    private ExecutionAuthenticator authenticator = new ExecutionAuthenticator() { };

    @Before
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class,
                Mockito.withSettings().extraInterfaces(IcebergUserSessionCatalog.class));
        IcebergUserSessionCatalog sessionCatalog = (IcebergUserSessionCatalog) dorisCatalog;
        restCatalog = Mockito.mock(RESTSessionCatalog.class);
        Mockito.when(sessionCatalog.getRestSessionCatalog()).thenReturn(restCatalog);
        Mockito.when(sessionCatalog.getDelegatedTokenMode()).thenReturn(DelegatedTokenMode.ACCESS_TOKEN);
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.doReturn(Optional.empty()).when(dorisCatalog).getDbForReplay(Mockito.anyString());
        ops = new IcebergMetadataOps(dorisCatalog, catalog);
    }

    @After
    public void tearDown() throws Exception {
        catalog.close();
    }

    private Table createTable(String name, int format, boolean nested) {
        Schema schema = nested ? new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "info", Types.StructType.of(
                        Types.NestedField.optional(3, "metric", Types.IntegerType.get()),
                        Types.NestedField.optional(4, "other", Types.IntegerType.get()))))
                : new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "metric", Types.IntegerType.get()),
                        Types.NestedField.optional(3, "other", Types.IntegerType.get()));
        return catalog.buildTable(TableIdentifier.of("db", name), schema)
                .withPartitionSpec(PartitionSpec.builderFor(schema).identity(nested ? "info.metric" : "metric").build())
                .withProperty(TableProperties.FORMAT_VERSION, Integer.toString(format)).create();
    }

    private TableMetadata metadata(String name) {
        return ((HasTableOperations) catalog.loadTable(TableIdentifier.of("db", name))).operations().current();
    }

    private void drop(Table table, ColumnPath path) throws UserException {
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(dorisTable.getRemoteName()).thenReturn(table.name().substring(table.name().lastIndexOf('.') + 1));
        TableOperations tableOperations = ((HasTableOperations) table).operations();
        if (tableOperations instanceof RestTableOperations) {
            Mockito.doAnswer(invocation -> {
                List<TableCommit> commits = invocation.getArgument(1);
                Assert.assertEquals(1, commits.size());
                ((RestTableOperations) tableOperations).commitTransaction(commits.get(0));
                return null;
            }).when(restCatalog).commitTransaction(Mockito.any(), Mockito.anyList());
        }
        IcebergExternalMetaCache.WritableTableLease lease =
                Mockito.mock(IcebergExternalMetaCache.WritableTableLease.class);
        Mockito.when(lease.getTable()).thenReturn(table);
        Mockito.when(lease.getAuthenticator()).thenReturn(authenticator);
        try (MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            utils.when(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable, ops)).thenReturn(lease);
            try {
                if (path.isNested()) {
                    ops.dropColumn(dorisTable, path, 1L);
                } else {
                    ops.dropColumn(dorisTable, path.getTopLevelName(), 1L);
                }
            } finally {
                Mockito.verify(lease).close();
            }
        }
    }

    private void assertProtected(String name, Table table, ColumnPath path) {
        String before = metadata(name).metadataFileLocation();
        Assert.assertNotNull(before);
        UserException error = Assert.assertThrows(UserException.class, () -> drop(table, path));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("partition spec"));
        Assert.assertEquals(before, metadata(name).metadataFileLocation());
    }

    @Test
    public void testCurrentPartitionSource() {
        Table table = createTable("current_source", 2, false);
        assertProtected("current_source", table, ColumnPath.of("metric"));
    }

    @Test
    public void testHistoricalSourceWithoutSnapshotsV2() {
        assertFileFreeHistoricalSource(2);
    }

    @Test
    public void testHistoricalSourceWithoutSnapshotsV3() {
        assertFileFreeHistoricalSource(3);
    }

    private void assertFileFreeHistoricalSource(int format) {
        Table table = createTable("empty", format, false);
        table.updateSpec().removeField("metric").commit();
        Assert.assertNull(table.currentSnapshot());
        assertProtected("empty", table, ColumnPath.of("metric"));
    }

    @Test
    public void testHistoricalSourceWithFile() {
        Table table = createTable("file", 2, false);
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath("memory:/file/data.parquet")
                .withFileSizeInBytes(1).withRecordCount(1).withPartitionPath("metric=5").build()).commit();
        table.updateSpec().removeField("metric").commit();
        assertProtected("file", table, ColumnPath.of("metric"));
    }

    @Test
    public void testRenamedHistoricalSource() {
        Table table = createTable("renamed", 2, false);
        int sourceId = table.schema().findField("metric").fieldId();
        table.updateSpec().removeField("metric").commit();
        table.updateSchema().renameColumn("metric", "renamed_metric").commit();
        Assert.assertEquals(sourceId, table.schema().findField("renamed_metric").fieldId());
        assertProtected("renamed", table, ColumnPath.of("RENAMED_METRIC"));
    }

    @Test
    public void testNestedHistoricalSource() {
        Table table = createTable("nested", 2, true);
        table.updateSpec().removeField(table.spec().fields().get(0).name()).commit();
        assertProtected("nested", table, ColumnPath.fromDotName("INFO.METRIC"));
    }

    @Test
    public void testParentOfHistoricalSource() {
        Table table = createTable("parent", 2, true);
        table.updateSpec().removeField(table.spec().fields().get(0).name()).commit();
        assertProtected("parent", table, ColumnPath.of("info"));
    }

    @Test
    public void testFormatV1VoidSource() {
        Table table = createTable("void_source", 1, false);
        table.updateSpec().removeField("metric").commit();
        Assert.assertTrue(table.spec().fields().get(0).transform().isVoid());
        assertProtected("void_source", table, ColumnPath.of("metric"));
    }

    @Test
    public void testUnreferencedColumnRemainsDroppable() throws Exception {
        Table table = createTable("ordinary", 2, false);
        table.updateSpec().removeField("metric").commit();
        drop(table, ColumnPath.of("other"));
        Assert.assertNull(metadata("ordinary").schema().findField("other"));
        Assert.assertNotNull(metadata("ordinary").schema().findField("metric"));
        Assert.assertEquals(2, metadata("ordinary").specs().size());
    }

    @Test
    public void testRestDropRebasesWithoutChangingPartitionFieldsAndCleansWriterProperties() throws Exception {
        Table table = createTable("rest_rebase", 2, false);
        String property = TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "other";
        int historicalOtherId = table.schema().findField("other").fieldId();
        table.updateProperties().set(property, "full")
                .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(MappingUtil.create(table.schema())))
                .commit();
        RestTableOperations rest = new RestTableOperations(((HasTableOperations) table).operations(), null);
        drop(new BaseTable(rest, table.name()), ColumnPath.of("other"));
        TableMetadata after = metadata("rest_rebase");
        Assert.assertNull(after.schema().findField("other"));
        Assert.assertFalse(after.properties().containsKey(property));
        Assert.assertEquals(Integer.valueOf(historicalOtherId), NameMappingParser.fromJson(
                after.properties().get(TableProperties.DEFAULT_NAME_MAPPING)).find("other").id());
        Assert.assertEquals(1, rest.commitAttempts);
        Assert.assertTrue(rest.requirements.stream()
                .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
        Assert.assertFalse(rest.clientChanges.stream().anyMatch(MetadataUpdate.RemovePartitionSpecs.class::isInstance));
        Assert.assertEquals(2, after.specs().size());
        Assert.assertEquals(table.spec().fields().get(0).fieldId(), after.lastAssignedPartitionId());
        Assert.assertEquals(0, after.defaultSpecId());
        Assert.assertFalse(rest.clientChanges.stream().anyMatch(MetadataUpdate.AddPartitionSpec.class::isInstance));
    }

    @Test
    public void testRestDropRejectsConcurrentPartitionSource() {
        Table table = createTable("rest_race", 2, false);
        RestTableOperations rest = new RestTableOperations(((HasTableOperations) table).operations(), "other");
        UserException error = Assert.assertThrows(UserException.class,
                () -> drop(new BaseTable(rest, table.name()), ColumnPath.of("other")));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("partition"));
        Assert.assertEquals(1, rest.commitAttempts);
        TableMetadata after = metadata("rest_race");
        int sourceId = after.schema().findField("other").fieldId();
        Assert.assertTrue(after.specs().stream().flatMap(spec -> spec.fields().stream())
                .anyMatch(field -> field.sourceId() == sourceId));
    }

    @Test
    public void testDirectDropRejectsConcurrentPartitionSource() {
        Table table = createTable("direct_race", 2, false);
        DirectRacingTableOperations racing = new DirectRacingTableOperations(((HasTableOperations) table).operations());
        Assert.assertThrows(UserException.class,
                () -> drop(new BaseTable(racing, table.name()), ColumnPath.of("other")));
        Assert.assertEquals(1, racing.commitAttempts);
        Assert.assertNotNull(metadata("direct_race").schema().findField("other"));
        Assert.assertEquals(2, metadata("direct_race").specs().size());
    }

    @Test
    public void testRestFormatV1UnreferencedDrop() throws Exception {
        Table table = createTable("rest_v1", 1, false);
        RestTableOperations rest = new RestTableOperations(((HasTableOperations) table).operations(), null);
        int partitionId = metadata("rest_v1").lastAssignedPartitionId();
        drop(new BaseTable(rest, table.name()), ColumnPath.of("other"));
        Assert.assertNull(metadata("rest_v1").schema().findField("other"));
        Assert.assertNotNull(metadata("rest_v1").schema().findField("metric"));
        Assert.assertEquals(partitionId, metadata("rest_v1").lastAssignedPartitionId());
        Assert.assertEquals(1, rest.commitAttempts);
    }

    @Test
    public void testDropRejectsRecreatedTableDuringRefresh() {
        Table original = createTable("original", 2, true);
        createTable("replacement", 2, true);
        RefreshingTableOperations refreshing = new RefreshingTableOperations(
                ((HasTableOperations) original).operations(),
                ((HasTableOperations) original).operations().current(), metadata("replacement"));
        assertConcurrentTargetChange(original, refreshing, ColumnPath.fromDotName("info.other"));
    }

    @Test
    public void testDropRejectsSamePathFieldReplacement() {
        Table table = createTable("field_replaced", 2, false);
        TableOperations delegate = ((HasTableOperations) table).operations();
        TableMetadata loaded = delegate.current();
        table.updateSchema().deleteColumn("other").commit();
        table.updateSchema().addColumn("other", Types.IntegerType.get()).commit();
        RefreshingTableOperations refreshing = new RefreshingTableOperations(delegate, loaded, delegate.refresh());
        assertConcurrentTargetChange(table, refreshing, ColumnPath.of("other"));
    }

    @Test
    public void testDropParentRejectsDescendantReplacement() {
        Table table = createTable("subtree_replaced", 2, true);
        TableOperations delegate = ((HasTableOperations) table).operations();
        TableMetadata loaded = delegate.current();
        table.updateSchema().deleteColumn("info.other").commit();
        table.updateSchema().addColumn("info", "other", Types.IntegerType.get()).commit();
        RefreshingTableOperations refreshing = new RefreshingTableOperations(delegate, loaded, delegate.refresh());
        assertConcurrentTargetChange(table, refreshing, ColumnPath.of("info"));
    }

    private void assertConcurrentTargetChange(Table original, RefreshingTableOperations refreshing, ColumnPath path) {
        UserException error = Assert.assertThrows(UserException.class,
                () -> drop(new BaseTable(refreshing, original.name()), path));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("changed concurrently"));
        Assert.assertEquals(0, refreshing.commitAttempts);
    }

    @Test
    public void testDropNestedRowLineageNameAndWriterProperties() throws Exception {
        Table table = createTable("nested_lineage", 3, true);
        table.updateSchema().renameColumn("info.other", "_row_id").commit();
        String metrics = TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "info._row_id";
        String bloom = TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "info._row_id";
        String stats = TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + "info._row_id";
        table.updateProperties().set(metrics, "full").set(bloom, "true").set(stats, "true").commit();
        drop(table, ColumnPath.fromDotName("info._row_id"));
        TableMetadata after = metadata("nested_lineage");
        Assert.assertNull(after.schema().findField("info._row_id"));
        Assert.assertNotNull(after.schema().findField("info.metric"));
        Assert.assertFalse(after.properties().containsKey(metrics));
        Assert.assertFalse(after.properties().containsKey(bloom));
        Assert.assertFalse(after.properties().containsKey(stats));
    }

    @Test
    public void testRefreshAndCommitUseLeaseAuthenticator() throws Exception {
        Table table = createTable("authenticated", 2, false);
        AtomicBoolean authenticated = new AtomicBoolean();
        authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                Assert.assertFalse(authenticated.getAndSet(true));
                try {
                    return task.call();
                } finally {
                    authenticated.set(false);
                }
            }
        };
        DelegatingTableOperations scoped = new DelegatingTableOperations(((HasTableOperations) table).operations()) {
            @Override
            public TableMetadata current() {
                Assert.assertTrue(authenticated.get());
                return super.current();
            }

            @Override
            public TableMetadata refresh() {
                Assert.assertTrue(authenticated.get());
                return super.refresh();
            }

            @Override
            public void commit(TableMetadata base, TableMetadata next) {
                Assert.assertTrue(authenticated.get());
                super.commit(base, next);
            }
        };
        drop(new BaseTable(scoped, table.name()), ColumnPath.of("other"));
        Assert.assertFalse(authenticated.get());
        Assert.assertNull(metadata("authenticated").schema().findField("other"));
    }

    @Test
    public void testDropPinsValidatedBaseAfterRefreshReturns() {
        Table table = createTable("post_refresh_replacement", 2, false);
        int originalId = table.schema().findField("other").fieldId();
        DelegatingTableOperations racing = new DelegatingTableOperations(((HasTableOperations) table).operations()) {
            @Override
            public TableMetadata refresh() {
                TableMetadata checked = delegate.refresh();
                // A concurrent writer changes the shared handle after refresh fetched its immutable result.
                Table writer = new BaseTable(delegate, "replacement_writer");
                writer.updateSchema().deleteColumn("other").commit();
                writer.updateSchema().addColumn("other", Types.IntegerType.get()).commit();
                return checked;
            }
        };
        Assert.assertThrows(UserException.class,
                () -> drop(new BaseTable(racing, table.name()), ColumnPath.of("other")));
        Assert.assertNotNull(metadata("post_refresh_replacement").schema().findField("other"));
        Assert.assertNotEquals(originalId,
                metadata("post_refresh_replacement").schema().findField("other").fieldId());
    }

    @Test
    public void testOnlyCurrentV1VoidSpecProtectsSource() {
        Table table = createTable("current_void", 1, false);
        int oldSpecId = table.spec().specId();
        table.updateSpec().removeField("metric").commit();
        TableOperations delegate = ((HasTableOperations) table).operations();
        TableMetadata before = delegate.current();
        TableMetadata.Builder cleaned = TableMetadata.buildFrom(before);
        new MetadataUpdate.RemovePartitionSpecs(Collections.singleton(oldSpecId)).applyTo(cleaned);
        delegate.commit(before, cleaned.build());
        Assert.assertEquals(1, metadata("current_void").specs().size());
        assertProtected("current_void", table, ColumnPath.of("metric"));
    }

    @Test
    public void testDirectV1DropDoesNotAllocatePartitionFieldId() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "other", Types.IntegerType.get()));
        Table table = catalog.buildTable(TableIdentifier.of("db", "v1_first_partition"), schema)
                .withProperty(TableProperties.FORMAT_VERSION, "1").create();
        drop(table, ColumnPath.of("other"));
        table.updateSpec().addField("id").commit();
        Assert.assertEquals(1000, table.spec().fields().get(0).fieldId());
    }

    @Test
    public void testRestDropWithOnlyCollectionRemaining() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "values", Types.ListType.ofOptional(3, Types.IntegerType.get())));
        Table table = catalog.buildTable(TableIdentifier.of("db", "collection_remaining"), schema)
                .withProperty(TableProperties.FORMAT_VERSION, "2").create();
        RestTableOperations rest = new RestTableOperations(((HasTableOperations) table).operations(), null);
        drop(new BaseTable(rest, table.name()), ColumnPath.of("id"));
        Assert.assertNull(metadata("collection_remaining").schema().findField("id"));
        Assert.assertNotNull(metadata("collection_remaining").schema().findField("values"));
        Assert.assertTrue(metadata("collection_remaining").specs().stream().allMatch(spec -> spec.fields().isEmpty()));
        Assert.assertEquals(1, rest.commitAttempts);
    }

    @Test
    public void testRestDropDoesNotCreateInternalPartitionFields() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "_doris_schema_drop_fence_1000", Types.LongType.get()),
                Types.NestedField.optional(2, "other", Types.IntegerType.get()));
        Table table = catalog.buildTable(TableIdentifier.of("db", "fence_collision"), schema).create();
        RestTableOperations rest = new RestTableOperations(((HasTableOperations) table).operations(), null);
        drop(new BaseTable(rest, table.name()), ColumnPath.of("other"));
        Assert.assertNotNull(metadata("fence_collision").schema().findField("_doris_schema_drop_fence_1000"));
        Assert.assertTrue(metadata("fence_collision").specs().stream().allMatch(spec -> spec.fields().isEmpty()));
    }

    @Test
    public void testRestConsecutiveOrdinaryDrops() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "a", Types.LongType.get()),
                Types.NestedField.optional(3, "b", Types.LongType.get()));
        Table table = catalog.buildTable(TableIdentifier.of("db", "consecutive_drops"), schema)
                .withProperty(TableProperties.FORMAT_VERSION, "2").create();
        drop(new BaseTable(new RestTableOperations(((HasTableOperations) table).operations(), null), table.name()),
                ColumnPath.of("b"));
        table = catalog.loadTable(TableIdentifier.of("db", "consecutive_drops"));
        drop(new BaseTable(new RestTableOperations(((HasTableOperations) table).operations(), null), table.name()),
                ColumnPath.of("id"));
        TableMetadata after = metadata("consecutive_drops");
        Assert.assertNull(after.schema().findField("id"));
        Assert.assertNull(after.schema().findField("b"));
        Assert.assertNotNull(after.schema().findField("a"));
        Assert.assertEquals(0, after.defaultSpecId());
    }

    @Test
    public void testUnconfiguredRestCommitFailsBeforeWriting() {
        Table table = createTable("unsupported_rest", 2, false);
        ExternalCatalog unconfigured = Mockito.mock(ExternalCatalog.class);
        Mockito.when(unconfigured.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(unconfigured.getProperties()).thenReturn(Collections.emptyMap());
        ops = new IcebergMetadataOps(unconfigured, catalog);
        RestTableOperations rest = new RestTableOperations(((HasTableOperations) table).operations(), null);
        String before = metadata("unsupported_rest").metadataFileLocation();
        UserException error = Assert.assertThrows(UserException.class,
                () -> drop(new BaseTable(rest, table.name()), ColumnPath.of("other")));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("retained REST session catalog"));
        Assert.assertEquals(0, rest.commitAttempts);
        Assert.assertEquals(before, metadata("unsupported_rest").metadataFileLocation());
    }

    @Test
    public void testDropRevalidatesRowLineageAfterFormatUpgrade() {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "_row_id", Types.LongType.get()));
        Table table = catalog.buildTable(TableIdentifier.of("db", "format_upgrade"), schema)
                .withProperty(TableProperties.FORMAT_VERSION, "2").create();
        AtomicInteger commits = new AtomicInteger();
        DelegatingTableOperations upgrading = new DelegatingTableOperations(((HasTableOperations) table).operations()) {
            @Override
            public TableMetadata refresh() {
                new BaseTable(delegate, "upgrading_writer").updateProperties()
                        .set(TableProperties.FORMAT_VERSION, "3").commit();
                return delegate.refresh();
            }

            @Override
            public void commit(TableMetadata base, TableMetadata next) {
                commits.incrementAndGet();
                super.commit(base, next);
            }
        };
        UserException error = Assert.assertThrows(UserException.class,
                () -> drop(new BaseTable(upgrading, table.name()), ColumnPath.of("_row_id")));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("row lineage"));
        Assert.assertEquals(0, commits.get());
        Assert.assertEquals(3, metadata("format_upgrade").formatVersion());
        Assert.assertNotNull(metadata("format_upgrade").schema().findField("_row_id"));
    }

    private static class DelegatingTableOperations implements TableOperations {
        protected final TableOperations delegate;

        private DelegatingTableOperations(TableOperations delegate) {
            this.delegate = delegate;
        }

        @Override
        public TableMetadata current() {
            return delegate.current();
        }

        @Override
        public TableMetadata refresh() {
            return delegate.refresh();
        }

        @Override
        public void commit(TableMetadata base, TableMetadata next) {
            delegate.commit(base, next);
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption() {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String name) {
            return delegate.metadataFileLocation(name);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }
    }

    /** Emulates REST requirement validation and server-side spec-ID rebasing. */
    private static final class RestTableOperations extends DelegatingTableOperations {
        private final String sourceColumn;
        private boolean injectConcurrentSpec = true;
        private int commitAttempts;
        private List<UpdateRequirement> requirements = Collections.emptyList();
        private List<MetadataUpdate> clientChanges = Collections.emptyList();

        private RestTableOperations(TableOperations delegate, String sourceColumn) {
            super(delegate);
            this.sourceColumn = sourceColumn;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata next) {
            throw new AssertionError("REST DROP must use the transaction endpoint");
        }

        private void commitTransaction(TableCommit commit) {
            commitAttempts++;
            Assert.assertTrue("DROP must submit once", injectConcurrentSpec);
            injectConcurrentSpec = false;
            TableMetadata base = delegate.current();
            UpdateTableRequest request = UpdateTableRequest.create(
                    commit.identifier(), commit.requirements(), commit.updates());
            request = UpdateTableRequestParser.fromJson(UpdateTableRequestParser.toJson(request));
            clientChanges = request.updates();
            requirements = request.requirements();
            int specId = base.specs().stream().mapToInt(PartitionSpec::specId).max().orElse(-1) + 1;
            if (sourceColumn == null) {
                PartitionSpec concurrentSpec = PartitionSpec.builderFor(base.schema()).withSpecId(specId).build();
                delegate.commit(base, TableMetadata.buildFrom(base).addPartitionSpec(concurrentSpec).build());
            } else {
                // Let the real SDK allocate a fresh partition field ID; reusing 1000 from a builder
                // would not model a legal concurrent addition when the table is already partitioned.
                new BaseTable(delegate, "concurrent_spec").updateSpec().addField(sourceColumn)
                        .addNonDefaultSpec().commit();
            }
            TableMetadata rebased = delegate.refresh();
            for (UpdateRequirement requirement : requirements) {
                requirement.validate(rebased);
            }
            TableMetadata.Builder update = TableMetadata.buildFrom(rebased);
            clientChanges.forEach(change -> change.applyTo(update));
            delegate.commit(rebased, update.build());
        }
    }

    private static final class DirectRacingTableOperations extends DelegatingTableOperations {
        private int commitAttempts;

        private DirectRacingTableOperations(TableOperations delegate) {
            super(delegate);
        }

        @Override
        public void commit(TableMetadata base, TableMetadata next) {
            commitAttempts++;
            new BaseTable(delegate, "concurrent").updateSpec().addField("other").commit();
            delegate.commit(base, next);
        }
    }

    private static final class RefreshingTableOperations extends DelegatingTableOperations {
        private final TableMetadata loaded;
        private final TableMetadata refreshed;
        private boolean didRefresh;
        private int commitAttempts;

        private RefreshingTableOperations(TableOperations delegate, TableMetadata loaded, TableMetadata refreshed) {
            super(delegate);
            this.loaded = loaded;
            this.refreshed = refreshed;
        }

        @Override
        public TableMetadata current() {
            return didRefresh ? refreshed : loaded;
        }

        @Override
        public TableMetadata refresh() {
            didRefresh = true;
            return refreshed;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata next) {
            commitAttempts++;
            Assert.fail("Changed drop target must never commit");
        }
    }
}
