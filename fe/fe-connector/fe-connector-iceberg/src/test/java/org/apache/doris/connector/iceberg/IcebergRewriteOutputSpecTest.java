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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.thrift.TIcebergTableSink;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Exercises procedure planning, transaction handoff, sink metadata and the committed file spec together. */
public class IcebergRewriteOutputSpecTest {
    private static final Map<String, String> CATALOG_PROPERTIES =
            Collections.singletonMap("iceberg.catalog.type", "hadoop");
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "rewrite_spec");
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db", "rewrite_spec");
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "event_time", Types.TimestampType.withoutZone()));

    @Test
    public void historicalDaySpecReachesEveryGroupAndCommittedFile() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.builderFor(SCHEMA).identity("id").build());
            table.updateSpec().removeField("id").addField(Expressions.day("event_time")).commit();
            int daySpecId = table.spec().specId();
            append(table, "old1", "event_time_day=2026-11-01");
            table.updateSpec().removeField("event_time_day").addField(Expressions.month("event_time")).commit();
            int currentSpecId = table.spec().specId();
            int month = (2026 - 1970) * 12 + 10;
            append(table, "old2", "event_time_month=" + month);

            Rewrite rewrite = plan(table, Integer.toString(daySpecId));
            Assertions.assertEquals(2, rewrite.groups.size());
            Assertions.assertNotSame(rewrite.outerSession.getStatementScope(),
                    rewrite.groupSession.getStatementScope());
            for (ConnectorRewriteGroup group : rewrite.groups) {
                Assertions.assertEquals(1, group.getDataFilePaths().size());
                TIcebergTableSink sink = rewrite.sink(new Session(rewrite.transaction));
                Assertions.assertEquals(daySpecId, sink.getPartitionSpecId());
                PartitionSpec output = PartitionSpecParser.fromJson(table.schema(),
                        sink.getPartitionSpecsJson().get(daySpecId));
                Assertions.assertEquals("day", output.fields().get(0).transform().toString());
            }
            rewrite.registerSources();
            Set<Integer> expectedDays = new HashSet<>();
            for (String date : Arrays.asList("2026-11-01", "2026-11-02")) {
                int day = Math.toIntExact(LocalDate.parse(date).toEpochDay());
                expectedDays.add(day);
                rewrite.report(date, Collections.singletonList(date));
            }
            rewrite.transaction.commit();

            Table committed = catalog.loadTable(TABLE_ID);
            Assertions.assertEquals(currentSpecId, committed.spec().specId());
            Set<Integer> actualDays = new HashSet<>();
            for (DataFile file : files(committed)) {
                Assertions.assertEquals(daySpecId, file.specId());
                actualDays.add(file.partition().get(0, Integer.class));
            }
            Assertions.assertEquals(expectedDays, actualDays);
            Assertions.assertEquals(2, rewrite.transaction.getRewriteAddedDataFilesCount());
        }
    }

    @Test
    public void explicitZeroUsesTheHistoricalUnpartitionedSpec() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.unpartitioned());
            table.updateSpec().addField("id").commit();
            int currentSpecId = table.spec().specId();
            append(table, "old", "id=1");

            Rewrite rewrite = plan(table, "0");
            TIcebergTableSink sink = rewrite.sink();
            Assertions.assertEquals(0, sink.getPartitionSpecId());
            Assertions.assertTrue(sink.getPartitionSpecsJson().containsKey(0));
            rewrite.registerSources();
            rewrite.report("unpartitioned", Collections.emptyList());
            rewrite.transaction.commit();

            Table committed = catalog.loadTable(TABLE_ID);
            Assertions.assertEquals(currentSpecId, committed.spec().specId());
            List<DataFile> files = files(committed);
            Assertions.assertEquals(1, files.size());
            Assertions.assertEquals(0, files.get(0).specId());
            Assertions.assertEquals(0, files.get(0).partition().size());
        }
    }

    @Test
    public void nonzeroUnpartitionedSpecIsExplicitOnTheSink() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.builderFor(SCHEMA).identity("id").build());
            table.updateSpec().removeField("id").commit();
            int unpartitionedId = table.spec().specId();
            Assertions.assertTrue(unpartitionedId > 0);
            table.updateSpec().addField(Expressions.day("event_time")).commit();
            append(table, "old", "event_time_day=2026-11-01");

            Rewrite rewrite = plan(table, Integer.toString(unpartitionedId));
            TIcebergTableSink sink = rewrite.sink();
            Assertions.assertEquals(unpartitionedId, sink.getPartitionSpecId());
            Assertions.assertTrue(sink.getPartitionSpecsJson().containsKey(unpartitionedId));
            rewrite.registerSources();
            rewrite.report("unpartitioned", Collections.emptyList());
            rewrite.transaction.commit();
            Assertions.assertEquals(unpartitionedId, files(catalog.loadTable(TABLE_ID)).get(0).specId());
        }
    }

    @Test
    public void omittedSpecStaysPinnedWhenTheDefaultChanges() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.unpartitioned());
            table.updateSpec().addField("id").commit();
            int plannedSpecId = table.spec().specId();
            append(table, "old", "id=1");
            Rewrite rewrite = plan(table, null);
            table.updateSpec().removeField("id").commit();

            Assertions.assertNotEquals(plannedSpecId, table.spec().specId());
            Assertions.assertEquals(plannedSpecId, rewrite.sink().getPartitionSpecId());
            rewrite.registerSources();
            rewrite.report("id", Collections.singletonList("1"));
            rewrite.transaction.commit();
            Assertions.assertEquals(plannedSpecId, files(catalog.loadTable(TABLE_ID)).get(0).specId());
        }
    }

    @Test
    public void unknownSpecFailsOnAnEmptyTable() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.unpartitioned());
            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                    () -> plan(table, "9999"));
            Assertions.assertTrue(failure.getMessage().contains("Unknown output partition spec ID: 9999"));
            Assertions.assertNull(table.currentSnapshot());
        }
    }

    @Test
    public void historicalSpecWithADroppedSourceFailsBeforeWriting() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.builderFor(SCHEMA).identity("id").build());
            table.updateSpec().removeField("id").commit();
            table.updateSchema().deleteColumn("id").commit();
            Assertions.assertTrue(table.specs().containsKey(0));
            Assertions.assertThrows(DorisConnectorException.class, () -> plan(table, "0"));
            Assertions.assertNull(table.currentSnapshot());
        }
    }

    @Test
    public void schemaEvolutionAfterPlanningFailsBeforeReturningASink() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.unpartitioned());
            append(table, "old", null);
            Rewrite rewrite = plan(table, null);
            table.updateSchema().addColumn("new_column", Types.IntegerType.get()).commit();
            Assertions.assertThrows(DorisConnectorException.class, rewrite::sink);
            Assertions.assertTrue(rewrite.transaction.getCommitDataList().isEmpty());
        }
    }

    @Test
    public void sortEvolutionAfterPlanningFailsBeforeReturningASink() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            Table table = createTable(catalog, PartitionSpec.unpartitioned());
            table.replaceSortOrder().asc("id").commit();
            append(table, "old", null);
            Rewrite rewrite = plan(table, null);
            table.replaceSortOrder().desc("id").commit();

            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, rewrite::sink);
            Assertions.assertTrue(failure.getMessage().contains("sort order changed during rewrite planning"));
            Assertions.assertTrue(rewrite.transaction.getCommitDataList().isEmpty());
        }
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static Table createTable(InMemoryCatalog catalog, PartitionSpec spec) {
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.data.path", "s3://bucket/rewrite/data");
        return catalog.createTable(TABLE_ID, SCHEMA, spec, properties);
    }

    private static void append(Table table, String name, String partitionPath) {
        DataFiles.Builder file = DataFiles.builder(table.spec()).withPath("s3://bucket/rewrite/data/" + name)
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(100).withRecordCount(1);
        if (partitionPath != null) {
            file.withPartitionPath(partitionPath);
        }
        table.newAppend().appendFile(file.build()).commit();
    }

    private static List<DataFile> files(Table table) throws Exception {
        List<DataFile> result = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                result.add(task.file());
            }
        }
        return result;
    }

    private static Rewrite plan(Table table, String outputSpecId) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        RecordingConnectorContext context = new RecordingConnectorContext();
        Session outer = new Session(null);
        Map<String, String> options = new HashMap<>();
        options.put("rewrite-all", "true");
        options.put("max-file-group-size-bytes", "100");
        if (outputSpecId != null) {
            options.put("output-spec-id", outputSpecId);
        }
        List<ConnectorRewriteGroup> groups = new IcebergProcedureOps(CATALOG_PROPERTIES, ops, context)
                .planRewrite(outer, HANDLE, "rewrite_data_files", options, null, Collections.emptyList());
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(
                ops, IcebergCatalogProperties.of(CATALOG_PROPERTIES), context);
        IcebergConnectorTransaction transaction =
                (IcebergConnectorTransaction) metadata.beginTransaction(outer, HANDLE);
        return new Rewrite(groups, outer, new Session(transaction), transaction,
                new IcebergWritePlanProvider(IcebergCatalogProperties.of(CATALOG_PROPERTIES), ops, context));
    }

    private static final class Rewrite {
        private final List<ConnectorRewriteGroup> groups;
        private final Session outerSession;
        private final Session groupSession;
        private final IcebergConnectorTransaction transaction;
        private final IcebergWritePlanProvider provider;

        private Rewrite(List<ConnectorRewriteGroup> groups, Session outerSession, Session groupSession,
                IcebergConnectorTransaction transaction, IcebergWritePlanProvider provider) {
            this.groups = groups;
            this.outerSession = outerSession;
            this.groupSession = groupSession;
            this.transaction = transaction;
            this.provider = provider;
        }

        private TIcebergTableSink sink() {
            return sink(groupSession);
        }

        private TIcebergTableSink sink(Session session) {
            return provider.planWrite(session, new WriteHandle()).getDataSink().getIcebergTableSink();
        }

        private void registerSources() {
            Set<String> sources = new HashSet<>();
            groups.forEach(group -> sources.addAll(group.getDataFilePaths()));
            transaction.registerRewriteSourceFiles(sources);
        }

        private void report(String name, List<String> partitions) throws Exception {
            TIcebergCommitData data = new TIcebergCommitData();
            data.setFilePath("s3://bucket/rewrite/data/new-" + name + ".parquet");
            data.setFileContent(TFileContent.DATA);
            data.setRowCount(1);
            data.setFileSize(100);
            data.setPartitionValues(partitions);
            transaction.addCommitData(new TSerializer(new TBinaryProtocol.Factory()).serialize(data));
        }
    }

    private static final class WriteHandle implements ConnectorWriteHandle {
        @Override
        public ConnectorTableHandle getTableHandle() {
            return HANDLE;
        }

        @Override
        public List<ConnectorColumn> getColumns() {
            return Collections.emptyList();
        }

        @Override
        public boolean isOverwrite() {
            return false;
        }

        @Override
        public Map<String, String> getStaticPartitionSpec() {
            return Collections.emptyMap();
        }

        @Override
        public WriteOperation getWriteOperation() {
            return WriteOperation.REWRITE;
        }
    }

    private static final class Session implements ConnectorSession {
        private final ConnectorTransaction transaction;
        private final ConnectorStatementScope scope = new TestStatementScope();

        private Session(ConnectorTransaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public String getQueryId() {
            return "rewrite-query";
        }

        @Override
        public String getUser() {
            return "test";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 1;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return CATALOG_PROPERTIES;
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return scope;
        }

        @Override
        public Optional<ConnectorTransaction> getCurrentTransaction() {
            return Optional.ofNullable(transaction);
        }

        @Override
        public long allocateTransactionId() {
            return 42;
        }
    }
}
