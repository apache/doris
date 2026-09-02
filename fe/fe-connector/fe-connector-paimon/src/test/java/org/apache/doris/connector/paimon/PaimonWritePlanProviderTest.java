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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonWritePlanProviderTest {

    private static final Identifier TABLE = Identifier.create("db", "t");

    @TempDir
    Path warehouse;

    @Test
    public void keepsWritesOnSingleWriterDistribution() throws Exception {
        Assertions.assertFalse(fixture().provider.requiresParallelWrite());
    }

    @Test
    public void plansJniAppendSinkThroughConnectorSpi() throws Exception {
        Fixture fixture = fixture();
        List<ConnectorColumn> columns = fixture.provider.getWriteColumns(
                fixture.session, fixture.tableHandle, Optional.empty()).get();

        ConnectorSinkPlan plan = fixture.provider.planWrite(
                fixture.session, new WriteHandle(fixture.tableHandle, columns));
        TPaimonTableSink sink = plan.getDataSink().getPaimonTableSink();

        Assertions.assertEquals(TDataSinkType.PAIMON_TABLE_SINK, plan.getDataSink().getType());
        Assertions.assertEquals(TPaimonWriteMode.APPEND, sink.getWriteMode());
        Assertions.assertEquals(201L, sink.getTransactionId());
        Assertions.assertEquals(fixture.transaction.getCommitUser(), sink.getCommitUser());
        Assertions.assertEquals(java.util.Arrays.asList("id", "pt"), sink.getColumnNames());
        Assertions.assertFalse(sink.getSerializedTable().isEmpty());
    }

    @Test
    public void plansOverwriteAndCanonicalizesStaticPartitionName() throws Exception {
        Fixture fixture = fixture();
        List<ConnectorColumn> columns = fixture.provider.getWriteColumns(
                fixture.session, fixture.tableHandle, Optional.empty()).get();
        Map<String, String> partition = new LinkedHashMap<>();
        partition.put("PT", "2026-08-14");

        ConnectorSinkPlan plan = fixture.provider.planWrite(
                fixture.session, new WriteHandle(fixture.tableHandle, columns)
                        .overwrite(true).staticPartition(partition));

        Assertions.assertEquals(
                TPaimonWriteMode.OVERWRITE,
                plan.getDataSink().getPaimonTableSink().getWriteMode());
    }

    // Regression for the 2026-08-20 production crash: a static-partition INSERT OVERWRITE dropped one
    // BE with DORIS_CHECK_EQ(column_names.size(), block.columns()). BindSink materializes the
    // PARTITION-clause literal into the row (PaimonWritePlanProvider#requiresMaterializeStaticPartitionValues
    // returns true), so the projected block the writer actually receives carries the FULL bound schema —
    // but planWrite was still sizing/populating column_names off handle.getColumns(), which BindSink's
    // selectConnectorSinkBindColumns deliberately narrows to a subset that EXCLUDES the static-partition
    // column (see ConnectorWriteHandle#getColumns javadoc). This pins the two lists to their real-world
    // shape (getColumns() sans "pt", getBoundTargetColumns() with it) and asserts column_names lines up
    // with the full bound schema, not the narrowed INSERT list.
    @Test
    public void staticPartitionOverwriteColumnNamesMatchTheFullBoundSchemaNotTheInsertList() throws Exception {
        Fixture fixture = fixture();
        List<ConnectorColumn> fullSchema = fixture.provider.getWriteColumns(
                fixture.session, fixture.tableHandle, Optional.empty()).get();
        List<ConnectorColumn> insertColumnsSansPartition = fullSchema.stream()
                .filter(column -> !"pt".equalsIgnoreCase(column.getName()))
                .collect(java.util.stream.Collectors.toList());
        Map<String, String> partition = new LinkedHashMap<>();
        partition.put("pt", "2026-08-14");

        ConnectorSinkPlan plan = fixture.provider.planWrite(
                fixture.session, new WriteHandle(fixture.tableHandle, insertColumnsSansPartition)
                        .boundTargetColumns(fullSchema)
                        .overwrite(true).staticPartition(partition));
        TPaimonTableSink sink = plan.getDataSink().getPaimonTableSink();

        // The writer's Arrow schema is built from the projected block, which BindSink materialized to
        // the full bound schema (id, pt) — not the caller-supplied insert list (id only). Sizing
        // column_names off the narrower list here is exactly what the production crash reproduced:
        // DORIS_CHECK_EQ(column_names.size(), block.columns()) failing 1 != 2.
        Assertions.assertEquals(fullSchema.size(), sink.getColumnNames().size(),
                "column_names must size to the materialized full bound schema, not the narrowed "
                        + "INSERT list that excludes the static-partition column");
        Assertions.assertEquals(
                fullSchema.stream().map(ConnectorColumn::getName).collect(java.util.stream.Collectors.toList()),
                sink.getColumnNames());
    }

    @Test
    public void rejectsNonPartitionStaticColumn() throws Exception {
        Fixture fixture = fixture();
        List<ConnectorColumn> columns = fixture.provider.getWriteColumns(
                fixture.session, fixture.tableHandle, Optional.empty()).get();

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> fixture.provider.planWrite(
                        fixture.session, new WriteHandle(fixture.tableHandle, columns)
                                .staticPartition(Collections.singletonMap("id", "1"))));

        Assertions.assertTrue(exception.getMessage().contains("not a partition column"));
    }

    @Test
    public void rejectsSchemaDriftAfterTargetBinding() throws Exception {
        Fixture fixture = fixture();
        List<ConnectorColumn> boundColumns = fixture.provider.getWriteColumns(
                fixture.session, fixture.tableHandle, Optional.empty()).get();
        fixture.catalog.alterTable(
                TABLE, SchemaChange.addColumn("extra", DataTypes.BIGINT()), false);

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> fixture.provider.planWrite(
                        fixture.session, new WriteHandle(fixture.tableHandle, boundColumns)));

        Assertions.assertTrue(exception.getMessage().contains("metadata changed"));
    }

    @Test
    public void rejectsUnsupportedWriteOperation() throws Exception {
        // DELETE and MERGE are plannable now (UPDATE arrives as MERGE from the translator), so the
        // unsupported sample is the raw UPDATE enum: no real plan ever sends it, and the backstop must
        // hold even if the analysis-time gate were bypassed.
        Fixture fixture = fixture();
        List<ConnectorColumn> columns = fixture.provider.getWriteColumns(
                fixture.session, fixture.tableHandle, Optional.empty()).get();

        Assertions.assertThrows(
                DorisConnectorException.class,
                () -> fixture.provider.planWrite(
                        fixture.session, new WriteHandle(fixture.tableHandle, columns)
                                .operation(WriteOperation.UPDATE)),
                "the raw UPDATE enum must be rejected by the planWrite backstop");
    }

    @Test
    public void exposesPinnedWriteNullabilityAndDefaults() throws Exception {
        Catalog catalog = new FileSystemCatalog(
                LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()));
        catalog.createDatabase("db", false);
        catalog.createTable(TABLE, Schema.newBuilder()
                .column("required_col", DataTypes.INT().notNull())
                .column("default_col", DataTypes.STRING(), null, "'fallback'")
                .build(), false);
        PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
        PaimonWritePlanProvider provider = new PaimonWritePlanProvider(
                PaimonCatalogProperties.of(Collections.emptyMap()),
                ops,
                new RecordingConnectorContext());
        List<ConnectorColumn> columns = provider.getWriteColumns(
                new WriteSession(new PaimonConnectorTransaction(
                        202L, ops, new RecordingConnectorContext())),
                new PaimonTableHandle("db", "t", Collections.emptyList(), Collections.emptyList()),
                Optional.empty()).get();

        Assertions.assertFalse(columns.get(0).isNullable());
        Assertions.assertNull(columns.get(0).getDefaultValueSql());
        Assertions.assertTrue(columns.get(1).isNullable());
        Assertions.assertEquals("'fallback'", columns.get(1).getDefaultValueSql());
    }

    private Fixture fixture() throws Exception {
        Catalog catalog = new FileSystemCatalog(
                LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()));
        catalog.createDatabase("db", false);
        catalog.createTable(TABLE, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("pt", DataTypes.STRING())
                .partitionKeys("pt")
                .build(), false);
        PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
        RecordingConnectorContext context = new RecordingConnectorContext();
        PaimonConnectorTransaction transaction = new PaimonConnectorTransaction(201L, ops, context);
        WriteSession session = new WriteSession(transaction);
        PaimonWritePlanProvider provider = new PaimonWritePlanProvider(
                PaimonCatalogProperties.of(Collections.emptyMap()), ops, context);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "db", "t", Collections.singletonList("pt"), Collections.emptyList());
        return new Fixture(catalog, provider, transaction, session, tableHandle);
    }

    private static final class Fixture {
        private final Catalog catalog;
        private final PaimonWritePlanProvider provider;
        private final PaimonConnectorTransaction transaction;
        private final WriteSession session;
        private final PaimonTableHandle tableHandle;

        private Fixture(Catalog catalog, PaimonWritePlanProvider provider,
                PaimonConnectorTransaction transaction, WriteSession session,
                PaimonTableHandle tableHandle) {
            this.catalog = catalog;
            this.provider = provider;
            this.transaction = transaction;
            this.session = session;
            this.tableHandle = tableHandle;
        }
    }

    private static final class WriteHandle implements ConnectorWriteHandle {
        private final ConnectorTableHandle tableHandle;
        private final List<ConnectorColumn> columns;
        // Defaults to columns, mirroring the SPI default (ConnectorWriteHandle#getBoundTargetColumns):
        // a handle that never calls boundTargetColumns(...) behaves like a plain full-column write,
        // where the INSERT list and the bound target schema coincide.
        private List<ConnectorColumn> boundTargetColumns;
        private boolean overwrite;
        private Map<String, String> staticPartition = Collections.emptyMap();
        private WriteOperation operation = WriteOperation.INSERT;

        private WriteHandle(ConnectorTableHandle tableHandle, List<ConnectorColumn> columns) {
            this.tableHandle = tableHandle;
            this.columns = columns;
            this.boundTargetColumns = columns;
        }

        private WriteHandle overwrite(boolean value) {
            overwrite = value;
            return this;
        }

        private WriteHandle staticPartition(Map<String, String> value) {
            staticPartition = value;
            return this;
        }

        private WriteHandle operation(WriteOperation value) {
            operation = value;
            return this;
        }

        // Simulates BindSink#bindConnectorTableSink on a real static-partition write: getColumns() is
        // the INSERT column list with the partition column excluded (canonicalStaticPartitionColNames /
        // selectConnectorSinkBindColumns), while getBoundTargetColumns() stays the full bound schema the
        // partition literal is later materialized into. Real callers never diverge these by hand; this
        // setter exists purely so the test can pin the two lists to different values.
        private WriteHandle boundTargetColumns(List<ConnectorColumn> value) {
            boundTargetColumns = value;
            return this;
        }

        @Override
        public ConnectorTableHandle getTableHandle() {
            return tableHandle;
        }

        @Override
        public List<ConnectorColumn> getColumns() {
            return columns;
        }

        @Override
        public List<ConnectorColumn> getBoundTargetColumns() {
            return boundTargetColumns;
        }

        @Override
        public boolean isOverwrite() {
            return overwrite;
        }

        @Override
        public Map<String, String> getStaticPartitionSpec() {
            return staticPartition;
        }

        @Override
        public WriteOperation getWriteOperation() {
            return operation;
        }
    }

    private static final class WriteSession implements ConnectorSession {
        private final ConnectorTransaction transaction;

        private WriteSession(ConnectorTransaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public Optional<ConnectorTransaction> getCurrentTransaction() {
            return Optional.of(transaction);
        }

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
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
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
