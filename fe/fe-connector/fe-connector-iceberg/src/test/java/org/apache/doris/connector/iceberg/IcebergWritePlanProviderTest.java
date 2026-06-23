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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWriteSortColumn;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteSink;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergTableSink;
import org.apache.doris.thrift.TSortField;
import org.apache.doris.thrift.TSortInfo;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins {@link IcebergWritePlanProvider#planWrite} for INSERT/OVERWRITE against legacy
 * {@code planner.IcebergTableSink.bindDataSink} expected <b>values</b> (real {@link InMemoryCatalog},
 * no Mockito).
 *
 * <p><b>WHY this matters:</b> T06 moves the {@code TIcebergTableSink} assembly out of the fe-core
 * planner into the connector. The sink Thrift goes to BE unchanged (C2, zero BE change), so every
 * field must be byte-identical to the legacy sink: schema-json, partition specs, sort info, file
 * format/compression, the vended-aware hadoop config, and the normalized output path. A
 * parity-by-omission (a dropped field) silently corrupts writes once iceberg cuts over at P6.6.</p>
 */
public class IcebergWritePlanProviderTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    private static final Map<String, String> NON_REST_PROPS =
            Collections.singletonMap("iceberg.catalog.type", "hadoop");

    private static InMemoryCatalog freshCatalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog;
    }

    /** A partitioned (identity id), sorted (id ASC NULLS FIRST) parquet+zstd table at a known oss:// data path. */
    private static Table partitionedSortedTable(InMemoryCatalog catalog) {
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.parquet.compression-codec", "zstd");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/t1/data");
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("id").build(), tableProps);
        table.replaceSortOrder().asc("id", NullOrder.NULLS_FIRST).commit();
        return catalog.loadTable(TableIdentifier.of("db1", "t1"));
    }

    private static Table unpartitionedUnsortedTable(InMemoryCatalog catalog) {
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/t2/data");
        return catalog.createTable(TableIdentifier.of("db1", "t2"), SCHEMA,
                PartitionSpec.unpartitioned(), tableProps);
    }

    /** A format-version 3 table (exercises the merge sink's row-lineage schema append + v3 delete path). */
    private static Table formatVersionThreeTable(InMemoryCatalog catalog) {
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/tv3/data");
        tableProps.put("format-version", "3");
        return catalog.createTable(TableIdentifier.of("db1", "tv3"), SCHEMA,
                PartitionSpec.unpartitioned(), tableProps);
    }

    private static RecordingConnectorContext contextWithStorage() {
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.storageProperties = Collections.singletonList(new FakeHadoopStorageProperties(
                Collections.singletonMap("fs.s3a.access.key", "AK123")));
        ctx.backendFileType = TFileType.FILE_S3;
        return ctx;
    }

    private static IcebergWritePlanProvider providerFor(Table table, RecordingConnectorContext ctx) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return new IcebergWritePlanProvider(NON_REST_PROPS, ops, ctx);
    }

    /** A session that carries the bound iceberg connector transaction (the provider reads it). */
    private static WriteSession sessionFor(Table table, RecordingConnectorContext ctx) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        IcebergConnectorTransaction txn = new IcebergConnectorTransaction(42L, ops, ctx);
        return new WriteSession(txn);
    }

    private static TIcebergTableSink planSink(Table table, RecordingConnectorContext ctx,
            ConnectorWriteHandle handle) {
        ConnectorSinkPlan plan = providerFor(table, ctx).planWrite(sessionFor(table, ctx), handle);
        Assertions.assertEquals(TDataSinkType.ICEBERG_TABLE_SINK, plan.getDataSink().getType());
        return plan.getDataSink().getIcebergTableSink();
    }

    // ───────────────────────────── INSERT: table-derived fields ─────────────────────────────

    @Test
    public void planWriteBuildsInsertSinkWithTableDerivedFields() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")));

        Assertions.assertEquals("db1", sink.getDbName());
        Assertions.assertEquals("t1", sink.getTbName());
        Assertions.assertEquals(SchemaParser.toJson(table.schema()), sink.getSchemaJson(),
                "schema-json must equal the legacy SchemaParser.toJson(table.schema()) (no v3 rewrite append)");
        Assertions.assertEquals(table.spec().specId(), sink.getPartitionSpecId());
        Assertions.assertNotNull(sink.getPartitionSpecsJson(), "partitioned table must emit partition specs");
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, sink.getFileFormat());
        Assertions.assertEquals(TFileCompressType.ZSTD, sink.getCompressionType());
        Assertions.assertFalse(sink.isOverwrite());
        Assertions.assertFalse(sink.isSetStaticPartitionValues());
    }

    @Test
    public void planWriteNormalizesOutputPathAndKeepsOriginalRaw() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")));

        Assertions.assertEquals("s3://bucket/wh/db1/t1/data", sink.getOutputPath(),
                "output path must be normalized through the context seam (oss -> s3) for the BE writer");
        Assertions.assertEquals("oss://bucket/wh/db1/t1/data", sink.getOriginalOutputPath(),
                "original output path must stay raw (legacy setOriginalOutputPath)");
        Assertions.assertEquals(TFileType.FILE_S3, sink.getFileType());
    }

    @Test
    public void planWriteMergesStorageHadoopConfig() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")));

        Assertions.assertEquals("AK123", sink.getHadoopConfig().get("fs.s3a.access.key"),
                "hadoop config must be merged from the catalog's storage properties (legacy getBackendConfigProperties)");
    }

    @Test
    public void planWriteNonPartitionedOmitsPartitionSpec() {
        InMemoryCatalog catalog = freshCatalog();
        partitionedSortedTable(catalog);
        Table table = unpartitionedUnsortedTable(catalog);
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t2")));

        Assertions.assertFalse(sink.isSetPartitionSpecsJson(),
                "an unpartitioned table must not emit partition specs (legacy gates on spec().isPartitioned())");
    }

    // ───────────────────────────── OVERWRITE + static partition ─────────────────────────────

    @Test
    public void planWriteOverwriteSetsOverwriteFlag() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).overwrite(true));

        Assertions.assertTrue(sink.isOverwrite());
    }

    @Test
    public void planWriteStaticPartitionOverwriteSetsStaticValues() {
        Table table = partitionedSortedTable(freshCatalog());
        Map<String, String> staticValues = Collections.singletonMap("id", "7");
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).overwrite(true).writeContext(staticValues));

        Assertions.assertTrue(sink.isOverwrite());
        Assertions.assertEquals(staticValues, sink.getStaticPartitionValues(),
                "INSERT OVERWRITE ... PARTITION must pass the static partition values to BE");
    }

    // ───────────────────────────── sort info (engine-built, stamped) ─────────────────────────────

    @Test
    public void planWriteStampsHandleSortInfoOntoSink() {
        Table table = partitionedSortedTable(freshCatalog());
        TSortInfo engineBuilt = new TSortInfo();
        engineBuilt.setIsAscOrder(Collections.singletonList(true));
        engineBuilt.setNullsFirst(Collections.singletonList(true));
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).sortInfo(engineBuilt));

        Assertions.assertEquals(engineBuilt, sink.getSortInfo(),
                "the engine-built TSortInfo (from the connector's declared write-sort columns) must be "
                        + "stamped onto the sink verbatim");
    }

    @Test
    public void planWriteWithoutHandleSortInfoLeavesSinkUnsorted() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")));

        Assertions.assertFalse(sink.isSetSortInfo(),
                "no engine-built sort info on the handle -> no sort_info on the sink");
    }

    // ───────────────────────────── getWriteSortColumns (connector declares) ─────────────────────────────

    @Test
    public void getWriteSortColumnsForSortedTableMapsIdentityFields() {
        Table table = partitionedSortedTable(freshCatalog());
        List<ConnectorWriteSortColumn> cols = providerFor(table, contextWithStorage())
                .getWriteSortColumns(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals(0, cols.get(0).getColumnIndex(), "id is full-schema column 0");
        Assertions.assertTrue(cols.get(0).isAsc());
        Assertions.assertTrue(cols.get(0).isNullsFirst());
    }

    @Test
    public void getWriteSortColumnsNullForUnsortedTable() {
        // null == "no write sort order" (legacy gates setSortInfo on isSorted()) -> the engine emits no
        // TSortInfo, keeping the unsorted-write byte-parity.
        InMemoryCatalog catalog = freshCatalog();
        partitionedSortedTable(catalog);
        Table table = unpartitionedUnsortedTable(catalog);
        Assertions.assertNull(providerFor(table, contextWithStorage())
                .getWriteSortColumns(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "t2")));
    }

    @Test
    public void getWriteSortColumnsNonNullEmptyForSortOrderWithoutIdentityColumns() {
        // A table sorted ONLY by a non-identity transform (e.g. bucket) still HAS a write sort order, so
        // legacy unconditionally sets an (empty) sort_info inside the isSorted() branch -> BE uses the
        // sort writer. The connector must signal this as a non-null EMPTY list (not null), so the engine
        // emits an empty TSortInfo. MUTATION: returning null here -> BE uses the plain writer -> red.
        InMemoryCatalog catalog = freshCatalog();
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/t3/data");
        Table table = catalog.createTable(TableIdentifier.of("db1", "t3"), SCHEMA,
                PartitionSpec.unpartitioned(), tableProps);
        table.replaceSortOrder().asc(Expressions.bucket("id", 4)).commit();
        Table reloaded = catalog.loadTable(TableIdentifier.of("db1", "t3"));

        List<ConnectorWriteSortColumn> cols = providerFor(reloaded, contextWithStorage())
                .getWriteSortColumns(sessionFor(reloaded, contextWithStorage()),
                        new IcebergTableHandle("db1", "t3"));
        Assertions.assertNotNull(cols, "a sorted table (even by a non-identity transform) has a write sort order");
        Assertions.assertTrue(cols.isEmpty(), "no identity column resolves -> empty list -> empty TSortInfo");
    }

    // ───────────────────────────── fail-loud ─────────────────────────────

    @Test
    public void planWriteWithoutTransactionFailsLoud() {
        Table table = partitionedSortedTable(freshCatalog());
        IcebergWritePlanProvider provider = providerFor(table, contextWithStorage());
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(new WriteSession(null),
                        new WriteHandle(new IcebergTableHandle("db1", "t1"))));
        Assertions.assertTrue(ex.getMessage().contains("transaction"),
                "an iceberg write with no bound connector transaction must fail loud");
    }

    // ───────────────────────────── DELETE sink (TIcebergDeleteSink) ─────────────────────────────
    //
    // WHY: T07a moves the legacy planner.IcebergDeleteSink.bindDataSink Thrift assembly into the
    // connector. The sink goes to BE unchanged (C2), so every field must be byte-identical to the legacy
    // sink. Note the legacy delete sink uses compress_type (field 6), NOT the table/merge sink's
    // compression_type — a parity-by-omission silently corrupts v2 position-delete writes at P6.6.

    private static TIcebergDeleteSink planDeleteSink(Table table, RecordingConnectorContext ctx,
            ConnectorWriteHandle handle) {
        ConnectorSinkPlan plan = providerFor(table, ctx).planWrite(sessionFor(table, ctx), handle);
        Assertions.assertEquals(TDataSinkType.ICEBERG_DELETE_SINK, plan.getDataSink().getType(),
                "a DELETE write operation must dispatch to the TIcebergDeleteSink dialect");
        return plan.getDataSink().getIcebergDeleteSink();
    }

    @Test
    public void planWriteBuildsDeleteSinkWithTableDerivedFields() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergDeleteSink sink = planDeleteSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.DELETE));

        Assertions.assertEquals("db1", sink.getDbName());
        Assertions.assertEquals("t1", sink.getTbName());
        Assertions.assertEquals(TFileContent.POSITION_DELETES, sink.getDeleteType(),
                "iceberg delete is always a position delete (the only DeleteFileType)");
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, sink.getFileFormat());
        Assertions.assertEquals(TFileCompressType.ZSTD, sink.getCompressType(),
                "the delete sink carries compress_type (thrift field 6), NOT compression_type (the merge/table field)");
        Assertions.assertEquals("AK123", sink.getHadoopConfig().get("fs.s3a.access.key"));
        Assertions.assertEquals("s3://bucket/wh/db1/t1/data", sink.getOutputPath(),
                "delete output path is the normalized data location (legacy LocationPath.toStorageLocation)");
        Assertions.assertEquals("oss://bucket/wh/db1/t1/data", sink.getTableLocation(),
                "table_location stays the raw data location (legacy IcebergUtils.dataLocation)");
        Assertions.assertEquals(TFileType.FILE_S3, sink.getFileType());
        Assertions.assertEquals(table.spec().specId(), sink.getPartitionSpecId());
        Assertions.assertEquals(2, sink.getFormatVersion());
        Assertions.assertFalse(sink.isSetRewritableDeleteFileSets(),
                "the bindDataSink port never stamps rewritable delete file sets (post-finalize, fv3, T07c)");
    }

    @Test
    public void planWriteDeleteSinkNonPartitionedOmitsSpecId() {
        InMemoryCatalog catalog = freshCatalog();
        partitionedSortedTable(catalog);
        Table table = unpartitionedUnsortedTable(catalog);
        TIcebergDeleteSink sink = planDeleteSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t2")).writeOperation(WriteOperation.DELETE));

        Assertions.assertFalse(sink.isSetPartitionSpecId(),
                "an unpartitioned table must not emit a partition spec id (legacy gates on spec().isPartitioned())");
    }

    @Test
    public void planWriteDeleteSinkFormatVersionThree() {
        Table table = formatVersionThreeTable(freshCatalog());
        TIcebergDeleteSink sink = planDeleteSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.DELETE));

        Assertions.assertEquals(3, sink.getFormatVersion());
        Assertions.assertFalse(sink.isSetRewritableDeleteFileSets(),
                "a v3 delete with no live delete files to rewrite must not set rewritable sets (legacy gates on non-empty)");
    }

    // ───────────────────────────── MERGE sink (TIcebergMergeSink) ─────────────────────────────
    //
    // WHY: UPDATE and MERGE both write the TIcebergMergeSink dialect. Two parity traps vs the table/delete
    // sinks: (1) merge carries compression_type (field 8), NOT compress_type; (2) merge carries sort_fields
    // (field 6, a List<TSortField> built directly from the iceberg SortOrder), NOT the INSERT path's
    // sort_info(16). At fv3 the schema_json must include the row-lineage fields (legacy
    // appendRowLineageFieldsForV3), else BE v3 merge writes mismatch.

    private static TIcebergMergeSink planMergeSink(Table table, RecordingConnectorContext ctx,
            ConnectorWriteHandle handle) {
        ConnectorSinkPlan plan = providerFor(table, ctx).planWrite(sessionFor(table, ctx), handle);
        Assertions.assertEquals(TDataSinkType.ICEBERG_MERGE_SINK, plan.getDataSink().getType(),
                "an UPDATE/MERGE write operation must dispatch to the TIcebergMergeSink dialect");
        return plan.getDataSink().getIcebergMergeSink();
    }

    @Test
    public void planWriteBuildsMergeSinkWithTableDerivedFields() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.UPDATE));

        Assertions.assertEquals("db1", sink.getDbName());
        Assertions.assertEquals("t1", sink.getTbName());
        Assertions.assertEquals(2, sink.getFormatVersion());
        Assertions.assertEquals(SchemaParser.toJson(table.schema()), sink.getSchemaJson(),
                "fv2 merge schema-json equals SchemaParser.toJson(table.schema()) (no row-lineage append below v3)");
        Assertions.assertEquals(table.spec().specId(), sink.getPartitionSpecId());
        Assertions.assertNotNull(sink.getPartitionSpecsJson());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, sink.getFileFormat());
        Assertions.assertEquals(TFileCompressType.ZSTD, sink.getCompressionType(),
                "the merge sink carries compression_type (thrift field 8), NOT compress_type (the delete field)");
        Assertions.assertEquals("AK123", sink.getHadoopConfig().get("fs.s3a.access.key"));
        Assertions.assertEquals("s3://bucket/wh/db1/t1/data", sink.getOutputPath());
        Assertions.assertEquals("oss://bucket/wh/db1/t1/data", sink.getOriginalOutputPath());
        Assertions.assertEquals("oss://bucket/wh/db1/t1/data", sink.getTableLocation());
        Assertions.assertEquals(TFileType.FILE_S3, sink.getFileType());
        // delete side
        Assertions.assertEquals(TFileContent.POSITION_DELETES, sink.getDeleteType());
        Assertions.assertEquals(table.spec().specId(), sink.getPartitionSpecIdForDelete());
        Assertions.assertFalse(sink.isSetRewritableDeleteFileSets());
    }

    @Test
    public void planWriteMergeSinkSortFieldsFromIdentitySortOrder() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.UPDATE));

        Assertions.assertTrue(sink.isSetSortFields());
        Assertions.assertEquals(1, sink.getSortFields().size());
        TSortField sf = sink.getSortFields().get(0);
        Assertions.assertEquals(table.schema().findField("id").fieldId(), sf.getSourceColumnId(),
                "merge sort_fields carry the iceberg source field id directly (legacy SortField.sourceId), not a column index");
        Assertions.assertTrue(sf.isAscending());
        Assertions.assertTrue(sf.isNullFirst());
    }

    @Test
    public void planWriteMergeSinkUnsortedOmitsSortFields() {
        InMemoryCatalog catalog = freshCatalog();
        partitionedSortedTable(catalog);
        Table table = unpartitionedUnsortedTable(catalog);
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t2")).writeOperation(WriteOperation.UPDATE));

        Assertions.assertFalse(sink.isSetSortFields(),
                "an unsorted table must not emit sort_fields (legacy gates on sortOrder().isSorted())");
    }

    @Test
    public void planWriteMergeSinkFv3AppendsRowLineageSchema() {
        Table table = formatVersionThreeTable(freshCatalog());
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.MERGE));

        Assertions.assertEquals(3, sink.getFormatVersion());
        Assertions.assertTrue(sink.getSchemaJson().contains("_row_id"),
                "fv3 merge schema-json must include the row-lineage _row_id field (legacy appendRowLineageFieldsForV3)");
        Assertions.assertTrue(sink.getSchemaJson().contains("_last_updated_sequence_number"),
                "fv3 merge schema-json must include the row-lineage _last_updated_sequence_number field");
    }

    @Test
    public void planWriteMergeOperationAlsoBuildsMergeSink() {
        Table table = partitionedSortedTable(freshCatalog());
        // The MERGE write operation shares the merge sink family with UPDATE.
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.MERGE));
        Assertions.assertEquals("t1", sink.getTbName());
    }

    // ───────────────────────────── test doubles ─────────────────────────────

    /** A bound write request; mirrors the engine's PluginDrivenWriteHandle (which is fe-core-private). */
    private static final class WriteHandle implements ConnectorWriteHandle {
        private final ConnectorTableHandle tableHandle;
        private boolean overwrite;
        private Map<String, String> writeContext = Collections.emptyMap();
        private TSortInfo sortInfo;
        private WriteOperation writeOperation = WriteOperation.INSERT;

        WriteHandle(ConnectorTableHandle tableHandle) {
            this.tableHandle = tableHandle;
        }

        WriteHandle overwrite(boolean v) {
            this.overwrite = v;
            return this;
        }

        WriteHandle writeOperation(WriteOperation v) {
            this.writeOperation = v;
            return this;
        }

        @Override
        public WriteOperation getWriteOperation() {
            return writeOperation;
        }

        WriteHandle writeContext(Map<String, String> v) {
            this.writeContext = v;
            return this;
        }

        WriteHandle sortInfo(TSortInfo v) {
            this.sortInfo = v;
            return this;
        }

        @Override
        public ConnectorTableHandle getTableHandle() {
            return tableHandle;
        }

        @Override
        public List<ConnectorColumn> getColumns() {
            return Collections.emptyList();
        }

        @Override
        public boolean isOverwrite() {
            return overwrite;
        }

        @Override
        public Map<String, String> getWriteContext() {
            return writeContext;
        }

        @Override
        public TSortInfo getSortInfo() {
            return sortInfo;
        }
    }

    /** A session that returns the bound connector transaction; the timezone feeds beginWrite. */
    private static final class WriteSession implements ConnectorSession {
        private final ConnectorTransaction txn;

        WriteSession(ConnectorTransaction txn) {
            this.txn = txn;
        }

        @Override
        public Optional<ConnectorTransaction> getCurrentTransaction() {
            return Optional.ofNullable(txn);
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
            return 0;
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

    /** Minimal {@link StorageProperties} that yields a known hadoop configuration map. */
    private static final class FakeHadoopStorageProperties implements StorageProperties, HadoopStorageProperties {
        private final Map<String, String> hadoopConfig;

        FakeHadoopStorageProperties(Map<String, String> hadoopConfig) {
            this.hadoopConfig = hadoopConfig;
        }

        @Override
        public Optional<HadoopStorageProperties> toHadoopProperties() {
            return Optional.of(this);
        }

        @Override
        public Map<String, String> toHadoopConfigurationMap() {
            return hadoopConfig;
        }

        @Override
        public String providerName() {
            return "S3";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.OBJECT_STORAGE;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.S3;
        }

        @Override
        public Map<String, String> rawProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> matchedProperties() {
            return Collections.emptyMap();
        }
    }
}
