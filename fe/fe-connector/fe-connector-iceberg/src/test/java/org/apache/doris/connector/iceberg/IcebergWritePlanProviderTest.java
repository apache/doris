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
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePartitionField;
import org.apache.doris.connector.api.write.ConnectorWritePartitionSpec;
import org.apache.doris.connector.api.write.ConnectorWriteSortColumn;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergDeleteSink;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergRewritableDeleteFileSet;
import org.apache.doris.thrift.TIcebergTableSink;
import org.apache.doris.thrift.TIcebergWriteType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TSortField;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
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
        // Static catalog creds in BE-canonical form (AWS_*), the form the write sink ships to BE — NOT the
        // fs.s3a.* hadoop form (s3_util.cpp convert_properties_to_s3_conf reads only AWS_*). Fed through the
        // typed fe-filesystem seam (getStorageProperties() -> toBackendProperties().toMap()) that the write
        // now derives its BE creds from (design S3), the SAME source the scan path uses.
        ctx.storageProperties = Collections.singletonList(
                fakeBackendStorage(Collections.singletonMap("AWS_ACCESS_KEY", "AK123")));
        ctx.backendFileType = TFileType.FILE_S3;
        return ctx;
    }

    /** A fe-filesystem {@link StorageProperties} whose toBackendProperties().toMap() returns the given
     * BE-canonical map — mirrors how a real object-store binding hands BE creds to the connector, and how
     * the write path (design S3) sources its static creds. Adapted verbatim from the scan test. */
    private static StorageProperties fakeBackendStorage(Map<String, String> beMap) {
        BackendStorageProperties backend = new BackendStorageProperties() {
            @Override
            public BackendStorageKind backendKind() {
                return BackendStorageKind.S3_COMPATIBLE;
            }

            @Override
            public Map<String, String> toMap() {
                return beMap;
            }
        };
        return new StorageProperties() {
            @Override
            public String providerName() {
                return "fake";
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

            @Override
            public Optional<BackendStorageProperties> toBackendProperties() {
                return Optional.of(backend);
            }
        };
    }

    /** A context that resolves writes to a FILE_BROKER backend (ofs/gfs) with the given broker addresses. */
    private static RecordingConnectorContext contextWithBroker(List<ConnectorBrokerAddress> brokers) {
        RecordingConnectorContext ctx = contextWithStorage();
        ctx.backendFileType = TFileType.FILE_BROKER;
        ctx.brokerAddresses = brokers;
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
        // WP-001: byte-equal the legacy partition-specs JSON, not just non-null. A garbled/dropped spec JSON
        // silently corrupts partitioned writes once iceberg cuts over; the value is what BE reads back.
        Assertions.assertEquals(Maps.transformValues(table.specs(), PartitionSpecParser::toJson),
                sink.getPartitionSpecsJson(),
                "partition-specs-json must byte-equal Maps.transformValues(table.specs(), PartitionSpecParser::toJson)");
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, sink.getFileFormat());
        Assertions.assertEquals(TFileCompressType.ZSTD, sink.getCompressionType());
        Assertions.assertFalse(sink.isOverwrite());
        Assertions.assertFalse(sink.isSetStaticPartitionValues());
    }

    // ───────────────────────────── REWRITE: compaction sink (TIcebergTableSink) ─────────────────────────────
    //
    // WHY: post-cutover rewrite_data_files reuses the INSERT TIcebergTableSink dialect with two deltas vs
    // INSERT, byte-identical to legacy planner.IcebergTableSink.bindDataSink under isRewriting:
    // write_type=REWRITE and (fv>=3) the row-lineage schema append. Dormant until a connector rewrite
    // producer is wired, so these pin the sink dialect directly via planWrite.

    @Test
    public void planWriteRewriteSetsRewriteTypeAndKeepsInsertFields() {
        // fv2 table: REWRITE reuses the INSERT baseline (db/tb/schema/partition/format) but stamps
        // write_type=REWRITE; with no v3 row-lineage append the schema-json equals the plain table schema.
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.REWRITE));

        Assertions.assertEquals(TIcebergWriteType.REWRITE, sink.getWriteType(),
                "a REWRITE write must stamp write_type=REWRITE so the BE routes to RewriteFiles semantics");
        Assertions.assertEquals("db1", sink.getDbName());
        Assertions.assertEquals("t1", sink.getTbName());
        Assertions.assertEquals(SchemaParser.toJson(table.schema()), sink.getSchemaJson(),
                "a fv2 rewrite schema-json must equal the plain table schema (no v3 row-lineage append)");
        Assertions.assertEquals(table.spec().specId(), sink.getPartitionSpecId());
        Assertions.assertFalse(sink.isOverwrite());
    }

    @Test
    public void planWriteRewriteFv3AppendsRowLineageSchema() {
        Table table = formatVersionThreeTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.REWRITE));

        Assertions.assertEquals(TIcebergWriteType.REWRITE, sink.getWriteType());
        Assertions.assertTrue(sink.getSchemaJson().contains("_row_id"),
                "fv3 rewrite schema-json must include the row-lineage _row_id field (legacy appendRowLineageFieldsForV3)");
        Assertions.assertTrue(sink.getSchemaJson().contains("_last_updated_sequence_number"),
                "fv3 rewrite schema-json must include the row-lineage _last_updated_sequence_number field");
    }

    @Test
    public void planWriteRewriteRejectsOverwrite() {
        // REWRITE is a compaction, never a user INSERT OVERWRITE; the BE writer rejects the pairing, so the
        // connector fails loud rather than emit a sink with both REWRITE write-type and overwrite=true.
        Table table = partitionedSortedTable(freshCatalog());
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(table, contextWithStorage(),
                        new WriteHandle(new IcebergTableHandle("db1", "t1"))
                                .writeOperation(WriteOperation.REWRITE).overwrite(true)));
        Assertions.assertTrue(ex.getMessage().contains("overwrite"),
                "the rewrite-vs-overwrite rejection must name the offending overwrite flag");
    }

    @Test
    public void planWriteDataLocationFallsBackToObjectStoreThenFolderLocation() {
        // WP-007/parity: dataLocation cascades WRITE_DATA_LOCATION -> (OBJECT_STORE_ENABLED ? OBJECT_STORE_PATH)
        // -> WRITE_FOLDER_STORAGE_LOCATION -> {table.location}/data. Every other test sets WRITE_DATA_LOCATION, so
        // the two object-store / folder-storage fallbacks (which a misordered cascade would silently swap) were
        // never exercised. The resolved path is scheme-normalized (oss:// -> s3://) just like WRITE_DATA_LOCATION.

        // (a) object-store path wins when enabled and no write.data.path is set.
        Table objStore = unpartitionedTableWith("obj", ImmutableMap.of(
                "write.format.default", "parquet",
                TableProperties.OBJECT_STORE_ENABLED, "true",
                TableProperties.OBJECT_STORE_PATH, "oss://bucket/wh/db1/obj/objstore"));
        Assertions.assertEquals("s3://bucket/wh/db1/obj/objstore",
                planSink(objStore, contextWithStorage(),
                        new WriteHandle(new IcebergTableHandle("db1", "obj"))).getOutputPath());

        // (b) folder-storage location wins when object-store is disabled and no write.data.path is set.
        Table folder = unpartitionedTableWith("fold", ImmutableMap.of(
                "write.format.default", "parquet",
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "oss://bucket/wh/db1/fold/folder"));
        Assertions.assertEquals("s3://bucket/wh/db1/fold/folder",
                planSink(folder, contextWithStorage(),
                        new WriteHandle(new IcebergTableHandle("db1", "fold"))).getOutputPath());
    }

    @Test
    public void planWriteMapsFileFormatAndCompressionCodecVariety() {
        // WP-005 / WP-009 / parity: only parquet+zstd is otherwise exercised, yet toTFileFormatType +
        // toTFileCompressType map ORC and seven other codecs. A single enum mis-map (e.g. lz4 -> LZO, or
        // ORC -> PARQUET) silently corrupts every write in that format. Pin a representative ORC + non-zstd matrix.
        assertFormatAndCodec("orc", TableProperties.ORC_COMPRESSION, "zlib",
                TFileFormatType.FORMAT_ORC, TFileCompressType.ZLIB);
        assertFormatAndCodec("parquet", TableProperties.PARQUET_COMPRESSION, "snappy",
                TFileFormatType.FORMAT_PARQUET, TFileCompressType.SNAPPYBLOCK);
        assertFormatAndCodec("parquet", TableProperties.PARQUET_COMPRESSION, "lz4",
                TFileFormatType.FORMAT_PARQUET, TFileCompressType.LZ4BLOCK);
    }

    private void assertFormatAndCodec(String format, String codecKey, String codec,
            TFileFormatType expectedFormat, TFileCompressType expectedCompress) {
        Table table = unpartitionedTableWith("fmt", ImmutableMap.of(
                "write.format.default", format, codecKey, codec, "write.data.path", "oss://bucket/wh/db1/fmt/data"));
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "fmt")));
        Assertions.assertEquals(expectedFormat, sink.getFileFormat());
        Assertions.assertEquals(expectedCompress, sink.getCompressionType());
    }

    /** An unpartitioned table at a fresh catalog with the given properties. */
    private static Table unpartitionedTableWith(String name, Map<String, String> props) {
        return freshCatalog().createTable(TableIdentifier.of("db1", name), SCHEMA,
                PartitionSpec.unpartitioned(), new HashMap<>(props));
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

        // B-1: the sink's hadoop_config must carry the BE-canonical static creds (AWS_*), NOT the fs.s3a.*
        // hadoop form — BE s3_util.cpp reads only AWS_*, so fs.s3a.* would leave the writer credential-less.
        Assertions.assertEquals("AK123", sink.getHadoopConfig().get("AWS_ACCESS_KEY"),
                "hadoop config must carry BE-canonical static creds (legacy getBackendConfigProperties / AWS_*)");
        Assertions.assertNull(sink.getHadoopConfig().get("fs.s3a.access.key"),
                "the sink must not ship the fs.s3a.* hadoop form (BE cannot read it)");
    }

    // ───────────────────────────── broker backend (ofs:// / gfs:// -> FILE_BROKER) ─────────────────────────────
    //
    // WHY: SchemaTypeMapper maps ofs/gfs to FILE_BROKER; the sink must then carry the catalog's broker
    // addresses, or BE gets a broker sink with an empty broker list and the write fails. Legacy
    // IcebergTableSink/DeleteSink/MergeSink each did `if (FILE_BROKER) setBrokerAddresses(...)`; the migration
    // dropped it. The engine resolves the addresses (BrokerMgr); the connector maps the neutral host/port
    // pairs to TNetworkAddress and fails loud when none. MUTATION: dropping a setBrokerAddresses -> red.

    @Test
    public void planWriteBrokerBackendSetsBrokerAddressesOnEverySink() {
        List<ConnectorBrokerAddress> brokers = Arrays.asList(
                new ConnectorBrokerAddress("broker-h1", 8000),
                new ConnectorBrokerAddress("broker-h2", 8001));
        List<TNetworkAddress> expected = Arrays.asList(
                new TNetworkAddress("broker-h1", 8000),
                new TNetworkAddress("broker-h2", 8001));
        Table table = partitionedSortedTable(freshCatalog());

        TIcebergTableSink insert = planSink(table, contextWithBroker(brokers),
                new WriteHandle(new IcebergTableHandle("db1", "t1")));
        Assertions.assertEquals(TFileType.FILE_BROKER, insert.getFileType());
        Assertions.assertEquals(expected, insert.getBrokerAddresses(),
                "INSERT sink must carry the catalog broker addresses for a FILE_BROKER target");

        TIcebergDeleteSink delete = planDeleteSink(table, contextWithBroker(brokers),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.DELETE));
        Assertions.assertEquals(expected, delete.getBrokerAddresses(),
                "DELETE sink must carry the catalog broker addresses for a FILE_BROKER target");

        TIcebergMergeSink merge = planMergeSink(table, contextWithBroker(brokers),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.UPDATE));
        Assertions.assertEquals(expected, merge.getBrokerAddresses(),
                "MERGE sink must carry the catalog broker addresses for a FILE_BROKER target");
    }

    @Test
    public void planWriteBrokerBackendWithNoAliveBrokerFailsLoud() {
        // FILE_BROKER but the engine resolves no broker -> fail loud with the legacy message, never ship an
        // empty broker list to BE.
        Table table = partitionedSortedTable(freshCatalog());
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(table, contextWithBroker(Collections.emptyList()),
                        new WriteHandle(new IcebergTableHandle("db1", "t1"))));
        Assertions.assertEquals("No alive broker.", ex.getMessage());
    }

    @Test
    public void planWriteNonBrokerBackendLeavesBrokerAddressesUnset() {
        // S3/HDFS/local must NOT set broker_addresses (legacy gates the setter on FILE_BROKER).
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")));
        Assertions.assertEquals(TFileType.FILE_S3, sink.getFileType());
        Assertions.assertFalse(sink.isSetBrokerAddresses(),
                "a non-broker write must not set broker_addresses");
    }

    @Test
    public void planWriteOverlaysVendedCredentials() {
        // H-1: a REST vending catalog's static storage map is empty by design; the per-table vended token (read
        // from the table's FileIO) must be overlaid into the write sink's hadoop_config in BE-canonical form
        // (AWS_*), winning over a colliding static key — mirroring the scan path. The token here is non-empty so
        // RecordingConnectorContext.vendStorageCredentials yields the configured BE-canonical vended creds.
        //
        // We drive a FakeIcebergTable whose io() carries a (non-empty) vended token; beginWrite needs a live SDK
        // transaction, so we inject one from a throwaway real catalog table (the planning path stores but never
        // dereferences it).
        InMemoryCatalog catalog = freshCatalog();
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/tvend/data");
        Table real = catalog.createTable(TableIdentifier.of("db1", "tvend"), SCHEMA,
                PartitionSpec.unpartitioned(), tableProps);

        FakeIcebergTable fake = new FakeIcebergTable("tvend", SCHEMA, PartitionSpec.unpartitioned(),
                "oss://bucket/wh/db1/tvend", tableProps);
        fake.setIo(new PropsFileIO(Collections.singletonMap("s3.access-key-id", "vended-raw")));
        fake.setNewTransaction(real.newTransaction());

        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Map<String, String> staticCreds = new HashMap<>();
        staticCreds.put("AWS_ACCESS_KEY", "static-ak");
        staticCreds.put("AWS_REGION", "us-east-1");
        ctx.storageProperties = Collections.singletonList(fakeBackendStorage(staticCreds));
        Map<String, String> vendedCreds = new HashMap<>();
        vendedCreds.put("AWS_ACCESS_KEY", "vended-ak");
        vendedCreds.put("AWS_TOKEN", "vended-tok");
        ctx.vendedBeProps = vendedCreds;

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = fake;
        IcebergWritePlanProvider provider = new IcebergWritePlanProvider(restVendedProps(), ops, ctx);
        WriteSession session = new WriteSession(new IcebergConnectorTransaction(42L, ops, ctx));

        ConnectorSinkPlan plan = provider.planWrite(session,
                new WriteHandle(new IcebergTableHandle("db1", "tvend")));
        TIcebergTableSink sink = plan.getDataSink().getIcebergTableSink();

        Assertions.assertEquals("vended-ak", sink.getHadoopConfig().get("AWS_ACCESS_KEY"),
                "vended creds must win over a colliding static key (legacy/scan precedence)");
        Assertions.assertEquals("vended-tok", sink.getHadoopConfig().get("AWS_TOKEN"),
                "vended-only key must be present in the write sink's hadoop_config (H-1 overlay)");
        Assertions.assertEquals("us-east-1", sink.getHadoopConfig().get("AWS_REGION"),
                "static-only key must remain alongside the vended overlay");
    }

    private static Map<String, String> restVendedProps() {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        props.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        return props;
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

    @Test
    public void planWriteThreadsBranchFromHandleToTransaction() {
        // WHY: INSERT INTO t@branch threads the target branch onto the write handle; planWrite must hand it
        // to IcebergConnectorTransaction.beginWrite, which validates it against the table refs and points
        // the commit at the branch. A freshly created table has no "no_such_branch" ref, so a threaded
        // branch surfaces as a fail-loud "not founded" — proving the branch reached beginWrite.
        // MUTATION: passing Optional.empty() instead of handle.getBranchName() (the DV-T06-branch bug) drops
        // the branch -> no validation -> planWrite succeeds silently (write would land on the default ref)
        // -> this assertThrows turns red.
        Table table = unpartitionedUnsortedTable(freshCatalog());
        // beginWrite validates the branch against the table refs and wraps the failure as a
        // DorisConnectorException ("Failed to begin write ... no_such_branch is not founded").
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(table, contextWithStorage(),
                        new WriteHandle(new IcebergTableHandle("db1", "t2")).branch("no_such_branch")));
        Assertions.assertTrue(ex.getMessage().contains("no_such_branch"),
                "the branch threaded onto the write handle must reach beginWrite's ref validation");
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

    // ───────────────────────────── getWritePartitioning (connector declares, ② C3b-core) ─────────────────────────────
    //
    // WHY: post-flip the iceberg merge-write distribution (DistributionSpecMerge) is built fe-core-side, but
    // its native partition-spec walk (PhysicalIcebergMergeSink.buildInsertPartitionFields ->
    // icebergTable.getIcebergTable().spec()) is DEAD once iceberg is a PluginDrivenExternalCatalog (the native
    // table is unreachable across the connector's isolated classloader). The connector therefore declares the
    // partitioning in an engine-neutral carrier; the engine resolves source-column names to expr ids locally.
    // These pins guard byte-parity of the carried (transform, param, sourceColumnName, fieldName, sourceId,
    // specId) tuple against the legacy native walk.

    /** A bucket(id, 16)-partitioned table: distinct partition field name ("id_bucket") vs source column ("id"). */
    private static Table bucketPartitionedTable(InMemoryCatalog catalog) {
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/tb/data");
        return catalog.createTable(TableIdentifier.of("db1", "tb"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build(), tableProps);
    }

    @Test
    public void getWritePartitioningForIdentityPartitionMapsField() {
        Table table = partitionedSortedTable(freshCatalog());
        ConnectorWritePartitionSpec spec = providerFor(table, contextWithStorage())
                .getWritePartitioning(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "t1"));

        Assertions.assertNotNull(spec, "a partitioned table must declare its write partitioning");
        Assertions.assertEquals(table.spec().specId(), spec.getSpecId());
        Assertions.assertEquals(1, spec.getFields().size());
        ConnectorWritePartitionField f = spec.getFields().get(0);
        Assertions.assertEquals("identity", f.getTransform());
        Assertions.assertNull(f.getTransformParam(), "identity has no bracket argument");
        Assertions.assertEquals("id", f.getSourceColumnName(), "source column resolved from sourceId via the schema");
        Assertions.assertEquals("id", f.getFieldName(), "identity partition field name equals the source column");
        Assertions.assertEquals(table.schema().findField("id").fieldId(), f.getSourceId());
    }

    @Test
    public void getWritePartitioningNullForUnpartitionedTable() {
        // null == "unpartitioned" (legacy gates on spec().isPartitioned()) -> the engine uses its
        // non-partitioned merge distribution, keeping byte-parity for unpartitioned MERGE/UPDATE.
        InMemoryCatalog catalog = freshCatalog();
        partitionedSortedTable(catalog);
        Table table = unpartitionedUnsortedTable(catalog);
        Assertions.assertNull(providerFor(table, contextWithStorage())
                .getWritePartitioning(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "t2")));
    }

    @Test
    public void getWritePartitioningBucketTransformCarriesParamAndDistinctNames() {
        // MUTATION: this is the field that distinguishes the carrier's three name-ish bits. A walk that
        // carried fieldName ("id_bucket") where sourceColumnName ("id") is needed would resolve the wrong
        // (or no) expr id fe-core-side; dropping the parsed param would lose the bucket count BE needs.
        Table table = bucketPartitionedTable(freshCatalog());
        ConnectorWritePartitionSpec spec = providerFor(table, contextWithStorage())
                .getWritePartitioning(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "tb"));

        Assertions.assertNotNull(spec);
        Assertions.assertEquals(1, spec.getFields().size());
        ConnectorWritePartitionField f = spec.getFields().get(0);
        Assertions.assertEquals("bucket[16]", f.getTransform(), "transform string is the native PartitionField.transform().toString()");
        Assertions.assertEquals(Integer.valueOf(16), f.getTransformParam(), "the bracket argument [16] must be parsed out");
        Assertions.assertEquals("id", f.getSourceColumnName(), "source column is the base column, not the partition field");
        Assertions.assertEquals("id_bucket", f.getFieldName(), "partition field name is iceberg's derived 'id_bucket'");
        Assertions.assertEquals(table.schema().findField("id").fieldId(), f.getSourceId());
    }

    // ───────────────────── getSyntheticWriteColumns (connector declares the row-id STRUCT, ③ C3b-core) ─────────────────────
    //
    // WHY: post-flip the iceberg DML hidden column __DORIS_ICEBERG_ROWID_COL__ that legacy
    // IcebergExternalTable.getFullSchema injected is unreachable — a PluginDrivenExternalTable carries no
    // iceberg knowledge. The connector therefore declares it as an engine-neutral invisible ConnectorColumn;
    // fe-core converts + appends it (gated request-side) while a DML over the table is in flight. This pins
    // the carried STRUCT shape (name / 4 fields / types / invisible / not-null) against the legacy
    // fe-core IcebergRowId.createHiddenColumn() (its mirror is the fe-core ConnectorColumnConverter contract pin).

    @Test
    public void getSyntheticWriteColumnsDeclaresRowIdStruct() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        List<ConnectorColumn> cols = providerFor(table, contextWithStorage())
                .getSyntheticWriteColumns(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "t2"));

        Assertions.assertEquals(1, cols.size(), "iceberg declares exactly the row-id synthetic write column");
        ConnectorColumn rowId = cols.get(0);
        Assertions.assertEquals("__DORIS_ICEBERG_ROWID_COL__", rowId.getName());
        Assertions.assertFalse(rowId.isVisible(), "the row-id column must be hidden (invisible)");
        Assertions.assertFalse(rowId.isNullable(), "matches legacy IcebergRowId not-null");

        ConnectorType type = rowId.getType();
        Assertions.assertEquals("STRUCT", type.getTypeName());
        Assertions.assertEquals(
                Arrays.asList("file_path", "row_position", "partition_spec_id", "partition_data"),
                type.getFieldNames());
        List<ConnectorType> fieldTypes = type.getChildren();
        Assertions.assertEquals(4, fieldTypes.size());
        Assertions.assertEquals("STRING", fieldTypes.get(0).getTypeName());
        Assertions.assertEquals("BIGINT", fieldTypes.get(1).getTypeName());
        Assertions.assertEquals("INT", fieldTypes.get(2).getTypeName());
        Assertions.assertEquals("STRING", fieldTypes.get(3).getTypeName());
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
        Assertions.assertEquals("AK123", sink.getHadoopConfig().get("AWS_ACCESS_KEY"),
                "hadoop config must carry BE-canonical static creds (AWS_*), not the fs.s3a.* hadoop form");
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

    // ── commit-bridge supply (S4 part 2): planWrite drains the scan-time stash into rewritable_delete_file_sets ──

    private static final String STASH_QID = "qid-stash";

    private static IcebergWritePlanProvider providerWithStash(Table table, RecordingConnectorContext ctx,
            IcebergRewritableDeleteStash stash) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return new IcebergWritePlanProvider(NON_REST_PROPS, ops, ctx, stash);
    }

    private static WriteSession stashSession(Table table, RecordingConnectorContext ctx, String queryId) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        IcebergConnectorTransaction txn = new IcebergConnectorTransaction(42L, ops, ctx);
        return new WriteSession(txn, queryId);
    }

    private static List<TIcebergDeleteFileDesc> dvDescs(String path, long offset, long size) {
        TIcebergDeleteFileDesc d = new TIcebergDeleteFileDesc();
        d.setPath(path);
        d.setContent(3);
        d.setContentOffset(offset);
        d.setContentSizeInBytes(size);
        return Collections.singletonList(d);
    }

    @Test
    public void planWriteDeleteSinkAttachesRewritableSetsFromStashAndEvicts() {
        // The scan stashed this statement's old DV (referenced data file -> its non-equality deletes); planWrite
        // must drain it onto the sink so the BE OR-merges those deletes into the new DV. MUTATION: not reading
        // the stash / not setting the field -> the BE writes a DV without the old deletes -> resurrection.
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate(STASH_QID, "oss://bucket/wh/db1/tv3/data/f1.parquet",
                dvDescs("oss://bucket/wh/db1/tv3/data/dv1.puffin", 16L, 64L));

        ConnectorSinkPlan plan = providerWithStash(table, ctx, stash).planWrite(
                stashSession(table, ctx, STASH_QID),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.DELETE));
        TIcebergDeleteSink sink = plan.getDataSink().getIcebergDeleteSink();

        Assertions.assertTrue(sink.isSetRewritableDeleteFileSets());
        Assertions.assertEquals(1, sink.getRewritableDeleteFileSetsSize());
        TIcebergRewritableDeleteFileSet set = sink.getRewritableDeleteFileSets().get(0);
        // The set is keyed on the RAW referenced data-file path the BE matches on.
        Assertions.assertEquals("oss://bucket/wh/db1/tv3/data/f1.parquet", set.getReferencedDataFilePath());
        Assertions.assertEquals(1, set.getDeleteFilesSize());
        Assertions.assertEquals("oss://bucket/wh/db1/tv3/data/dv1.puffin", set.getDeleteFiles().get(0).getPath());
        Assertions.assertEquals(3, set.getDeleteFiles().get(0).getContent());
        // The retrieve is the primary eviction; a second statement must not re-read this entry.
        Assertions.assertEquals(0, stash.size(), "planWrite must evict the stash entry it consumed");
    }

    @Test
    public void planWriteMergeSinkAttachesRewritableSetsFromStash() {
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate(STASH_QID, "oss://bucket/wh/db1/tv3/data/f1.parquet",
                dvDescs("oss://bucket/wh/db1/tv3/data/dv1.puffin", 8L, 32L));

        ConnectorSinkPlan plan = providerWithStash(table, ctx, stash).planWrite(
                stashSession(table, ctx, STASH_QID),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.MERGE));
        TIcebergMergeSink sink = plan.getDataSink().getIcebergMergeSink();

        Assertions.assertTrue(sink.isSetRewritableDeleteFileSets(),
                "a v3 MERGE must carry rewritable_delete_file_sets (thrift field 25) too");
        Assertions.assertEquals("oss://bucket/wh/db1/tv3/data/f1.parquet",
                sink.getRewritableDeleteFileSets().get(0).getReferencedDataFilePath());
        Assertions.assertEquals(0, stash.size());
    }

    @Test
    public void planWriteDeleteSinkLeavesSetsUnsetWhenNoStashEntryForQuery() {
        // A v3 DELETE whose scan stashed nothing for this queryId (no live deletes) leaves the field unset —
        // byte-identical to legacy's empty gate.
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate("some-other-query", "oss://bucket/wh/db1/tv3/data/f1.parquet",
                dvDescs("dv", 1L, 2L));

        ConnectorSinkPlan plan = providerWithStash(table, ctx, stash).planWrite(
                stashSession(table, ctx, STASH_QID),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.DELETE));

        Assertions.assertFalse(plan.getDataSink().getIcebergDeleteSink().isSetRewritableDeleteFileSets());
        // The other query's entry is untouched.
        Assertions.assertEquals(1, stash.size());
    }

    @Test
    public void planWriteVersionTwoDeleteNeverAttachesRewritableSetsButStillEvicts() {
        // v2 deletes are plain position-delete files (no DV union), so even a stashed supply must NOT be emitted
        // (the BE ignores field 15 below v3). MUTATION: dropping the formatVersion>=3 gate would emit it. The
        // retrieve still evicts (no leak).
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "tv2"), SCHEMA,
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate(STASH_QID, "oss://bucket/wh/db1/tv2/data/f1.parquet", dvDescs("dv", 1L, 2L));

        ConnectorSinkPlan plan = providerWithStash(table, ctx, stash).planWrite(
                stashSession(table, ctx, STASH_QID),
                new WriteHandle(new IcebergTableHandle("db1", "tv2")).writeOperation(WriteOperation.DELETE));

        Assertions.assertFalse(plan.getDataSink().getIcebergDeleteSink().isSetRewritableDeleteFileSets());
        Assertions.assertEquals(0, stash.size(), "the retrieve evicts even when the v3 gate drops the supply");
    }

    @Test
    public void planWriteInsertEvictsStashEntryForTheStatement() {
        // INSERT ... SELECT FROM an iceberg source stashes the source scan's deletes, but the INSERT write does
        // not consume them; planWrite must still evict the entry so it does not leak. MUTATION: gating the
        // retrieve on DELETE/MERGE only would leak the INSERT path's entry.
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergRewritableDeleteStash stash = new IcebergRewritableDeleteStash();
        stash.accumulate(STASH_QID, "oss://bucket/wh/db1/tv3/data/src.parquet", dvDescs("dv", 1L, 2L));

        providerWithStash(table, ctx, stash).planWrite(
                stashSession(table, ctx, STASH_QID),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.INSERT));

        Assertions.assertEquals(0, stash.size(), "an INSERT write still evicts its statement's stash entry");
    }

    @Test
    public void planWriteThreadsPinnedReadSnapshotFromHandleToTransaction() {
        // [SHOULD-2] / Fix B: planWrite must read the MVCC read-snapshot pin off the (pinned) write table
        // handle and thread it into beginWrite, so the RowDelta anchors baseSnapshotId at the statement's
        // read snapshot (S_read), not a fresh re-read of current (S_write). The translator threads the pin
        // onto the handle (mirroring the scan); this proves the connector consumes handle.getSnapshotId().
        // A synthetic pin id is enough: beginWrite stores it (history validation is deferred to commit), and
        // the empty table's current snapshot is null, so a stored pin is unambiguously the threaded value.
        InMemoryCatalog catalog = freshCatalog();
        catalog.createTable(TableIdentifier.of("db1", "tv2"), SCHEMA,
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
        long pinnedReadSnapshot = 7777L;

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(TableIdentifier.of("db1", "tv2"));
        IcebergConnectorTransaction txn = new IcebergConnectorTransaction(42L, ops, contextWithStorage());
        WriteSession session = new WriteSession(txn);

        ConnectorWriteHandle handle = new WriteHandle(
                new IcebergTableHandle("db1", "tv2").withSnapshot(
                        pinnedReadSnapshot, null, ops.table.schema().schemaId()))
                .writeOperation(WriteOperation.DELETE);
        // The table has no current snapshot, so a non-threaded pin would leave baseSnapshotId null; a stored
        // 7777 is unambiguously the value read off the handle.
        providerFor(ops.table, contextWithStorage()).planWrite(session, handle);

        Assertions.assertEquals(Long.valueOf(pinnedReadSnapshot), txn.getBaseSnapshotId(),
                "planWrite must thread the handle's pinned read snapshot into beginWrite as baseSnapshotId");
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
        Assertions.assertEquals("AK123", sink.getHadoopConfig().get("AWS_ACCESS_KEY"),
                "hadoop config must carry BE-canonical static creds (AWS_*), not the fs.s3a.* hadoop form");
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

    // ───────────────────────────── write capability declarations (single-source-of-truth) ─────────────────────────────
    //
    // WHY: the write plan provider is now the single source of truth for a connector's write capabilities
    // (supportedOperations + the sink-trait defaults from ConnectorWritePlanProvider). Iceberg supports the
    // full DML surface (INSERT/OVERWRITE/DELETE/MERGE/REWRITE), write-targeted branches, parallel write,
    // full-schema write order, and materializing static partition values — but does NOT require
    // partition-local sort (unlike e.g. MaxCompute), so that one trait stays at its interface default (false).

    @Test
    public void declaresFullWriteOperationSet() {
        IcebergWritePlanProvider provider = providerFor(unpartitionedUnsortedTable(freshCatalog()), contextWithStorage());

        Assertions.assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
                WriteOperation.DELETE, WriteOperation.MERGE, WriteOperation.REWRITE), provider.supportedOperations());
        Assertions.assertTrue(provider.supportsWriteBranch());
        Assertions.assertTrue(provider.requiresParallelWrite());
        Assertions.assertTrue(provider.requiresFullSchemaWriteOrder());
        Assertions.assertTrue(provider.requiresMaterializeStaticPartitionValues());
        Assertions.assertFalse(provider.requiresPartitionLocalSort(),
                "iceberg does NOT require partition-local sort (unlike MaxCompute)");
    }

    // ───────────────────────────── test doubles ─────────────────────────────

    /** A bound write request; mirrors the engine's PluginDrivenWriteHandle (which is fe-core-private). */
    private static final class WriteHandle implements ConnectorWriteHandle {
        private final ConnectorTableHandle tableHandle;
        private boolean overwrite;
        private Map<String, String> writeContext = Collections.emptyMap();
        private TSortInfo sortInfo;
        private WriteOperation writeOperation = WriteOperation.INSERT;
        private Optional<String> branchName = Optional.empty();

        WriteHandle(ConnectorTableHandle tableHandle) {
            this.tableHandle = tableHandle;
        }

        WriteHandle branch(String v) {
            this.branchName = Optional.ofNullable(v);
            return this;
        }

        @Override
        public Optional<String> getBranchName() {
            return branchName;
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
        private final String queryId;

        WriteSession(ConnectorTransaction txn) {
            this(txn, "q");
        }

        WriteSession(ConnectorTransaction txn, String queryId) {
            this.txn = txn;
            this.queryId = queryId;
        }

        @Override
        public Optional<ConnectorTransaction> getCurrentTransaction() {
            return Optional.ofNullable(txn);
        }

        @Override
        public String getQueryId() {
            return queryId;
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

    /** Minimal {@link FileIO} whose {@link #properties()} yields a known (non-empty) vended token map, so
     * {@link IcebergScanPlanProvider#extractVendedToken} returns a non-empty token through the write path
     * (H-1). Mirrors the scan test's equivalent double; the read/write file methods are never exercised. */
    private static final class PropsFileIO implements FileIO {
        private final Map<String, String> props;

        PropsFileIO(Map<String, String> props) {
            this.props = props;
        }

        @Override
        public Map<String, String> properties() {
            return props;
        }

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }
}
