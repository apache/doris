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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveColumn;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartition;
import org.apache.doris.thrift.THiveTableSink;
import org.apache.doris.thrift.TNetworkAddress;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins {@link HiveWritePlanProvider#planWrite} for INSERT / INSERT OVERWRITE against legacy
 * {@code planner.HiveTableSink.bindDataSink} expected <b>values</b> (recording {@link HmsClient} fake, no
 * Mockito).
 *
 * <p><b>WHY this matters:</b> the {@code THiveTableSink} assembly moved out of the fe-core planner into the
 * connector. The sink Thrift goes to BE unchanged (zero BE change), so every field must be byte-identical to
 * the legacy sink: the tagged column list, the existing-partition list, bucket info, file format/compression,
 * the staging-vs-in-place location, the SerDe delimiters, and the overwrite flag. A parity-by-omission (a
 * dropped/mis-tagged field) silently corrupts hive writes once hive cuts over. Each assertion pins the WHY the
 * field is load-bearing for BE.</p>
 */
public class HiveWritePlanProviderTest {

    private static final String DB = "db";
    private static final String TBL = "t";

    private static final String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    private static final String ORC_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    private static final String PARQUET_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String LZO_INPUT_FORMAT = "com.hadoop.mapred.DeprecatedLzoTextInputFormat";
    private static final String TEXT_OUTPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    private static final String LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final String MULTI_DELIMIT_SERDE = "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";

    // ───────────────────────────── helpers ─────────────────────────────

    private static ConnectorColumn col(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), "", true, null);
    }

    private static HmsTableInfo.Builder tableBuilder() {
        return HmsTableInfo.builder()
                .dbName(DB).tableName(TBL).tableType("MANAGED_TABLE")
                .location("oss://bucket/db/t")
                .inputFormat(TEXT_INPUT_FORMAT)
                .outputFormat(TEXT_OUTPUT_FORMAT)
                .serializationLib(LAZY_SIMPLE_SERDE)
                .columns(Collections.singletonList(col("c1", "int")))
                .partitionKeys(Collections.emptyList())
                .parameters(Collections.emptyMap());
    }

    private HiveWritePlanProvider providerFor(RecordingHmsClient client, RecordingConnectorContext ctx) {
        return new HiveWritePlanProvider(client, Collections.emptyMap(), ctx);
    }

    private WriteSession sessionFor(RecordingHmsClient client, RecordingConnectorContext ctx,
            Map<String, String> sessionProperties) {
        return new WriteSession(new HiveConnectorTransaction(42L, client, ctx), sessionProperties);
    }

    private THiveTableSink planSink(RecordingHmsClient client, RecordingConnectorContext ctx,
            WriteHandle handle) {
        return planSink(client, ctx, handle, Collections.emptyMap());
    }

    private THiveTableSink planSink(RecordingHmsClient client, RecordingConnectorContext ctx,
            WriteHandle handle, Map<String, String> sessionProperties) {
        ConnectorSinkPlan plan = providerFor(client, ctx)
                .planWrite(sessionFor(client, ctx, sessionProperties), handle);
        Assertions.assertEquals(TDataSinkType.HIVE_TABLE_SINK, plan.getDataSink().getType());
        return plan.getDataSink().getHiveTableSink();
    }

    private static WriteHandle handle() {
        return new WriteHandle(new HiveTableHandle(DB, TBL, HiveTableType.HIVE));
    }

    // ───────────────────────────── columns ─────────────────────────────

    @Test
    public void planWriteEmitsRegularColumnsThenPartitionKeys() {
        // BE writes the data columns then maps the trailing partition keys; a wrong tag or order would write
        // rows into the wrong file layout. Data cols are REGULAR (in schema order), partition keys are
        // PARTITION_KEY, appended last (HMS/HMSExternalTable order).
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder()
                .columns(Arrays.asList(col("c1", "int"), col("c2", "string")))
                .partitionKeys(Collections.singletonList(col("dt", "string")))
                .build();

        List<THiveColumn> columns = planSink(client, new RecordingConnectorContext(), handle()).getColumns();

        Assertions.assertEquals(3, columns.size());
        Assertions.assertEquals("c1", columns.get(0).getName());
        Assertions.assertEquals(THiveColumnType.REGULAR, columns.get(0).getColumnType());
        Assertions.assertEquals("c2", columns.get(1).getName());
        Assertions.assertEquals(THiveColumnType.REGULAR, columns.get(1).getColumnType());
        Assertions.assertEquals("dt", columns.get(2).getName());
        Assertions.assertEquals(THiveColumnType.PARTITION_KEY, columns.get(2).getColumnType(),
                "partition keys must be tagged PARTITION_KEY and appended after the data columns");
    }

    // ───────────────────────────── bucket ─────────────────────────────

    @Test
    public void planWriteSetsBucketInfo() {
        // The BE hive writer buckets rows by these columns into this many files; a dropped bucket spec would
        // write an un-bucketed layout that a bucketed reader then mis-reads.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder()
                .bucketCols(Collections.singletonList("c1"))
                .numBuckets(8)
                .build();

        THiveTableSink sink = planSink(client, new RecordingConnectorContext(), handle());

        Assertions.assertEquals(Collections.singletonList("c1"), sink.getBucketInfo().getBucketedBy());
        Assertions.assertEquals(8, sink.getBucketInfo().getBucketCount());
    }

    // ───────────────────────────── file format ─────────────────────────────

    @Test
    public void planWriteFileFormatPerInputFormat() {
        // The input-format class name decides the BE writer dialect: orc -> ORC, parquet -> PARQUET, and
        // (parity trap) text -> FORMAT_CSV_PLAIN. Writing the wrong container makes every row unreadable.
        Assertions.assertEquals(TFileFormatType.FORMAT_ORC, fileFormatFor(ORC_INPUT_FORMAT));
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, fileFormatFor(PARQUET_INPUT_FORMAT));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_PLAIN, fileFormatFor(TEXT_INPUT_FORMAT));
    }

    private TFileFormatType fileFormatFor(String inputFormat) {
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().inputFormat(inputFormat).build();
        return planSink(client, new RecordingConnectorContext(), handle()).getFileFormat();
    }

    // ───────────────────────────── compression ─────────────────────────────

    @Test
    public void planWriteCompressionPerFormat() {
        // The compression codec is read from the table parameters per format (orc.compress /
        // parquet.compression / text.compression). BE writes the file with this codec; a wrong codec yields
        // files a reader cannot decompress.
        Assertions.assertEquals(TFileCompressType.ZLIB,
                compressionFor(ORC_INPUT_FORMAT, Collections.singletonMap("orc.compress", "ZLIB"),
                        Collections.emptyMap()));
        Assertions.assertEquals(TFileCompressType.SNAPPYBLOCK,
                compressionFor(PARQUET_INPUT_FORMAT, Collections.singletonMap("parquet.compression", "snappy"),
                        Collections.emptyMap()));
        Assertions.assertEquals(TFileCompressType.GZ,
                compressionFor(TEXT_INPUT_FORMAT, Collections.singletonMap("text.compression", "gzip"),
                        Collections.emptyMap()));
    }

    @Test
    public void planWriteTextCompressionFallsBackToSessionDefault() {
        // A text table with no text.compression property falls back to the session hive_text_compression
        // default (legacy SessionVariable.hiveTextCompression); "uncompressed" is aliased to "plain".
        Assertions.assertEquals(TFileCompressType.ZSTD,
                compressionFor(TEXT_INPUT_FORMAT, Collections.emptyMap(),
                        Collections.singletonMap("hive_text_compression", "zstd")));
        Assertions.assertEquals(TFileCompressType.PLAIN,
                compressionFor(TEXT_INPUT_FORMAT, Collections.emptyMap(),
                        Collections.singletonMap("hive_text_compression", "uncompressed")));
    }

    private TFileCompressType compressionFor(String inputFormat, Map<String, String> tableParams,
            Map<String, String> sessionProperties) {
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().inputFormat(inputFormat).parameters(tableParams).build();
        return planSink(client, new RecordingConnectorContext(), handle(), sessionProperties).getCompressionType();
    }

    // ───────────────────────────── location ─────────────────────────────

    @Test
    public void planWriteLocationInPlaceForObjectStore() {
        // An object-store write goes in place: the write and target paths are the normalized (s3://) URI and
        // the original raw URI is preserved so BE can resolve credentials for the native scheme. No staging
        // dir — BE writes straight to the table location.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().location("oss://bucket/db/t").build();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.backendFileType = TFileType.FILE_S3;

        THiveLocationParams loc = planSink(client, ctx, handle()).getLocation();

        Assertions.assertEquals(TFileType.FILE_S3, loc.getFileType());
        Assertions.assertEquals("s3://bucket/db/t", loc.getWritePath(),
                "an object-store write path must be the normalized (s3://) URI BE opens");
        Assertions.assertEquals("s3://bucket/db/t", loc.getTargetPath());
        Assertions.assertEquals("oss://bucket/db/t", loc.getOriginalWritePath(),
                "the original raw URI must be preserved so BE can resolve creds for the native scheme");
    }

    @Test
    public void planWriteLocationStagingForHdfs() {
        // A non-object-store write goes to a staging temp dir under the table location; BE writes there and the
        // commit renames it to the target. write == original == staging; target == the raw table location.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().location("hdfs://ns/wh/db/t").build();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.backendFileType = TFileType.FILE_HDFS;

        THiveLocationParams loc = planSink(client, ctx, handle()).getLocation();

        Assertions.assertEquals(TFileType.FILE_HDFS, loc.getFileType());
        Assertions.assertEquals("hdfs://ns/wh/db/t", loc.getTargetPath(),
                "the target path must remain the live table location the commit renames into");
        Assertions.assertEquals(loc.getWritePath(), loc.getOriginalWritePath(),
                "a staged write has no scheme rewrite, so write and original paths are identical");
        Assertions.assertNotEquals("hdfs://ns/wh/db/t", loc.getWritePath(),
                "the staged write path must differ from the live table location");
        Assertions.assertTrue(loc.getWritePath().contains(".doris_staging"),
                "the staged write path must sit under the .doris_staging base dir; got: " + loc.getWritePath());
    }

    // ───────────────────────────── serde delimiters ─────────────────────────────

    @Test
    public void planWriteSerdeDefaultDelimiters() {
        // With no SerDe params BE must fall back to the hive text defaults; a wrong default silently mis-splits
        // every text row. escape is unset (no escape.delim), matching legacy's Optional-only emission.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().build();

        THiveTableSink sink = planSink(client, new RecordingConnectorContext(), handle());

        Assertions.assertEquals("\1", sink.getSerdeProperties().getFieldDelim());
        Assertions.assertEquals("\n", sink.getSerdeProperties().getLineDelim());
        Assertions.assertEquals("\2", sink.getSerdeProperties().getCollectionDelim());
        Assertions.assertEquals("\003", sink.getSerdeProperties().getMapkvDelim());
        Assertions.assertEquals("\\N", sink.getSerdeProperties().getNullFormat());
        Assertions.assertFalse(sink.getSerdeProperties().isSetEscapeChar(),
                "with no escape.delim the escape char must stay unset (legacy sets it only when present)");
    }

    @Test
    public void planWriteSerdeMultiDelimitKeepsMultiCharFieldDelim() {
        // The MultiDelimitSerDe lib is the ONLY case where a multi-char field delimiter is passed through
        // verbatim (not byte-decoded); collapsing "||" to one char would corrupt every row boundary.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder()
                .serializationLib(MULTI_DELIMIT_SERDE)
                .sdParameters(Collections.singletonMap("field.delim", "||"))
                .build();

        Assertions.assertEquals("||",
                planSink(client, new RecordingConnectorContext(), handle()).getSerdeProperties().getFieldDelim());
    }

    @Test
    public void planWriteSerdeNumericFieldDelimDecodesByte() {
        // A numeric field.delim like "9" is a byte value, decoded to that char ((9 + 256) % 256 -> char 9 =
        // TAB) — not the literal digit '9'. A missed decode would split on the wrong byte.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder()
                .sdParameters(Collections.singletonMap("field.delim", "9"))
                .build();

        Assertions.assertEquals("\t",
                planSink(client, new RecordingConnectorContext(), handle()).getSerdeProperties().getFieldDelim());
    }

    @Test
    public void planWriteSerdeTableParamWinsOverSdParam() {
        // getSerdeProperty is table-param-FIRST: a field.delim in the table parameters overrides the SD SerDe
        // parameters. A reversed precedence would pick the wrong delimiter for a table that carries both.
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("field.delim", "a");
        client.table = tableBuilder()
                .parameters(tableParams)
                .sdParameters(Collections.singletonMap("field.delim", "b"))
                .build();

        // "a" is non-numeric, so getByte returns its first raw char "a" (proves the table value, not "b", won).
        Assertions.assertEquals("a",
                planSink(client, new RecordingConnectorContext(), handle()).getSerdeProperties().getFieldDelim());
    }

    // ───────────────────────────── overwrite + promotion ─────────────────────────────

    @Test
    public void planWriteOverwriteFlag() {
        // The overwrite flag drives BE's truncate-and-insert vs append; a dropped flag turns an INSERT
        // OVERWRITE into a silent append (duplicated data).
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().build();
        Assertions.assertTrue(planSink(client, new RecordingConnectorContext(), handle().overwrite(true))
                .isOverwrite());

        RecordingHmsClient client2 = new RecordingHmsClient();
        client2.table = tableBuilder().build();
        Assertions.assertFalse(planSink(client2, new RecordingConnectorContext(), handle().overwrite(false))
                .isOverwrite());
    }

    @Test
    public void buildWriteContextPromotesOverwritingInsertToOverwrite() {
        // An overwriting INSERT is promoted to the OVERWRITE operation on the op-context (the transaction
        // classifies APPEND vs OVERWRITE off this); a plain INSERT stays INSERT.
        RecordingHmsClient client = new RecordingHmsClient();
        HmsTableInfo table = tableBuilder().build();
        client.table = table;
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        HiveWritePlanProvider provider = providerFor(client, ctx);
        ConnectorSession session = new WriteSession(null, Collections.emptyMap());
        HiveTableHandle tableHandle = new HiveTableHandle(DB, TBL, HiveTableType.HIVE);

        HiveWriteContext promoted = provider.buildWriteContext(session, tableHandle, table,
                new WriteHandle(tableHandle).overwrite(true));
        Assertions.assertEquals(WriteOperation.OVERWRITE, promoted.getWriteOperation(),
                "an overwriting INSERT must be promoted to OVERWRITE");

        HiveWriteContext plain = provider.buildWriteContext(session, tableHandle, table,
                new WriteHandle(tableHandle).overwrite(false));
        Assertions.assertEquals(WriteOperation.INSERT, plain.getWriteOperation(),
                "a non-overwriting INSERT must stay INSERT");
    }

    // ───────────────────────────── broker backend ─────────────────────────────

    @Test
    public void planWriteBrokerBackendResolvesAddresses() {
        // A broker-backed write must carry the resolved broker addresses, or BE gets a broker sink with an
        // empty broker list and the write fails.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().location("ofs://bucket/db/t").build();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.backendFileType = TFileType.FILE_BROKER;
        ctx.brokerAddresses = Collections.singletonList(new ConnectorBrokerAddress("h1", 8000));

        List<TNetworkAddress> brokers = planSink(client, ctx, handle()).getBrokerAddresses();

        Assertions.assertEquals(1, brokers.size());
        Assertions.assertEquals("h1", brokers.get(0).getHostname());
        Assertions.assertEquals(8000, brokers.get(0).getPort());
    }

    @Test
    public void planWriteBrokerBackendFailsLoudWhenNoBroker() {
        // No alive broker for a broker-backed write must fail loud rather than ship BE an empty broker list.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().location("ofs://bucket/db/t").build();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.backendFileType = TFileType.FILE_BROKER;

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(client, ctx, handle()));
        Assertions.assertTrue(ex.getMessage().contains("No alive broker"),
                "the broker resolution must fail loud with 'No alive broker.', got: " + ex.getMessage());
    }

    // ───────────────────────────── LZO reject ─────────────────────────────

    @Test
    public void planWriteRejectsLzoInputFormat() {
        // A Doris-written LZO file has no .lzo suffix while the LZO read path filters to *.lzo only, so every
        // written row would be permanently invisible. The write must be rejected loudly at plan time.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().inputFormat(LZO_INPUT_FORMAT).build();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(client, new RecordingConnectorContext(), handle()));
        Assertions.assertTrue(ex.getMessage().contains("LZO"),
                "the LZO reject must name LZO, got: " + ex.getMessage());
    }

    // ───────────────────────────── existing partitions ─────────────────────────────

    @Test
    public void planWritePartitionedTableEmitsExistingPartitions() {
        // For a partitioned table BE needs the existing partition dirs (to distinguish APPEND from NEW); the
        // list is fetched live. Each THivePartition carries its values, its own file format, and its in-place
        // location (write == target).
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder()
                .partitionKeys(Collections.singletonList(col("dt", "string")))
                .build();
        client.partitionNames = Collections.singletonList("dt=2024-01-01");
        client.partitions = Collections.singletonList(new HmsPartitionInfo(
                Collections.singletonList("2024-01-01"), "oss://bucket/db/t/dt=2024-01-01",
                PARQUET_INPUT_FORMAT, TEXT_OUTPUT_FORMAT, LAZY_SIMPLE_SERDE, Collections.emptyMap()));
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.backendFileType = TFileType.FILE_S3;

        List<THivePartition> partitions = planSink(client, ctx, handle()).getPartitions();

        Assertions.assertEquals(1, partitions.size());
        THivePartition part = partitions.get(0);
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), part.getValues());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, part.getFileFormat(),
                "each partition's file format is resolved from its own input format");
        Assertions.assertEquals("oss://bucket/db/t/dt=2024-01-01", part.getLocation().getWritePath());
        Assertions.assertEquals("oss://bucket/db/t/dt=2024-01-01", part.getLocation().getTargetPath(),
                "an existing partition is read in place, so its write and target paths are the same");
        Assertions.assertEquals(TFileType.FILE_S3, part.getLocation().getFileType());
        Assertions.assertTrue(client.calls.contains("listPartitionNames"),
                "the existing-partition list must be fetched live; calls=" + client.calls);
        Assertions.assertTrue(client.calls.stream().anyMatch(c -> c.startsWith("getPartitions")),
                "the existing-partition metadata must be fetched live; calls=" + client.calls);
    }

    @Test
    public void planWriteUnpartitionedTableEmitsNoPartitions() {
        // An unpartitioned table must emit an empty partition list (and never touch the partition-listing
        // client calls) — a spurious partition would confuse the BE commit classification.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().build();

        THiveTableSink sink = planSink(client, new RecordingConnectorContext(), handle());

        Assertions.assertTrue(sink.getPartitions().isEmpty());
        Assertions.assertFalse(client.calls.contains("listPartitionNames"),
                "an unpartitioned table must not list partitions; calls=" + client.calls);
    }

    // ───────────────────────────── hadoop config ─────────────────────────────

    @Test
    public void planWriteHadoopConfigFromStorageProperties() {
        // BE's S3 sink reads ONLY the AWS_* canonical creds; they are sourced from the typed fe-filesystem
        // StorageProperties (the same source the scan path uses). A dropped cred yields a 403 on a private
        // bucket.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().build();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.storageProperties = Collections.singletonList(
                fakeBackendStorage(Collections.singletonMap("AWS_ACCESS_KEY", "AK123")));

        Assertions.assertEquals("AK123",
                planSink(client, ctx, handle()).getHadoopConfig().get("AWS_ACCESS_KEY"));
    }

    @Test
    public void planWriteHadoopConfigIncludesBackendStoragePropertiesForUntypedFsSchemes() {
        // C1 regression (External Regression build 1000131, test_jfs_hms_catalog_read): the jfs/oss-hdfs
        // fe-filesystem plugins have no typed bind(), so getStorageProperties() is EMPTY for a jfs catalog and
        // fs.jfs.impl (+ juicefs.*) would be dropped from the BE writer hadoopConfig -> libhdfs fails
        // "No FileSystem for scheme jfs" on INSERT. buildHadoopConfig must ALSO merge
        // getBackendStorageProperties() — the SAME source the hive READ path (HiveScanPlanProvider) uses, which
        // carries the fs./juicefs.* passthrough. MUTATION: dropping the getBackendStorageProperties() merge from
        // buildHadoopConfig -> fs.jfs.impl absent -> red.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().build();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        // Typed storageProperties intentionally EMPTY (models a jfs catalog whose plugin has no typed binding);
        // the fs.<scheme>.impl passthrough is available only via the backend-storage source.
        ctx.backendStorageProperties = Collections.singletonMap("fs.jfs.impl", "io.juicefs.JuiceFileSystem");

        Assertions.assertEquals("io.juicefs.JuiceFileSystem",
                planSink(client, ctx, handle()).getHadoopConfig().get("fs.jfs.impl"),
                "the backend-storage passthrough (fs.<scheme>.impl) must reach the BE writer hadoopConfig");
    }

    // ───────────────────────────── no transaction ─────────────────────────────

    @Test
    public void planWriteFailsLoudWithoutTransaction() {
        // planWrite requires the executor-bound connector transaction; without it the write must fail loud
        // (the cutover wires the transaction onto the session).
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = tableBuilder().build();
        HiveWritePlanProvider provider = providerFor(client, new RecordingConnectorContext());

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(new WriteSession(null, Collections.emptyMap()), handle()));
        Assertions.assertTrue(ex.getMessage().contains("active connector transaction"),
                "expected the missing-transaction message, got: " + ex.getMessage());
    }

    // ───────────────────────────── test doubles ─────────────────────────────

    /** A fe-filesystem {@link StorageProperties} whose {@code toBackendProperties().toMap()} returns the given
     * BE-canonical map — mirrors how a real object-store binding hands BE creds to the connector. Adapted from
     * the iceberg write test. */
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

    /** A bound write request wrapping a {@link HiveTableHandle}; mirrors the engine's PluginDrivenWriteHandle. */
    private static final class WriteHandle implements ConnectorWriteHandle {
        private final ConnectorTableHandle tableHandle;
        private boolean overwrite;

        WriteHandle(ConnectorTableHandle tableHandle) {
            this.tableHandle = tableHandle;
        }

        WriteHandle overwrite(boolean v) {
            this.overwrite = v;
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
            return Collections.emptyMap();
        }
    }

    /** A session carrying the bound connector transaction and the session-variable overrides. */
    private static final class WriteSession implements ConnectorSession {
        private final ConnectorTransaction txn;
        private final Map<String, String> sessionProperties;

        WriteSession(ConnectorTransaction txn, Map<String, String> sessionProperties) {
            this.txn = txn;
            this.sessionProperties = sessionProperties;
        }

        @Override
        public Optional<ConnectorTransaction> getCurrentTransaction() {
            return Optional.ofNullable(txn);
        }

        @Override
        public Map<String, String> getSessionProperties() {
            return sessionProperties;
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
        public long getCatalogId() {
            return 0;
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
            return Collections.emptyMap();
        }
    }

    /** Recording {@link HmsClient} fake returning canned table/partition metadata and recording the calls the
     * provider (and the transaction's begin-guard) make. */
    private static final class RecordingHmsClient implements HmsClient {
        private final List<String> calls = new ArrayList<>();
        private HmsTableInfo table;
        private List<String> partitionNames = Collections.emptyList();
        private List<HmsPartitionInfo> partitions = Collections.emptyList();

        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException("getDatabase");
        }

        @Override
        public List<String> listTables(String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return table != null;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            calls.add("getTable:" + dbName + "." + tableName);
            if (table == null) {
                throw new UnsupportedOperationException("no canned table");
            }
            return table;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            calls.add("listPartitionNames");
            return partitionNames;
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            calls.add("getPartitions:" + partNames);
            return partitions;
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException("getPartition");
        }

        @Override
        public void close() {
        }
    }
}
