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

import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.write.ConnectorRowChangeStyle;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWriteDistribution;
import org.apache.doris.connector.spi.write.ConnectorWritePartitionField;
import org.apache.doris.connector.spi.write.ConnectorWritePartitionSpec;
import org.apache.doris.connector.spi.write.ConnectorWriteSortColumn;
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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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

    @Test
    public void variantDataWritesNeedUnshreddedFormatThreeParquetTables() {
        // A write of data files whose target holds a VARIANT, at the root or nested, as plain VARIANT or as the
        // VARIANT_COMPUTE_V2 carrier the engine hands to the write path, needs format-version 3 and unshredded
        // Parquet data files. A write without data files (DELETE, a delete-only MERGE) and a target without a
        // VARIANT are not checked. MUTATION: dropping the VARIANT_COMPUTE_V2 arm of containsVariant -> red.
        ConnectorType carrier = ConnectorType.of("VARIANT_COMPUTE_V2");
        ConnectorColumn id = new ConnectorColumn("id", ConnectorType.of("INT"), null, true, null);
        List<ConnectorColumn> variants = Arrays.asList(
                new ConnectorColumn("v", ConnectorType.of("VARIANT"), null, true, null),
                new ConnectorColumn("v", carrier, null, true, null),
                new ConnectorColumn("payload", ConnectorType.structOf(Collections.singletonList("nested"),
                        Collections.singletonList(carrier)), null, true, null));
        Map<String, String> unshredded = Collections.emptyMap();
        for (ConnectorColumn variant : variants) {
            List<ConnectorColumn> columns = Arrays.asList(id, variant);
            Assertions.assertDoesNotThrow(() -> IcebergWritePlanProvider.validateVariantWrite(
                    columns, true, 3, FileFormat.PARQUET, unshredded), variant.getType().toString());
            assertVariantWriteRejected(columns, 2, FileFormat.PARQUET, unshredded,
                    "Iceberg VARIANT writes require table format-version 3, but found 2");
            assertVariantWriteRejected(columns, 3, FileFormat.ORC, unshredded,
                    "Iceberg VARIANT writes require Parquet data files, but found ORC");
            assertVariantWriteRejected(columns, 3, FileFormat.PARQUET,
                    Collections.singletonMap("write.parquet.shred-variants", "true"),
                    "Doris currently supports only unshredded Iceberg VARIANT writes");
            Assertions.assertDoesNotThrow(() -> IcebergWritePlanProvider.validateVariantWrite(
                    columns, false, 2, FileFormat.ORC, unshredded), variant.getType().toString());
        }
        Assertions.assertDoesNotThrow(() -> IcebergWritePlanProvider.validateVariantWrite(
                Collections.singletonList(id), true, 2, FileFormat.ORC, unshredded));
    }

    private static void assertVariantWriteRejected(List<ConnectorColumn> columns, int formatVersion,
            FileFormat fileFormat, Map<String, String> writerProperties, String message) {
        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergWritePlanProvider.validateVariantWrite(
                        columns, true, formatVersion, fileFormat, writerProperties));
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }

    @Test
    public void planWriteWritesVariantsOnlyToFormatThreeParquetTables() {
        // An INSERT whose column list omits the VARIANT column still binds the whole target schema, so the table it
        // writes is checked. A v3 Parquet table takes the write, with the VARIANT in the schema JSON BE parses and
        // column metrics collected for it although it is the only column with metrics, while a v3 ORC table and a
        // shredding Parquet table reject it. MUTATION: collecting metrics only for primitive fields -> red.
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.VariantType.get()));
        InMemoryCatalog catalog = freshCatalog();
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "3");
        props.put("write.format.default", "parquet");
        props.put("write.data.path", "oss://bucket/wh/db1/variants/data");
        props.put("write.metadata.metrics.default", "none");
        props.put("write.metadata.metrics.column.payload", "counts");
        Table parquet = catalog.createTable(TableIdentifier.of("db1", "variants"), schema,
                PartitionSpec.unpartitioned(), props);
        TIcebergTableSink sink = planSink(parquet, contextWithStorage(), variantInsert(parquet, "variants"));
        Assertions.assertTrue(sink.getSchemaJson().contains("\"type\":\"variant\""), sink.getSchemaJson());
        Assertions.assertTrue(sink.isCollectColumnStats());

        props.put("write.format.default", "orc");
        Table orc = catalog.createTable(TableIdentifier.of("db1", "orc_variants"), schema,
                PartitionSpec.unpartitioned(), props);
        DorisConnectorException orcError = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(orc, contextWithStorage(), variantInsert(orc, "orc_variants")));
        Assertions.assertTrue(orcError.getMessage().contains("require Parquet data files, but found ORC"),
                orcError.getMessage());

        props.put("write.format.default", "parquet");
        props.put("write.parquet.shred-variants", "true");
        Table shredded = catalog.createTable(TableIdentifier.of("db1", "shredded_variants"), schema,
                PartitionSpec.unpartitioned(), props);
        DorisConnectorException shredError = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(shredded, contextWithStorage(), variantInsert(shredded, "shredded_variants")));
        Assertions.assertTrue(shredError.getMessage().contains("unshredded"), shredError.getMessage());
    }

    /** An INSERT into {@code id} only, bound to the whole write schema of the table. */
    private static WriteHandle variantInsert(Table table, String tableName) {
        RecordingConnectorContext context = contextWithStorage();
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", tableName);
        List<ConnectorColumn> columns = providerFor(table, context).getWriteColumns(
                sessionFor(table, context), tableHandle, Optional.empty()).orElseThrow(AssertionError::new);
        return new WriteHandle(tableHandle).columns(columns.subList(0, 1)).boundTargetColumns(columns);
    }

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
        return new IcebergWritePlanProvider(IcebergCatalogProperties.of(NON_REST_PROPS), ops, ctx);
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
        return planSink(table, ctx, handle, NON_REST_PROPS);
    }

    private static TIcebergTableSink planSink(Table table, RecordingConnectorContext ctx,
            ConnectorWriteHandle handle, Map<String, String> properties) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        ConnectorSinkPlan plan = new IcebergWritePlanProvider(IcebergCatalogProperties.of(properties), ops, ctx)
                .planWrite(sessionFor(table, ctx), handle);
        Assertions.assertEquals(TDataSinkType.ICEBERG_TABLE_SINK, plan.getDataSink().getType());
        return plan.getDataSink().getIcebergTableSink();
    }

    @Test
    public void getWriteColumnsCarriesPinnedTypedDefaultsAndNullability() {
        Schema writeSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional("value")
                        .withId(2).ofType(Types.IntegerType.get()).withWriteDefault(42).build(),
                Types.NestedField.optional("text")
                        .withId(3).ofType(Types.StringType.get()).withWriteDefault("O'Reilly").build(),
                Types.NestedField.optional("windows_path")
                        .withId(7).ofType(Types.StringType.get()).withWriteDefault("C:\\new").build(),
                Types.NestedField.optional("payload")
                        .withId(4).ofType(Types.BinaryType.get())
                        .withWriteDefault(ByteBuffer.wrap(new byte[] {0x00, 0x0f, (byte) 0xff})).build(),
                Types.NestedField.optional(5, "nullable_value", Types.IntegerType.get()),
                Types.NestedField.required(6, "required_value", Types.IntegerType.get()),
                Types.NestedField.optional(8, "uuid_value", Types.UUIDType.get()),
                Types.NestedField.optional(9, "uuid_array", Types.ListType.ofOptional(10, Types.UUIDType.get())));
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "defaults"), writeSchema,
                PartitionSpec.unpartitioned());
        RecordingConnectorContext context = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, context);

        List<ConnectorColumn> writeColumns = provider.getWriteColumns(
                sessionFor(table, context), new IcebergTableHandle("db1", "defaults"),
                Optional.empty()).orElseThrow(AssertionError::new);
        Map<String, ConnectorColumn> columns = new HashMap<>();
        for (ConnectorColumn column : writeColumns) {
            columns.put(column.getName(), column);
        }

        Assertions.assertEquals(ConnectorType.of("UUID"), columns.get("uuid_value").getStringWriteType());
        Assertions.assertEquals(ConnectorType.of("UUID"), columns.get("uuid_value").getType());
        Assertions.assertNull(columns.get("payload").getStringWriteType());
        Assertions.assertEquals(ConnectorType.of("UUID"),
                columns.get("uuid_array").getStringWriteType().getChildren().get(0));
        Assertions.assertFalse(columns.get("id").isNullable());
        Assertions.assertEquals("42", columns.get("value").getDefaultValueSql());
        Assertions.assertEquals("'O''Reilly'", columns.get("text").getDefaultValueSql());
        Assertions.assertEquals("UNHEX('433A5C6E6577')",
                columns.get("windows_path").getDefaultValueSql());
        Assertions.assertEquals("X'000FFF'", columns.get("payload").getDefaultValueSql());
        Assertions.assertEquals("NULL", columns.get("nullable_value").getDefaultValueSql());
        Assertions.assertNull(columns.get("required_value").getDefaultValueSql());
        for (ConnectorColumn column : writeColumns) {
            Assertions.assertNull(column.getDefaultValue(),
                    "request-scoped Iceberg defaults must not leak into cached metadata");
        }
    }

    @Test
    public void floatingPointDefaultsUseTypedSqlForNonFiniteValuesRecursively() {
        Assertions.assertEquals("CAST('NaN' AS FLOAT)",
                IcebergWriteSchemaContext.toDorisSql(
                        Types.FloatType.get(), Float.NaN, false, false));
        Assertions.assertEquals("CAST('Infinity' AS FLOAT)",
                IcebergWriteSchemaContext.toDorisSql(
                        Types.FloatType.get(), Float.POSITIVE_INFINITY, false, false));
        Assertions.assertEquals("CAST('-Infinity' AS DOUBLE)",
                IcebergWriteSchemaContext.toDorisSql(
                        Types.DoubleType.get(), Double.NEGATIVE_INFINITY, false, false));

        Types.StructType nestedType = Types.StructType.of(
                Types.NestedField.optional(20, "float_value", Types.FloatType.get()),
                Types.NestedField.optional(21, "double_values",
                        Types.ListType.ofOptional(22, Types.DoubleType.get())));
        GenericRecord nestedDefault = GenericRecord.create(nestedType);
        nestedDefault.setField("float_value", Float.NaN);
        nestedDefault.setField("double_values",
                Arrays.asList(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));

        Assertions.assertEquals(
                "named_struct('float_value', CAST('NaN' AS FLOAT), "
                        + "'double_values', array(CAST('Infinity' AS DOUBLE), "
                        + "CAST('-Infinity' AS DOUBLE)))",
                IcebergWriteSchemaContext.toDorisSql(nestedType, nestedDefault, false, false));
    }

    @Test
    public void primitiveWriteDefaultsBecomeTypedSqlLiterals() {
        // Each primitive write-default reaches the engine as SQL of the column's own type. The initial-default only
        // describes rows written before the field existed, so it must never become the value of a new row.
        Schema schema = new Schema(
                defaultField(1, "boolean_col", Types.BooleanType.get(), true, false),
                defaultField(2, "int_col", Types.IntegerType.get(), 34, 35),
                defaultField(3, "long_col", Types.LongType.get(), 4_900_000_000L, 4_900_000_001L),
                defaultField(4, "float_col", Types.FloatType.get(), 12.25F, 13.5F),
                defaultField(5, "double_col", Types.DoubleType.get(), -123.5D, 456.75D),
                defaultField(6, "decimal_col", Types.DecimalType.of(20, 4),
                        new BigDecimal("12345.6789"), new BigDecimal("98765.4321")),
                defaultField(7, "date_col", Types.DateType.get(),
                        DateTimeUtil.isoDateToDays("2024-12-17"), DateTimeUtil.isoDateToDays("2025-01-18")),
                defaultField(8, "timestamp_col", Types.TimestampType.withoutZone(),
                        DateTimeUtil.isoTimestampToMicros("2024-12-17T23:59:59.123456"),
                        DateTimeUtil.isoTimestampToMicros("2025-01-18T01:02:03.654321")),
                defaultField(9, "timestamptz_col", Types.TimestampType.withZone(),
                        DateTimeUtil.isoTimestamptzToMicros("2024-12-17T23:59:59.123456+00:00"),
                        DateTimeUtil.isoTimestamptzToMicros("2025-01-18T01:02:03.654321+00:00")),
                defaultField(10, "string_col", Types.StringType.get(), "initial-default", "write-default"),
                defaultField(11, "uuid_col", Types.UUIDType.get(),
                        UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                        UUID.fromString("123e4567-e89b-12d3-a456-426614174001")),
                defaultField(12, "fixed_col", Types.FixedType.ofLength(4),
                        ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03, 0x04}),
                        ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})),
                defaultField(13, "binary_col", Types.BinaryType.get(),
                        ByteBuffer.wrap(new byte[] {0x05, 0x06}), ByteBuffer.wrap(new byte[] {0x0e, 0x0f})),
                Types.NestedField.optional(14, "optional_col", Types.IntegerType.get()),
                Types.NestedField.required(15, "required_col", Types.IntegerType.get()));
        Table table = freshCatalog().createTable(TableIdentifier.of("db1", "typed_defaults"), schema,
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"));
        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.typed_defaults", Optional.empty(), false, false);
        Map<String, ConnectorColumn> columns = new HashMap<>();
        for (ConnectorColumn column : writeContext.getColumns()) {
            columns.put(column.getName(), column);
        }

        assertWriteDefault(columns.get("boolean_col"), ConnectorType.of("BOOLEAN"), "false");
        assertWriteDefault(columns.get("int_col"), ConnectorType.of("INT"), "35");
        assertWriteDefault(columns.get("long_col"), ConnectorType.of("BIGINT"), "4900000001");
        assertWriteDefault(columns.get("float_col"), ConnectorType.of("FLOAT"), "13.5");
        assertWriteDefault(columns.get("double_col"), ConnectorType.of("DOUBLE"), "456.75");
        assertWriteDefault(columns.get("decimal_col"), ConnectorType.of("DECIMALV3", 20, 4), "98765.4321");
        assertWriteDefault(columns.get("date_col"), ConnectorType.of("DATEV2"), "'2025-01-18'");
        assertWriteDefault(columns.get("timestamp_col"), ConnectorType.of("DATETIMEV2", 6, 0),
                "'2025-01-18 01:02:03.654321'");
        assertWriteDefault(columns.get("timestamptz_col"), ConnectorType.of("TIMESTAMPTZ", 6, 0),
                "'2025-01-18 01:02:03.654321+00:00'");
        assertWriteDefault(columns.get("string_col"), ConnectorType.of("STRING"), "'write-default'");
        assertWriteDefault(columns.get("uuid_col"), ConnectorType.of("UUID"),
                "'123e4567-e89b-12d3-a456-426614174001'");
        assertWriteDefault(columns.get("fixed_col"), ConnectorType.of("VARBINARY", 4, 0), "X'0A0B0C0D'");
        assertWriteDefault(columns.get("binary_col"), ConnectorType.of("VARBINARY"), "X'0E0F'");
        assertWriteDefault(columns.get("optional_col"), ConnectorType.of("INT"), "NULL");
        assertWriteDefault(columns.get("required_col"), ConnectorType.of("INT"), null);
        Assertions.assertFalse(columns.get("string_col").isNullable());
        Assertions.assertEquals(table.schema().schemaId(), writeContext.getSchema().schemaId());
        Assertions.assertEquals(SchemaParser.toJson(table.schema()), writeContext.getSchemaJson());
        Assertions.assertTrue(writeContext.getMergeSchemaJson().contains("_row_id"));
    }

    private static Types.NestedField defaultField(int id, String name, Type type,
            Object initialDefault, Object writeDefault) {
        return Types.NestedField.required(name).withId(id).ofType(type)
                .withInitialDefault(initialDefault).withWriteDefault(writeDefault).build();
    }

    private static void assertWriteDefault(ConnectorColumn column, ConnectorType type, String defaultSql) {
        Assertions.assertEquals(type, column.getType(), column.getName());
        Assertions.assertEquals(defaultSql, column.getDefaultValueSql(), column.getName());
    }

    @Test
    public void complexWriteDefaultsRenderMapsAndNullElementsRecursively() {
        // A complex write-default keeps every nested NULL in place and renders maps entry by entry.
        Types.StructType structType = Types.StructType.of(
                Types.NestedField.required(101, "count", Types.IntegerType.get()),
                Types.NestedField.optional(102, "items", Types.ListType.ofOptional(103, Types.StringType.get())),
                Types.NestedField.optional(104, "attributes",
                        Types.MapType.ofOptional(105, 106, Types.StringType.get(), Types.IntegerType.get())));
        Map<String, Integer> attributes = new LinkedHashMap<>();
        attributes.put("one", 1);
        attributes.put("none", null);
        GenericRecord structDefault = GenericRecord.create(structType);
        structDefault.setField("count", 7);
        structDefault.setField("items", Arrays.asList("a", null));
        structDefault.setField("attributes", attributes);

        Assertions.assertEquals("named_struct('count', 7, 'items', array('a', NULL), "
                        + "'attributes', map('one', 1, 'none', NULL))",
                IcebergWriteSchemaContext.toDorisSql(structType, structDefault, false, false));
    }

    @Test
    public void zonedAndNestedBinaryWriteDefaultsIgnoreLegacyMappingFlags() {
        // A zoned default keeps its instant, and a nested binary leaf stays bytes, with the old flags off.
        Assertions.assertEquals("'2025-01-18 01:02:03.654321+00:00'", IcebergWriteSchemaContext.toDorisSql(
                Types.TimestampType.withZone(), 1_737_162_123_654_321L, false, false));
        Types.StructType structType = Types.StructType.of(
                Types.NestedField.required(101, "payload", Types.BinaryType.get()));
        GenericRecord structDefault = GenericRecord.create(structType);
        structDefault.setField("payload", ByteBuffer.wrap(new byte[] {(byte) 0x80, 0x00, (byte) 0xff}));
        Assertions.assertEquals("named_struct('payload', X'8000FF')",
                IcebergWriteSchemaContext.toDorisSql(structType, structDefault, false, false));
    }

    @Test
    public void variantWriteDefaultsMustBeNull() {
        // An optional VARIANT column without a write-default writes NULL and a required one has no default, while a
        // VARIANT write-default other than NULL is rejected; a STRUCT default keeps its VARIANT child NULL.
        Schema schema = new Schema(
                Types.NestedField.optional(1, "optional_payload", Types.VariantType.get()),
                Types.NestedField.required(2, "required_payload", Types.VariantType.get()));
        Table table = freshCatalog().createTable(TableIdentifier.of("db1", "variant_defaults"), schema,
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"));
        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.variant_defaults", Optional.empty(), false, false);
        Assertions.assertEquals("NULL", writeContext.getColumns().get(0).getDefaultValueSql());
        Assertions.assertNull(writeContext.getColumns().get(1).getDefaultValueSql());

        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergWriteSchemaContext.toDorisSql(Types.VariantType.get(), "{}", false, false));
        Assertions.assertTrue(exception.getMessage().contains("Iceberg VARIANT write-default must be NULL"),
                exception.getMessage());

        Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(3, "payload", Types.VariantType.get()));
        GenericRecord structDefault = GenericRecord.create(structType);
        structDefault.set(0, null);
        Assertions.assertEquals("named_struct('payload', NULL)",
                IcebergWriteSchemaContext.toDorisSql(structType, structDefault, false, false));
    }

    @Test
    public void branchWriteUsesTableCurrentSchema() {
        // A branch only selects the parent snapshot. Iceberg stamps a branch commit with the table-current
        // schema and Spark binds branch writes to it, so a branch created before a rename writes the new name.
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        table.newAppend().commit();
        table.manageSnapshots().createBranch("old_schema", table.currentSnapshot().snapshotId()).commit();
        table.updateSchema().renameColumn("name", "renamed_name").commit();

        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.t2", Optional.of("old_schema"), false, false);

        Assertions.assertEquals(table.schema().schemaId(), writeContext.getSchema().schemaId());
        Assertions.assertNotNull(writeContext.getSchema().findField("renamed_name"));
        Assertions.assertNull(writeContext.getSchema().findField("name"));
        Assertions.assertDoesNotThrow(() -> writeContext.validateCurrentSchema(table, false));
    }

    @Test
    public void planWriteBindsBranchColumnsToTableCurrentSchema() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        table.newAppend().commit();
        table.manageSnapshots().createBranch("old_schema", table.currentSnapshot().snapshotId()).commit();
        table.updateSchema().renameColumn("name", "renamed_name").commit();
        RecordingConnectorContext context = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, context);
        WriteSession session = sessionFor(table, context);
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "t2");
        List<ConnectorColumn> branchColumns = provider.getWriteColumns(
                session, tableHandle, Optional.of("old_schema")).orElseThrow(AssertionError::new);

        Assertions.assertEquals(Arrays.asList("id", "renamed_name"),
                branchColumns.stream().map(ConnectorColumn::getName).collect(Collectors.toList()));
        ConnectorSinkPlan plan = Assertions.assertDoesNotThrow(() -> provider.planWrite(session,
                new WriteHandle(tableHandle).branch("old_schema").boundTargetColumns(branchColumns)));

        Assertions.assertEquals(SchemaParser.toJson(table.schema()),
                plan.getDataSink().getIcebergTableSink().getSchemaJson());
    }

    @Test
    public void writeSchemaAllowsNestedPrimitivePartitionSource() {
        InMemoryCatalog catalog = freshCatalog();
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "part", Types.IntegerType.get()))));
        Table table = catalog.createTable(TableIdentifier.of("db1", "nested_partition"), schema,
                PartitionSpec.builderFor(schema).identity("payload.part").build());

        Assertions.assertDoesNotThrow(() -> IcebergWriteSchemaContext.create(
                table, "db1.nested_partition", Optional.empty(), false, false));
    }

    /** A v1 table whose region partition field was dropped, which Iceberg keeps as void(region). */
    private static Table v1TableWithVoidRegion(InMemoryCatalog catalog, String name, boolean identityCategory,
            boolean dropRegion) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "region", Types.StringType.get()),
                Types.NestedField.optional(3, "category", Types.StringType.get()));
        PartitionSpec.Builder spec = PartitionSpec.builderFor(schema).identity("region");
        if (identityCategory) {
            spec.identity("category");
        }
        Table table = catalog.createTable(TableIdentifier.of("db1", name), schema, spec.build(),
                Collections.singletonMap("format-version", "1"));
        table.updateSpec().removeField("region").commit();
        if (dropRegion) {
            table.updateSchema().deleteColumn("region").commit();
        }
        return table;
    }

    @Test
    public void writeSchemaPinsV1VoidPartitionFields() {
        // Spark keeps writing a v1 table after a partition field becomes void, also once its source is dropped.
        // MUTATION: pinning an unpartitioned stand-in for a void-only spec -> the revalidation rejects it; checking
        // the source column of a void field -> create rejects the dropped region -> red.
        InMemoryCatalog catalog = freshCatalog();
        List<Table> tables = Arrays.asList(
                v1TableWithVoidRegion(catalog, "void_only", false, false),
                v1TableWithVoidRegion(catalog, "void_only_dropped", false, true),
                v1TableWithVoidRegion(catalog, "void_and_identity_dropped", true, true));
        for (Table table : tables) {
            IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                    table, table.name(), Optional.empty(), false, false);

            Assertions.assertEquals(PartitionSpecParser.toJson(table.spec()), writeContext.getPartitionSpecJson(),
                    table.name());
            Assertions.assertDoesNotThrow(() -> writeContext.validateCurrentSchema(table, true), table.name());
        }
    }

    @Test
    public void writeRoutingSkipsV1VoidPartitionFields() {
        // A void field is NULL for every row, so routing by the other fields still keeps one partition on one
        // writer, and the void field's source may be gone. MUTATION: routing the void field -> no route column for
        // the dropped region -> random write -> red.
        Table table = v1TableWithVoidRegion(freshCatalog(), "void_and_identity", true, true);
        IcebergTableHandle handle = new IcebergTableHandle("db1", "void_and_identity");

        ConnectorWriteDistribution distribution = providerFor(table, contextWithStorage())
                .getWriteDistribution(sessionFor(table, contextWithStorage()), handle);
        Assertions.assertNotNull(distribution);
        Assertions.assertEquals(Collections.singletonList("category"), distribution.getRouteColumns());
        Assertions.assertEquals(Collections.singletonMap("transforms", "identity"),
                distribution.getPartitionFunctionOptions());

        ConnectorWritePartitionSpec partitioning = providerFor(table, contextWithStorage())
                .getWritePartitioning(sessionFor(table, contextWithStorage()), handle);
        Assertions.assertEquals(table.spec().specId(), partitioning.getSpecId());
        Assertions.assertEquals(1, partitioning.getFields().size());
        Assertions.assertEquals("category", partitioning.getFields().get(0).getSourceColumnName());
    }

    @Test
    public void branchWriteRejectsTagOrMissingRef() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        table.newAppend().commit();
        table.manageSnapshots().createTag("old_tag", table.currentSnapshot().snapshotId()).commit();

        DorisConnectorException tagException = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergWriteSchemaContext.create(table, "db1.t2", Optional.of("old_tag"), false, false));
        Assertions.assertTrue(tagException.getMessage().contains(
                "old_tag is a tag, not a branch. Tags cannot be targets for producing snapshots"),
                tagException.getMessage());
        DorisConnectorException missingException = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergWriteSchemaContext.create(table, "db1.t2", Optional.of("no_such_ref"), false, false));
        Assertions.assertTrue(missingException.getMessage().contains("no_such_ref is not founded in db1.t2"),
                missingException.getMessage());
    }

    @Test
    public void branchWriteBindsCurrentRequiredFieldWithoutDefault() {
        // A required column added on main after the branch is part of the branch write. It has no default, so
        // omitting it fails at analysis exactly as it does for a write to main.
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        table.newAppend().commit();
        table.manageSnapshots().createBranch("old_schema", table.currentSnapshot().snapshotId()).commit();
        table.updateSchema().allowIncompatibleChanges()
                .addRequiredColumn("required_value", Types.IntegerType.get()).commit();

        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.t2", Optional.of("old_schema"), false, false);

        ConnectorColumn requiredValue = writeColumn(writeContext, "required_value");
        Assertions.assertFalse(requiredValue.isNullable());
        Assertions.assertNull(requiredValue.getDefaultValueSql());
        Assertions.assertDoesNotThrow(() -> writeContext.validateCurrentSchema(table, false));
    }

    @Test
    public void branchWriteUsesTableCurrentWriteDefaults() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = formatVersionThreeTable(catalog);
        table.updateSchema().addColumn("default_int", Types.IntegerType.get(), Literal.of(35)).commit();
        table.newAppend().commit();
        table.manageSnapshots().createBranch("old_defaults", table.currentSnapshot().snapshotId()).commit();
        table.updateSchema().updateColumnDefault("default_int", Literal.of(36))
                .addRequiredColumn("required_value", Types.IntegerType.get(), Literal.of(7)).commit();

        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.tv3", Optional.of("old_defaults"), false, false);

        Assertions.assertEquals("36", writeColumn(writeContext, "default_int").getDefaultValueSql());
        Assertions.assertEquals("7", writeColumn(writeContext, "required_value").getDefaultValueSql());
        Assertions.assertDoesNotThrow(() -> writeContext.validateCurrentSchema(table, false));
    }

    @Test
    public void branchWriteBindsPartitionSpecToTableCurrentSchema() {
        // The current spec partitions by a column the branch head schema never had.
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        table.newAppend().commit();
        table.manageSnapshots().createBranch("old_schema", table.currentSnapshot().snapshotId()).commit();
        table.updateSchema().addColumn("region", Types.StringType.get()).commit();
        table.updateSpec().addField("region").commit();

        IcebergWriteSchemaContext writeContext = Assertions.assertDoesNotThrow(() -> IcebergWriteSchemaContext.create(
                table, "db1.t2", Optional.of("old_schema"), false, false));

        Assertions.assertEquals(table.spec().specId(), writeContext.getPartitionSpec().specId());
        Assertions.assertEquals(table.schema().findField("region").fieldId(),
                writeContext.getPartitionSpec().fields().get(0).sourceId());
    }

    @Test
    public void branchWriteRejectsTableSchemaChangeBeforeCommit() {
        // The branch head schema stays the same, but the files were written with the pinned current schema, so
        // any later change to the table-current schema must fail the commit, as it does for a write to main.
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        table.newAppend().commit();
        table.manageSnapshots().createBranch("old_schema", table.currentSnapshot().snapshotId()).commit();
        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.t2", Optional.of("old_schema"), false, false);
        int pinnedSchemaId = table.schema().schemaId();
        table.updateSchema().addColumn("late_value", Types.IntegerType.get()).commit();

        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> writeContext.validateCurrentSchema(table, false));

        Assertions.assertTrue(exception.getMessage().contains("pinned schema " + pinnedSchemaId + "/format"),
                exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains(
                "current schema " + table.schema().schemaId() + "/format"), exception.getMessage());
    }

    private static ConnectorColumn writeColumn(IcebergWriteSchemaContext writeContext, String name) {
        return writeContext.getColumns().stream()
                .filter(column -> column.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing write column " + name));
    }

    @Test
    public void pinnedWriterMetadataRejectsSchemaFormatSpecAndSortOrderDrift() {
        // Pinned writer metadata stays valid only while its schema generation, format version, partition spec and
        // sort order are still the table's; a spec or sort order that is gone, or reused with another definition,
        // would label the written files with metadata they were not written with.
        Table table = partitionedSortedTable(freshCatalog());
        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.t1", Optional.empty(), false, false);
        Assertions.assertDoesNotThrow(() -> writeContext.validateCurrentSchema(table, false));

        assertWriterMetadataDrift(writeContext, withMetadata(table, "specs", Collections.emptyMap()),
                "partition spec changed during write planning");
        PartitionSpec redefinedSpec = PartitionSpec.builderFor(table.schema())
                .withSpecId(table.spec().specId()).identity("name").build();
        assertWriterMetadataDrift(writeContext, withMetadata(table, "specs",
                Collections.singletonMap(redefinedSpec.specId(), redefinedSpec)),
                "partition spec changed during write planning");
        assertWriterMetadataDrift(writeContext, withMetadata(table, "sortOrders", Collections.emptyMap()),
                "sort order changed during write planning");

        table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
        assertWriterMetadataDrift(writeContext, table, "schema changed during write planning");
        assertWriterMetadataDrift(writeContext, table, "/format 3");
        table.updateSchema().addColumn("added_after_planning", Types.IntegerType.get()).commit();
        assertWriterMetadataDrift(writeContext, table, "current schema " + table.schema().schemaId() + "/");
    }

    private static void assertWriterMetadataDrift(IcebergWriteSchemaContext writeContext, Table current,
            String expectedMessage) {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> writeContext.validateCurrentSchema(current, false));
        Assertions.assertTrue(ex.getMessage().contains(expectedMessage), ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("retry the statement"), ex.getMessage());
    }

    /** A view of {@code table} whose {@code method} returns {@code value}; every other call reaches the table. */
    private static Table withMetadata(Table table, String method, Object value) {
        return (Table) Proxy.newProxyInstance(Table.class.getClassLoader(),
                new Class<?>[] {Table.class, HasTableOperations.class}, (proxy, invoked, args) -> {
                    if (method.equals(invoked.getName())) {
                        return value;
                    }
                    try {
                        return invoked.invoke(table, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    public void v1MetadataIdentityAcceptsRetainedAncestorAndRejectsReplacement() {
        // A V1 table without a UUID is identified by its metadata file and timestamp: a later commit that keeps the
        // pinned file in its metadata log is the same table, a reused file name with another timestamp is not.
        String pinnedFile = "file:///tmp/v1_table/metadata/v1.metadata.json";
        MutableMetadataOperations operations =
                new MutableMetadataOperations(v1Metadata(pinnedFile, 1234L, null, 0L));
        Assertions.assertNull(operations.current().uuid());
        Table table = new BaseTable(operations, "db1.v1_table");
        IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.create(
                table, "db1.v1_table", Optional.empty(), false, false);
        Assertions.assertDoesNotThrow(() -> writeContext.validateCurrentSchema(table, false));

        operations.metadata = v1Metadata("file:///tmp/v1_table/metadata/v2.metadata.json", 2345L,
                pinnedFile, 1234L);
        Assertions.assertDoesNotThrow(() -> writeContext.validateCurrentSchema(table, false));

        operations.metadata = v1Metadata(pinnedFile, 3456L, null, 0L);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> writeContext.validateCurrentSchema(table, false));
        Assertions.assertTrue(ex.getMessage().contains("identity changed"), ex.getMessage());
    }

    /** V1 metadata without a table UUID, as written before Iceberg assigned one to every table. */
    private static TableMetadata v1Metadata(String metadataFile, long lastUpdatedMillis,
            String previousFile, long previousTimestampMillis) {
        String metadataLog = previousFile == null ? "[]" : "[{\"timestamp-ms\":" + previousTimestampMillis
                + ",\"metadata-file\":\"" + previousFile + "\"}]";
        return TableMetadataParser.fromJson(metadataFile, "{\"format-version\":1,"
                + "\"location\":\"file:///tmp/v1_table\","
                + "\"last-updated-ms\":" + lastUpdatedMillis + ","
                + "\"last-column-id\":1,"
                + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
                + "{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"int\"}]},"
                + "\"partition-spec\":[],"
                + "\"properties\":{\"write.format.default\":\"parquet\"},"
                + "\"current-snapshot-id\":-1,"
                + "\"snapshots\":[],"
                + "\"metadata-log\":" + metadataLog + "}");
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

    @Test
    public void planWriteValidatesFullBoundSchemaForPartialInsert() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        List<ConnectorColumn> fullBoundSchema = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "", false, null)
                        .withUniqueId(table.schema().findField("id").fieldId()),
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)
                        .withUniqueId(table.schema().findField("name").fieldId()));
        WriteHandle handle = new WriteHandle(new IcebergTableHandle("db1", "t2"))
                .columns(Collections.singletonList(fullBoundSchema.get(0)))
                .boundTargetColumns(fullBoundSchema);

        TIcebergTableSink sink = planSink(table, contextWithStorage(), handle);

        Assertions.assertEquals(SchemaParser.toJson(table.schema()), sink.getSchemaJson());
    }

    @Test
    public void planWriteAcceptsCanonicalEquivalentScalarDefaults() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        List<ConnectorColumn> boundSchema = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT", 0, 0), "", false, null)
                        .withUniqueId(table.schema().findField("id").fieldId()),
                new ConnectorColumn("name", ConnectorType.of("STRING", 0, 0), "", true, null)
                        .withUniqueId(table.schema().findField("name").fieldId()));

        // Doris scalar defaults are an encoding detail, not an Iceberg schema change. Rejecting this exact
        // production conversion shape broke every ordinary Iceberg INSERT in external regression.
        Assertions.assertDoesNotThrow(() -> planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t2")).boundTargetColumns(boundSchema)));
    }

    @Test
    public void planWriteAcceptsReadFacingTopLevelNullability() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        List<ConnectorColumn> boundSchema = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "", true, null)
                        .withUniqueId(table.schema().findField("id").fieldId()),
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)
                        .withUniqueId(table.schema().findField("name").fieldId()));

        // Iceberg exposes required top-level fields as nullable Doris scan columns so schema-evolution
        // default fill can still produce NULL. The write validator must not treat that read contract as drift.
        Assertions.assertDoesNotThrow(() -> planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t2")).boundTargetColumns(boundSchema)));
    }

    @Test
    public void planWriteAcceptsDorisPhysicalScalarAliases() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "amount", Types.DecimalType.of(12, 2)),
                Types.NestedField.optional(2, "event_time", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(3, "payload", Types.BinaryType.get()));
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "scalars"), schema,
                PartitionSpec.unpartitioned());
        List<ConnectorColumn> boundSchema = Arrays.asList(
                new ConnectorColumn("amount", ConnectorType.of("DECIMAL64", 12, 2), "", false, null)
                        .withUniqueId(1),
                new ConnectorColumn("event_time", ConnectorType.of("DATETIMEV2", 6, 0), "", true, null)
                        .withUniqueId(2),
                new ConnectorColumn("payload", ConnectorType.of("VARBINARY"), "", true, null)
                        .withUniqueId(3));

        // Doris physical decimal widths and unbounded binary encoding must not look like schema evolution.
        Map<String, String> properties = new HashMap<>(NON_REST_PROPS);
        properties.put(IcebergCatalogProperties.ENABLE_MAPPING_VARBINARY, "true");
        Assertions.assertDoesNotThrow(() -> planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "scalars")).boundTargetColumns(boundSchema),
                properties));
    }

    @Test
    public void planWriteRejectsRecreatedNestedFieldWithSameShape() {
        Schema nestedSchema = new Schema(Types.NestedField.optional(1, "payload",
                Types.StructType.of(Types.NestedField.optional(2, "value", Types.FixedType.ofLength(4)))));
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "nested"), nestedSchema,
                PartitionSpec.unpartitioned());
        ConnectorType boundType = ConnectorType.structOf(
                Collections.singletonList("value"),
                Collections.singletonList(ConnectorType.of("CHAR", 4, 0)),
                Collections.singletonList(true), Collections.singletonList(null))
                .withChildrenFieldIds(Collections.singletonList(2));
        ConnectorColumn bound = new ConnectorColumn("payload", boundType, "", true, null)
                .withUniqueId(1);

        table.updateSchema().deleteColumn("payload.value").commit();
        table.updateSchema().addColumn("payload", "value", Types.FixedType.ofLength(4)).commit();

        // The replacement has the same name/type/ordinal but a new nested field id. Accepting it would write
        // the old bound payload under a different Iceberg identity.
        Assertions.assertThrows(DorisConnectorException.class, () -> planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "nested"))
                        .boundTargetColumns(Collections.singletonList(bound))));
    }

    @Test
    public void planWriteIgnoresReservedRowLineageColumnsDuringSchemaValidation() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        List<ConnectorColumn> fullBoundSchema = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "", false, null)
                        .withUniqueId(table.schema().findField("id").fieldId()),
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)
                        .withUniqueId(table.schema().findField("name").fieldId()),
                new ConnectorColumn("_row_id", ConnectorType.of("BIGINT"), "", true, null)
                        .invisible().reservedPassthrough(),
                new ConnectorColumn("_last_updated_sequence_number", ConnectorType.of("BIGINT"), "", true, null)
                        .invisible().reservedPassthrough());
        WriteHandle handle = new WriteHandle(new IcebergTableHandle("db1", "t2"))
                .columns(Collections.singletonList(fullBoundSchema.get(0)))
                .boundTargetColumns(fullBoundSchema);

        Assertions.assertDoesNotThrow(() -> planSink(table, contextWithStorage(), handle));
    }

    @Test
    public void planWriteRejectsRecreatedColumnWithSameNameAndType() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        WriteHandle handle = new WriteHandle(new IcebergTableHandle("db1", "t2")).columns(Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "", false, null)
                        .withUniqueId(table.schema().findField("id").fieldId()),
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)
                        .withUniqueId(table.schema().findField("name").fieldId())));
        table.updateSchema().deleteColumn("name").commit();
        table.updateSchema().addColumn("name", Types.StringType.get()).commit();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(table, contextWithStorage(), handle));
    }

    @Test
    public void planWriteRejectsSchemaReorderedAfterBinding() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        WriteHandle handle = new WriteHandle(new IcebergTableHandle("db1", "t2")).columns(Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "", false, null),
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)));
        table.updateSchema().moveFirst("name").commit();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(table, contextWithStorage(), handle));
        Assertions.assertTrue(ex.getMessage().contains("schema changed"));
    }

    @Test
    public void planWriteRejectsColumnTypeChangedAfterBinding() {
        Table table = unpartitionedUnsortedTable(freshCatalog());
        WriteHandle handle = new WriteHandle(new IcebergTableHandle("db1", "t2")).columns(Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "", false, null),
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)));
        table.updateSchema().updateColumn("id", Types.LongType.get()).commit();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> planSink(table, contextWithStorage(), handle));
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
    public void planWriteRewriteRejectsRequestScopedRowLocatorForV2AndV3() {
        assertRewriteRejectsRequestScopedRowLocator(unpartitionedUnsortedTable(freshCatalog()));
        assertRewriteRejectsRequestScopedRowLocator(formatVersionThreeTable(freshCatalog()));
    }

    private static void assertRewriteRejectsRequestScopedRowLocator(Table table) {
        RecordingConnectorContext context = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, context);
        WriteSession session = sessionFor(table, context);
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1",
                IcebergWriterHelper.getFormatVersion(table) >= 3 ? "tv3" : "t2");
        List<ConnectorColumn> boundColumns = new ArrayList<>(boundDataColumns(table));
        if (IcebergWriterHelper.getFormatVersion(table) >= 3) {
            boundColumns.add(new ConnectorColumn("_row_id", ConnectorType.of("BIGINT"), "", true, null)
                    .invisible().reservedPassthrough());
            boundColumns.add(new ConnectorColumn(
                    "_last_updated_sequence_number", ConnectorType.of("BIGINT"), "", true, null)
                    .invisible().reservedPassthrough());
        }
        boundColumns.add(provider.getSyntheticWriteColumns(session, tableHandle).get(0));

        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session, new WriteHandle(tableHandle)
                        .boundTargetColumns(boundColumns)
                        .writeOperation(WriteOperation.REWRITE)));
        Assertions.assertTrue(exception.getMessage().contains("schema changed"));
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

    @Test
    public void planWritePrefersOssBackendConfigOverGenericS3Fallback() {
        Table table = partitionedSortedTable(freshCatalog());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.backendFileType = TFileType.FILE_S3;
        ctx.storageProperties = Arrays.asList(
                new FakeS3CompatibleStorageProperties("OSS").backendProperties(ImmutableMap.of(
                        "AWS_ENDPOINT", "https://oss-cn-beijing.aliyuncs.com",
                        "AWS_REGION", "cn-beijing",
                        "use_path_style", "false")),
                new FakeS3CompatibleStorageProperties("S3").backendProperties(ImmutableMap.of(
                        "AWS_ENDPOINT", "https://s3.cn-beijing.amazonaws.com",
                        "AWS_REGION", "cn-beijing",
                        "use_path_style", "true",
                        "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT")));

        TIcebergTableSink sink = planSink(table, ctx,
                new WriteHandle(new IcebergTableHandle("db1", "t1")));

        Assertions.assertEquals("https://oss-cn-beijing.aliyuncs.com",
                sink.getHadoopConfig().get("AWS_ENDPOINT"));
        Assertions.assertEquals("false", sink.getHadoopConfig().get("use_path_style"));
        Assertions.assertNull(sink.getHadoopConfig().get("AWS_CREDENTIALS_PROVIDER_TYPE"),
                "generic S3-only properties must not leak into an explicitly matched OSS write");
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
        // Wrap a real table's operations with a FileIO carrying a non-empty vended token. Keeping the real table
        // operations also exercises the statement-pinned writer identity and fresh-metadata validation.
        InMemoryCatalog catalog = freshCatalog();
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/tvend/data");
        Table real = catalog.createTable(TableIdentifier.of("db1", "tvend"), SCHEMA,
                PartitionSpec.unpartitioned(), tableProps);

        Table vendedTable = new BaseTable(
                new IcebergAuthenticatedTableOperations(
                        ((HasTableOperations) real).operations(),
                        new PropsFileIO(Collections.singletonMap("s3.access-key-id", "vended-raw"))),
                real.name());

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
        ops.table = vendedTable;
        IcebergWritePlanProvider provider = new IcebergWritePlanProvider(IcebergCatalogProperties.of(restVendedProps()), ops, ctx);
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
        props.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, IcebergCatalogProperties.TYPE_REST);
        props.put("iceberg.rest.vended-credentials-enabled", "true");
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
    public void planWriteStaticPartitionKeysTheRenamedPartitionField() {
        // ADD PARTITION KEY id AS id_identity: the engine keys the spec by the SOURCE column (the identifier Spark
        // resolves for a static PARTITION clause, and the one an identity value can be materialized through), while
        // the BE writer matches static_partition_values against the partition FIELD name. Shipping "id" unchanged
        // would make BE treat the write as dynamic, so the planner must rewrite the key to the field name.
        // MUTATION: dropping the rewrite -> {"id"="7"} != {"id_identity"="7"} -> red.
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "value", Types.StringType.get()));
        Table table = freshCatalog().createTable(TableIdentifier.of("db1", "t1"), schema,
                PartitionSpec.builderFor(schema).identity("id", "id_identity").build(),
                Collections.singletonMap("write.format.default", "parquet"));

        TIcebergTableSink sink = planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).overwrite(true)
                        .writeContext(Collections.singletonMap("id", "7")));

        Assertions.assertEquals(Collections.singletonMap("id_identity", "7"), sink.getStaticPartitionValues());
    }

    @Test
    public void planWriteStaticPartitionNullMarkerReachesSink() {
        Table table = partitionedSortedTable(freshCatalog());
        WriteHandle handle = new WriteHandle(new IcebergTableHandle("db1", "t1"))
                .overwrite(true).writeContext(Collections.singletonMap("id", "NULL"));
        handle.staticPartitionNullKeys = Collections.singleton("id");
        TIcebergTableSink sink = planSink(table, contextWithStorage(), handle);

        Assertions.assertEquals(Collections.singleton("id"), sink.getStaticPartitionNullKeys());
        Assertions.assertEquals("null", sink.getStaticPartitionValues().get("id"));
    }

    @Test
    public void planWriteKeepsNullBinaryKeysDistinctFromBytesForFullAndHybridSpecs() {
        String bytes = "0x00112233445566778899aabbccddeeff";
        for (org.apache.iceberg.types.Type keyType : Arrays.asList(
                Types.BinaryType.get(), Types.FixedType.ofLength(16), Types.UUIDType.get())) {
            for (boolean hybrid : new boolean[] {false, true}) {
                Schema schema = new Schema(Types.NestedField.optional(1, "key", keyType),
                        Types.NestedField.optional(2, "region", Types.StringType.get()));
                PartitionSpec.Builder spec = PartitionSpec.builderFor(schema).identity("key");
                if (hybrid) {
                    spec.identity("region");
                }
                Table table = freshCatalog().createTable(TableIdentifier.of("db1", "t1"), schema, spec.build(),
                        Collections.singletonMap("write.format.default", "parquet"));
                String message = keyType + " hybrid=" + hybrid;

                Map<String, String> nullValues = new HashMap<>(Collections.singletonMap("key", "NULL"));
                WriteHandle nullHandle = new WriteHandle(new IcebergTableHandle("db1", "t1"))
                        .overwrite(true).writeContext(nullValues);
                nullHandle.staticPartitionNullKeys = Collections.singleton("key");
                TIcebergTableSink nullSink = planSink(table, contextWithStorage(), nullHandle);
                Assertions.assertEquals(Collections.singleton("key"), nullSink.getStaticPartitionNullKeys(), message);
                Assertions.assertEquals("null", nullSink.getStaticPartitionValues().get("key"), message);
                // Encoding the wire marker must not rewrite the engine's static partition spec.
                Assertions.assertEquals("NULL", nullValues.get("key"), message);

                TIcebergTableSink bytesSink = planSink(table, contextWithStorage(),
                        new WriteHandle(new IcebergTableHandle("db1", "t1"))
                                .overwrite(true).writeContext(Collections.singletonMap("key", bytes)));
                Assertions.assertTrue(bytesSink.getStaticPartitionNullKeys().isEmpty(), message);
                Assertions.assertEquals(bytes, bytesSink.getStaticPartitionValues().get("key"), message);
            }
        }
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
    public void getWriteSortColumnsUsesBoundFieldIdentityInsteadOfLiveOrdinal() {
        Table table = partitionedSortedTable(freshCatalog());
        List<ConnectorColumn> reversedBoundSchema = Arrays.asList(
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)
                        .withUniqueId(table.schema().findField("name").fieldId()),
                new ConnectorColumn("id", ConnectorType.of("INT"), "", false, null)
                        .withUniqueId(table.schema().findField("id").fieldId()));

        List<ConnectorWriteSortColumn> cols = providerFor(table, contextWithStorage())
                .getWriteSortColumns(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "t1"), reversedBoundSchema);

        // Sort positions index the already-bound output. A live ordinal would incorrectly select name here.
        Assertions.assertEquals(1, cols.get(0).getColumnIndex());
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

    @Test
    public void planWriteRejectsSortOrderEvolutionAfterPhysicalShaping() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t2");
        Table plannedTable = unpartitionedUnsortedTable(catalog);
        RecordingConnectorContext ctx = contextWithStorage();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = plannedTable;
        IcebergWritePlanProvider provider = new IcebergWritePlanProvider(IcebergCatalogProperties.of(NON_REST_PROPS), ops, ctx);
        WriteSession session = new WriteSession(new IcebergConnectorTransaction(42L, ops, ctx));
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "t2");
        String boundMetadataIdentity = provider.getWriteMetadataIdentity(session, tableHandle);
        Assertions.assertNull(provider.getWriteSortColumns(session, tableHandle),
                "the physical plan must be shaped as unsorted at S0");

        catalog.loadTable(id).replaceSortOrder().asc("name").commit();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session,
                        new WriteHandle(tableHandle).boundWriteMetadataIdentity(boundMetadataIdentity)));
        Assertions.assertTrue(ex.getMessage().contains("write metadata changed"),
                "a post-bind sort-order change must fail before files can be labeled with the refreshed order");
    }

    @Test
    public void planWriteRejectsPartitionFieldRenameAfterStaticOverwriteBinding() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table plannedTable = catalog.createTable(id, SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("id").build());
        RecordingConnectorContext ctx = contextWithStorage();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = plannedTable;
        IcebergWritePlanProvider provider = new IcebergWritePlanProvider(IcebergCatalogProperties.of(NON_REST_PROPS), ops, ctx);
        WriteSession session = new WriteSession(new IcebergConnectorTransaction(42L, ops, ctx));
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "t1");
        String boundMetadataIdentity = provider.getWriteMetadataIdentity(session, tableHandle);

        catalog.loadTable(id).updateSpec().renameField("id", "renamed_id").commit();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session, new WriteHandle(tableHandle)
                        .overwrite(true)
                        .writeContext(Collections.singletonMap("id", "7"))
                        .boundWriteMetadataIdentity(boundMetadataIdentity)));
        Assertions.assertTrue(ex.getMessage().contains("write metadata changed"),
                "a post-bind partition-field rename must fail before the stale static spec reaches commit");
    }

    @Test
    public void planWriteRejectsDeleteModeEvolution() {
        assertRowLevelModeEvolutionRejected(TableProperties.DELETE_MODE, WriteOperation.DELETE);
    }

    @Test
    public void planWriteRejectsUpdateModeEvolutionAfterUpdateIsEncodedAsMerge() {
        assertRowLevelModeEvolutionRejected(TableProperties.UPDATE_MODE, WriteOperation.MERGE);
    }

    @Test
    public void planWriteRejectsMergeModeEvolution() {
        assertRowLevelModeEvolutionRejected(TableProperties.MERGE_MODE, WriteOperation.MERGE);
    }

    private static void assertRowLevelModeEvolutionRejected(String modeProperty, WriteOperation sinkOperation) {
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        RecordingConnectorContext ctx = contextWithStorage();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        IcebergWritePlanProvider provider = new IcebergWritePlanProvider(IcebergCatalogProperties.of(NON_REST_PROPS), ops, ctx);
        WriteSession session = new WriteSession(new IcebergConnectorTransaction(42L, ops, ctx));
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "t2");
        table.updateProperties().set(modeProperty, "merge-on-read").commit();
        String boundMetadataIdentity = provider.getWriteMetadataIdentity(session, tableHandle);

        table.updateProperties().set(modeProperty, "copy-on-write").commit();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session, new WriteHandle(tableHandle)
                        .boundWriteMetadataIdentity(boundMetadataIdentity)
                        .writeOperation(sinkOperation)));
        Assertions.assertTrue(ex.getMessage().contains("write metadata changed"),
                "row-level mode changes must invalidate the generation that admitted the statement");
    }

    @Test
    public void writeMetadataIdentityChangesWithFormatVersion() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, ctx);
        WriteSession session = sessionFor(table, ctx);
        IcebergTableHandle handle = new IcebergTableHandle("db1", "t2");
        String v2Identity = provider.getWriteMetadataIdentity(session, handle);

        table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

        Assertions.assertNotEquals(v2Identity, provider.getWriteMetadataIdentity(session, handle),
                "format version changes the physical write schema and must change the generation fence");
    }

    @Test
    public void planWriteRejectsDropRecreateBeforeFirstWritableTableLoad() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier identifier = TableIdentifier.of("db1", "t2");
        Table original = unpartitionedUnsortedTable(catalog);
        IcebergTableHandle handle = new IcebergTableHandle("db1", "t2");
        // BindSink got U0's schema and identity from one load. Construct the write provider now, but do not
        // resolve its sharedWritableTable until after the same-name replacement below.
        String originalIdentity = IcebergWritePlanProvider.writeMetadataIdentity(original);
        RecordingConnectorContext ctx = contextWithStorage();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = original;
        IcebergWritePlanProvider provider = new IcebergWritePlanProvider(IcebergCatalogProperties.of(NON_REST_PROPS), ops, ctx);
        WriteSession session = new WriteSession(new IcebergConnectorTransaction(42L, ops, ctx));

        catalog.dropTable(identifier, false);
        Table recreated = unpartitionedUnsortedTable(catalog);
        ops.table = recreated;

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session,
                        new WriteHandle(handle).boundWriteMetadataIdentity(originalIdentity)));
        Assertions.assertTrue(ex.getMessage().contains("write metadata changed"),
                "the first writable-table load must reject U1 instead of moving U0's conflict baseline");
    }

    @Test
    public void planWriteRejectsWriteDefaultEvolutionBySchemaGeneration() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = formatVersionThreeTable(catalog);
        table.updateSchema().updateColumnDefault("id", Literal.of(42)).commit();
        RecordingConnectorContext context = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, context);
        WriteSession session = sessionFor(table, context);
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "tv3");
        // Bind columns and generation in one statement scope, as a real INSERT does.
        List<ConnectorColumn> boundColumns = provider.getWriteColumns(
                session, tableHandle, Optional.empty()).orElseThrow(AssertionError::new);
        String boundIdentity = provider.getWriteMetadataIdentity(session, tableHandle);

        table.updateSchema().updateColumnDefault("id", Literal.of(7)).commit();

        // A default change always commits a new schema id, so the schema-generation fences reject the stale
        // write before any column comparison; write defaults need no comparison of their own.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session, new WriteHandle(tableHandle)
                        .boundTargetColumns(boundColumns)
                        .boundWriteMetadataIdentity(boundIdentity)));
        Assertions.assertTrue(ex.getMessage().contains("changed"), ex.getMessage());
    }

    @Test
    public void planWriteAcceptsPinnedRequestScopedDefaults() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = formatVersionThreeTable(catalog);
        table.updateSchema().updateColumnDefault("id", Literal.of(42)).commit();
        RecordingConnectorContext context = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, context);
        WriteSession session = sessionFor(table, context);
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "tv3");
        List<ConnectorColumn> boundColumns = provider.getWriteColumns(
                session, tableHandle, Optional.empty()).orElseThrow(AssertionError::new);

        TIcebergTableSink sink = Assertions.assertDoesNotThrow(() -> provider.planWrite(session,
                new WriteHandle(tableHandle).boundTargetColumns(boundColumns)))
                .getDataSink().getIcebergTableSink();

        Assertions.assertTrue(sink.getSchemaJson().contains("\"write-default\":42"));
    }

    @Test
    public void planRewriteAcceptsStableWriteDefault() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = formatVersionThreeTable(catalog);
        table.updateSchema().addColumn("bonus", Types.IntegerType.get(), Literal.of(7)).commit();
        table.updateSchema().updateColumnDefault("bonus", Literal.of(9)).commit();
        // rewrite_data_files binds the cached read schema, which deliberately carries no write default.
        List<ConnectorColumn> boundColumns = new ArrayList<>(boundDataColumns(table));
        boundColumns.add(new ConnectorColumn("bonus", ConnectorType.of("INT"), "", true, null)
                .withUniqueId(table.schema().findField("bonus").fieldId()));
        boundColumns.add(new ConnectorColumn("_row_id", ConnectorType.of("BIGINT"), "", true, null)
                .invisible().reservedPassthrough());
        boundColumns.add(new ConnectorColumn(
                "_last_updated_sequence_number", ConnectorType.of("BIGINT"), "", true, null)
                .invisible().reservedPassthrough());

        TIcebergTableSink sink = Assertions.assertDoesNotThrow(() -> planSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "tv3"))
                        .boundTargetColumns(boundColumns)
                        .writeOperation(WriteOperation.REWRITE)));
        Assertions.assertEquals(TIcebergWriteType.REWRITE, sink.getWriteType());
    }

    @Test
    public void planRewriteRejectsV2BoundSchemaAtV3Planning() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        List<ConnectorColumn> v2BoundSchema = boundDataColumns(table);
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, ctx);
        WriteSession session = sessionFor(table, ctx);
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "t2");

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session, new WriteHandle(tableHandle)
                        .boundTargetColumns(v2BoundSchema)
                        .boundWriteMetadataIdentity(provider.getWriteMetadataIdentity(session, tableHandle))
                        .writeOperation(WriteOperation.REWRITE)));
        Assertions.assertTrue(ex.getMessage().contains("write metadata changed"),
                "a v3 rewrite sink must not consume output bound without row-lineage columns");
    }

    @Test
    public void planMergeRejectsV2BoundSchemaAtV3Planning() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = unpartitionedUnsortedTable(catalog);
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, ctx);
        WriteSession session = sessionFor(table, ctx);
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "t2");
        List<ConnectorColumn> v2DataColumns = boundDataColumns(table);
        List<ConnectorColumn> v2BoundSchemaWithRowId = Arrays.asList(
                v2DataColumns.get(0), v2DataColumns.get(1),
                provider.getSyntheticWriteColumns(session, tableHandle).get(0));
        table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planWrite(session, new WriteHandle(tableHandle)
                        .boundTargetColumns(v2BoundSchemaWithRowId)
                        .boundWriteMetadataIdentity(provider.getWriteMetadataIdentity(session, tableHandle))
                        .writeOperation(WriteOperation.MERGE)));
        Assertions.assertTrue(ex.getMessage().contains("write metadata changed"),
                "a v3 merge sink must not advertise lineage columns absent from the bound output");
    }

    private static List<ConnectorColumn> boundDataColumns(Table table) {
        return Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "", false, null)
                        .withUniqueId(table.schema().findField("id").fieldId()),
                new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)
                        .withUniqueId(table.schema().findField("name").fieldId()));
    }

    // ───────────────────────────── getWritePartitioning (connector declares, ② C3b-core) ─────────────────────────────
    //
    // WHY: post-flip the iceberg merge-write distribution (DistributionSpecMerge) is built fe-core-side, but
    // its native partition-spec walk (PhysicalExternalRowLevelMergeSink.buildInsertPartitionFields ->
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

    @Test
    public void getWritePartitioningRoutesNestedSourceByItsTopLevelColumn() {
        InMemoryCatalog catalog = freshCatalog();
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "part", Types.IntegerType.get()))));
        Table table = catalog.createTable(TableIdentifier.of("db1", "nested_partition"), schema,
                PartitionSpec.builderFor(schema).bucket("payload.part", 8).build());

        ConnectorWritePartitionField field = providerFor(table, contextWithStorage())
                .getWritePartitioning(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "nested_partition"))
                .getFields().get(0);

        Assertions.assertEquals("payload", field.getSourceColumnName(),
                "merge exchange must route a nested source through its bound top-level struct slot");
        Assertions.assertEquals(Integer.valueOf(3), field.getSourceId());
    }

    // ───────────────────────────── getWriteDistribution (partition-hash INSERT routing) ─────────────────────────────
    //
    // WHY: without a declared distribution a partitioned INSERT is spread randomly, so every writer opens a file for
    // every partition it sees and the write fails once one writer holds more than
    // table_sink_partition_write_max_partition_nums_per_writer (128) open partitions. The BE iceberg_transform
    // function hashes the transformed partition value, so one partition reaches one writer.

    @Test
    public void getWriteDistributionRoutesByTheIcebergTransform() {
        Table table = bucketPartitionedTable(freshCatalog());
        ConnectorWriteDistribution distribution = providerFor(table, contextWithStorage())
                .getWriteDistribution(sessionFor(table, contextWithStorage()), new IcebergTableHandle("db1", "tb"));

        Assertions.assertNotNull(distribution, "a partitioned INSERT must be routed by its partition value");
        Assertions.assertEquals(ConnectorWriteDistribution.Mode.EXTERNAL_HASH, distribution.getMode());
        // The route column is the source column ("id"), not the partition field ("id_bucket"): fe-core binds it to
        // the sink output, and the BE applies the transform.
        Assertions.assertEquals(Collections.singletonList("id"), distribution.getRouteColumns());
        Assertions.assertEquals("iceberg_transform", distribution.getPartitionFunction());
        Assertions.assertEquals(Collections.singletonMap("transforms", "bucket[16]"),
                distribution.getPartitionFunctionOptions());
        Assertions.assertEquals(ConnectorWriteDistribution.WriterAssignment.SKEWED, distribution.getWriterAssignment());
    }

    @Test
    public void getWriteDistributionCarriesEveryPartitionFieldInSpecOrder() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()));
        Table table = freshCatalog().createTable(TableIdentifier.of("db1", "multi_field"), schema,
                PartitionSpec.builderFor(schema).day("ts").identity("name").bucket("id", 4).build());

        ConnectorWriteDistribution distribution = providerFor(table, contextWithStorage())
                .getWriteDistribution(sessionFor(table, contextWithStorage()),
                        new IcebergTableHandle("db1", "multi_field"));

        // The BE pairs the i-th route expression with the i-th transform, so both lists follow the spec order.
        Assertions.assertEquals(Arrays.asList("ts", "name", "id"), distribution.getRouteColumns());
        Assertions.assertEquals(Collections.singletonMap("transforms", "day,identity,bucket[4]"),
                distribution.getPartitionFunctionOptions());
    }

    @Test
    public void getWriteDistributionKeepsRandomWritesWhenRoutingIsUnavailable() {
        InMemoryCatalog catalog = freshCatalog();
        partitionedSortedTable(catalog);
        Table unpartitioned = unpartitionedUnsortedTable(catalog);
        Assertions.assertNull(providerFor(unpartitioned, contextWithStorage()).getWriteDistribution(
                sessionFor(unpartitioned, contextWithStorage()), new IcebergTableHandle("db1", "t2")));

        // The route columns are top-level sink outputs; a nested source has none, so keep the random write.
        // MUTATION: resolving the nested source to its top-level struct column -> routes by "payload" -> red.
        Schema nestedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "part", Types.IntegerType.get()))));
        Table nested = catalog.createTable(TableIdentifier.of("db1", "nested_partition"), nestedSchema,
                PartitionSpec.builderFor(nestedSchema).bucket("payload.part", 8).build());
        Assertions.assertNull(providerFor(nested, contextWithStorage()).getWriteDistribution(
                sessionFor(nested, contextWithStorage()), new IcebergTableHandle("db1", "nested_partition")));

        // A backend older than exec version 18 rejects iceberg_transform, so the INSERT keeps the random write it
        // already runs there. MUTATION: dropping the version gate -> red.
        Table partitioned = bucketPartitionedTable(catalog);
        IcebergTableHandle handle = new IcebergTableHandle("db1", "tb");
        Assertions.assertNull(providerFor(partitioned, contextWithStorage()).getWriteDistribution(
                sessionFor(partitioned, contextWithStorage()).beExecVersion(17), handle));
        Assertions.assertNotNull(providerFor(partitioned, contextWithStorage()).getWriteDistribution(
                sessionFor(partitioned, contextWithStorage()).beExecVersion(18), handle));
    }

    @Test
    public void getWriteDistributionUsesTheStatementPinnedSchema() {
        Table table = bucketPartitionedTable(freshCatalog());
        IcebergWritePlanProvider provider = providerFor(table, contextWithStorage());
        WriteSession session = sessionFor(table, contextWithStorage());
        IcebergTableHandle handle = new IcebergTableHandle("db1", "tb");
        // Binding the INSERT pins the write schema for the statement; the sink output carries its column names.
        provider.getWriteColumns(session, handle, Optional.empty());
        table.updateSchema().renameColumn("id", "renamed_id").commit();

        // MUTATION: reading the live table instead of the pinned schema -> routes by "renamed_id", which the bound
        // sink output does not have -> red.
        Assertions.assertEquals(Collections.singletonList("id"),
                provider.getWriteDistribution(session, handle).getRouteColumns());
    }

    @Test
    public void getWriteDistributionRoutesBinaryPartitionValuesOnlyOnBackendsThatHashThem() {
        // The exchange hashes the transformed value. A backend below exec version 19 cannot hash the BE column of a
        // BINARY or FIXED value, so routing identity(binary_key) there fails the INSERT once it has two writers;
        // such a write keeps random writes. bucket() turns the bytes into an INT, so it is routed from version 18.
        // MUTATION: dropping the version check -> identity_binary routes at 18 -> red; skipping BINARY and FIXED
        // unconditionally -> nothing routes at 19 -> red.
        InMemoryCatalog catalog = freshCatalog();
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "binary_key", Types.BinaryType.get()),
                Types.NestedField.optional(3, "fixed_key", Types.FixedType.ofLength(4)));
        Map<String, PartitionSpec> binaryValued = new LinkedHashMap<>();
        binaryValued.put("identity_binary", PartitionSpec.builderFor(schema).identity("binary_key").build());
        binaryValued.put("truncate_binary", PartitionSpec.builderFor(schema).truncate("binary_key", 1).build());
        binaryValued.put("identity_fixed", PartitionSpec.builderFor(schema).identity("fixed_key").build());
        // One binary-valued field is enough: the route hashes every field of the spec together.
        binaryValued.put("int_and_binary",
                PartitionSpec.builderFor(schema).identity("id").identity("binary_key").build());
        Map<String, List<String>> routeColumns = new LinkedHashMap<>();
        routeColumns.put("identity_binary", Collections.singletonList("binary_key"));
        routeColumns.put("truncate_binary", Collections.singletonList("binary_key"));
        routeColumns.put("identity_fixed", Collections.singletonList("fixed_key"));
        routeColumns.put("int_and_binary", Arrays.asList("id", "binary_key"));
        Map<String, String> transforms = new LinkedHashMap<>();
        transforms.put("identity_binary", "identity");
        transforms.put("truncate_binary", "truncate[1]");
        transforms.put("identity_fixed", "identity");
        transforms.put("int_and_binary", "identity,identity");
        for (Map.Entry<String, PartitionSpec> entry : binaryValued.entrySet()) {
            String name = entry.getKey();
            Table table = catalog.createTable(TableIdentifier.of("db1", name), schema, entry.getValue());
            IcebergTableHandle handle = new IcebergTableHandle("db1", name);
            Assertions.assertNull(providerFor(table, contextWithStorage()).getWriteDistribution(
                    sessionFor(table, contextWithStorage()).beExecVersion(18), handle), name);
            ConnectorWriteDistribution distribution = providerFor(table, contextWithStorage()).getWriteDistribution(
                    sessionFor(table, contextWithStorage()).beExecVersion(19), handle);
            Assertions.assertEquals(routeColumns.get(name), distribution.getRouteColumns(), name);
            Assertions.assertEquals(Collections.singletonMap("transforms", transforms.get(name)),
                    distribution.getPartitionFunctionOptions(), name);
        }

        Table bucketed = catalog.createTable(TableIdentifier.of("db1", "bucket_binary"), schema,
                PartitionSpec.builderFor(schema).bucket("binary_key", 16).build());
        ConnectorWriteDistribution distribution = providerFor(bucketed, contextWithStorage()).getWriteDistribution(
                sessionFor(bucketed, contextWithStorage()).beExecVersion(18),
                new IcebergTableHandle("db1", "bucket_binary"));
        Assertions.assertEquals(Collections.singletonList("binary_key"), distribution.getRouteColumns());
        Assertions.assertEquals(Collections.singletonMap("transforms", "bucket[16]"),
                distribution.getPartitionFunctionOptions());
    }

    @Test
    public void oldBeRejectsDataWritesUsingNestedPartitionSources() {
        InMemoryCatalog catalog = freshCatalog();
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StructType.of(
                        Types.NestedField.optional(3, "part", Types.IntegerType.get()))));
        Table table = catalog.createTable(TableIdentifier.of("db1", "nested_partition"), schema,
                PartitionSpec.builderFor(schema).bucket("payload.part", 8).build());

        WriteHandle oldBeWrite = new WriteHandle(
                new IcebergTableHandle("db1", "nested_partition")).beExecVersion(11);
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> providerFor(table, contextWithStorage()).planWrite(
                        sessionFor(table, contextWithStorage()), oldBeWrite));
        Assertions.assertTrue(error.getMessage().contains("nested partition"));

        WriteHandle currentBeWrite = new WriteHandle(
                new IcebergTableHandle("db1", "nested_partition")).beExecVersion(12);
        Assertions.assertDoesNotThrow(() -> providerFor(table, contextWithStorage()).planWrite(
                sessionFor(table, contextWithStorage()), currentBeWrite));

        WriteHandle oldBeRewrite = new WriteHandle(new IcebergTableHandle("db1", "nested_partition"))
                .writeOperation(WriteOperation.REWRITE).beExecVersion(11);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> providerFor(table, contextWithStorage()).planWrite(
                        sessionFor(table, contextWithStorage()), oldBeRewrite));

        WriteHandle currentBeRewrite = new WriteHandle(new IcebergTableHandle("db1", "nested_partition"))
                .writeOperation(WriteOperation.REWRITE).beExecVersion(12);
        Assertions.assertDoesNotThrow(() -> providerFor(table, contextWithStorage()).planWrite(
                sessionFor(table, contextWithStorage()), currentBeRewrite));

        WriteHandle deleteOnlyMerge = new WriteHandle(new IcebergTableHandle("db1", "nested_partition"))
                .writeOperation(WriteOperation.MERGE).writesDataFiles(false).beExecVersion(11);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> providerFor(table, contextWithStorage()).planWrite(
                        sessionFor(table, contextWithStorage()), deleteOnlyMerge));

        WriteHandle delete = new WriteHandle(new IcebergTableHandle("db1", "nested_partition"))
                .writeOperation(WriteOperation.DELETE).writesDataFiles(false).beExecVersion(11);
        Assertions.assertDoesNotThrow(() -> providerFor(table, contextWithStorage()).planWrite(
                sessionFor(table, contextWithStorage()), delete));
    }

    // ───────────────────── getSyntheticWriteColumns (connector declares the row-id STRUCT, ③ C3b-core) ─────────────────────
    //
    // WHY: post-flip the iceberg DML hidden column __DORIS_ICEBERG_ROWID_COL__ that legacy
    // IcebergExternalTable.getFullSchema injected is unreachable — a PluginDrivenExternalTable carries no
    // iceberg knowledge. The connector therefore declares it as an engine-neutral invisible ConnectorColumn;
    // fe-core converts + appends it (gated request-side) while a DML over the table is in flight. The connector
    // is the sole source of this identity; this pins the carried STRUCT shape (name / 4 fields / types /
    // invisible / not-null), mirrored by the fe-core ConnectorColumnConverter contract pin.

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
        Assertions.assertFalse(rowId.isNullable(), "the row-id column must be not-null");

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

    // ── commit-bridge supply (S4 part 2): planWrite drains the per-statement scope into rewritable_delete_file_sets ──

    private static final String STASH_QID = "qid-stash";

    private static IcebergWritePlanProvider supplyProvider(Table table, RecordingConnectorContext ctx) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return new IcebergWritePlanProvider(IcebergCatalogProperties.of(NON_REST_PROPS), ops, ctx);
    }

    private static WriteSession supplySession(Table table, RecordingConnectorContext ctx, String queryId) {
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
    public void planWriteDeleteSinkAttachesRewritableSetsFromScope() {
        // The scan accumulated this statement's old DV (referenced data file -> its non-equality deletes) into the
        // per-statement scope; planWrite must drain it onto the sink so the BE OR-merges those deletes into the
        // new DV. MUTATION: not reading the scope / not setting the field -> the BE writes a DV without the old
        // deletes -> resurrection.
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        WriteSession session = supplySession(table, ctx, STASH_QID);
        IcebergStatementScope.rewritableDeleteSupply(session).put("oss://bucket/wh/db1/tv3/data/f1.parquet",
                dvDescs("oss://bucket/wh/db1/tv3/data/dv1.puffin", 16L, 64L));

        ConnectorSinkPlan plan = supplyProvider(table, ctx).planWrite(session,
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
    }

    @Test
    public void planWriteMergeSinkAttachesRewritableSetsFromScope() {
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        WriteSession session = supplySession(table, ctx, STASH_QID);
        IcebergStatementScope.rewritableDeleteSupply(session).put("oss://bucket/wh/db1/tv3/data/f1.parquet",
                dvDescs("oss://bucket/wh/db1/tv3/data/dv1.puffin", 8L, 32L));

        ConnectorSinkPlan plan = supplyProvider(table, ctx).planWrite(session,
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.MERGE));
        TIcebergMergeSink sink = plan.getDataSink().getIcebergMergeSink();

        Assertions.assertTrue(sink.isSetRewritableDeleteFileSets(),
                "a v3 MERGE must carry rewritable_delete_file_sets (thrift field 25) too");
        Assertions.assertEquals("oss://bucket/wh/db1/tv3/data/f1.parquet",
                sink.getRewritableDeleteFileSets().get(0).getReferencedDataFilePath());
    }

    @Test
    public void planWriteDeleteSinkLeavesSetsUnsetWhenScopeHasNoSupply() {
        // A v3 DELETE whose scan accumulated nothing into the scope (no live deletes) leaves the field unset —
        // byte-identical to legacy's empty gate. The queryId scoping means a different statement's supply is
        // simply not visible here.
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();

        ConnectorSinkPlan plan = supplyProvider(table, ctx).planWrite(
                supplySession(table, ctx, STASH_QID),
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.DELETE));

        Assertions.assertFalse(plan.getDataSink().getIcebergDeleteSink().isSetRewritableDeleteFileSets());
    }

    @Test
    public void planWriteVersionTwoDeleteNeverAttachesRewritableSets() {
        // v2 deletes are plain position-delete files (no DV union), so even a scope-carried supply must NOT be
        // emitted (the BE ignores field 15 below v3). MUTATION: dropping the formatVersion>=3 gate would emit it.
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "tv2"), SCHEMA,
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
        RecordingConnectorContext ctx = contextWithStorage();
        WriteSession session = supplySession(table, ctx, STASH_QID);
        IcebergStatementScope.rewritableDeleteSupply(session)
                .put("oss://bucket/wh/db1/tv2/data/f1.parquet", dvDescs("dv", 1L, 2L));

        ConnectorSinkPlan plan = supplyProvider(table, ctx).planWrite(session,
                new WriteHandle(new IcebergTableHandle("db1", "tv2")).writeOperation(WriteOperation.DELETE));

        Assertions.assertFalse(plan.getDataSink().getIcebergDeleteSink().isSetRewritableDeleteFileSets());
    }

    @Test
    public void planWriteInsertIgnoresScopeSupply() {
        // INSERT ... SELECT FROM an iceberg source accumulates the source scan's deletes into the scope, but the
        // INSERT write does not consume them; planWrite must still produce a plain table sink (the supply is
        // simply ignored, not attached). MUTATION: attaching the supply to an INSERT sink -> red.
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        WriteSession session = supplySession(table, ctx, STASH_QID);
        IcebergStatementScope.rewritableDeleteSupply(session)
                .put("oss://bucket/wh/db1/tv3/data/src.parquet", dvDescs("dv", 1L, 2L));

        ConnectorSinkPlan plan = supplyProvider(table, ctx).planWrite(session,
                new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.INSERT));

        Assertions.assertTrue(plan.getDataSink().isSetIcebergTableSink(), "INSERT builds a plain table sink");
    }

    @Test
    public void planWriteV3RowLevelDmlUnderNoneScopeFailsLoud() {
        // A format-version>=3 DELETE/MERGE under NONE (no live statement scope) cannot have received the scan's
        // rewritable-delete supply, so planWrite must reject it rather than silently write a DV that drops old
        // deletes. MUTATION: dropping the fail-loud -> a v3 DELETE under NONE resurrects previously-deleted rows.
        Table table = formatVersionThreeTable(freshCatalog());
        RecordingConnectorContext ctx = contextWithStorage();
        WriteSession session = supplySession(table, ctx, STASH_QID).noScope();

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class, () ->
                supplyProvider(table, ctx).planWrite(session,
                        new WriteHandle(new IcebergTableHandle("db1", "tv3")).writeOperation(WriteOperation.DELETE)));
        Assertions.assertTrue(ex.getMessage().contains("per-statement scope"),
                "fail-loud message must name the missing per-statement scope, got: " + ex.getMessage());
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

    @Test
    public void planMergePreservesExplicitlyEmptyReadAcrossConcurrentFirstAppend() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "tv2");
        Table empty = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        RecordingConnectorContext ctx = contextWithStorage();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = empty;
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(ops, IcebergCatalogProperties.of(Collections.emptyMap()), ctx);
        ConnectorMvccSnapshot emptySnapshot = metadata.beginQuerySnapshot(null,
                new IcebergTableHandle("db1", "tv2")).orElseThrow(AssertionError::new);
        IcebergTableHandle emptyPinnedHandle = (IcebergTableHandle) metadata.applySnapshot(
                null, new IcebergTableHandle("db1", "tv2"), emptySnapshot);

        empty.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://bucket/db1/tv2/concurrent.parquet")
                .withFileSizeInBytes(100)
                .withRecordCount(1)
                .withFormat(FileFormat.PARQUET)
                .build()).commit();
        ops.table = catalog.loadTable(id);
        Assertions.assertNotNull(ops.table.currentSnapshot(), "the concurrent append must create S1");

        IcebergConnectorTransaction txn = new IcebergConnectorTransaction(42L, ops, ctx);
        providerFor(ops.table, ctx).planWrite(new WriteSession(txn),
                new WriteHandle(emptyPinnedHandle).writeOperation(WriteOperation.MERGE));

        Assertions.assertEquals(Long.valueOf(-1L), txn.getBaseSnapshotId(),
                "an explicitly empty read must preserve its OCC generation fence across the first append");
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

    // The replacement data files an UPDATE / SQL MERGE writes go through the same iceberg parquet writer as
    // an INSERT, and VIcebergMergeSink builds its inner TIcebergTableSink by copying fields across one by
    // one. A NaN-count policy shipped only on the INSERT sink therefore leaves merge-written files reporting
    // no nan_value_counts, so they stay unprunable even when NaN-free — the pruning restoration would cover
    // only part of what Doris writes. This pins that the merge dialect carries it too.
    @Test
    public void planWriteMergeSinkShipsTheNanCountFieldIds() {
        Schema floatSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "d", Types.DoubleType.get()));
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/t1/data");
        Table table = freshCatalog().createTable(TableIdentifier.of("db1", "t1"), floatSchema,
                PartitionSpec.unpartitioned(), tableProps);
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1"))
                        .writeOperation(WriteOperation.MERGE));

        Assertions.assertTrue(sink.isSetNanCountFieldIds(),
                "the merge dialect must carry the NaN-count policy, not just the INSERT sink");
        Assertions.assertEquals(Collections.singletonList(2), sink.getNanCountFieldIds());
    }

    // #66112: UPDATE and SQL MERGE share the TIcebergMergeSink dialect, but only SQL MERGE carries the
    // one-source-row cardinality rule. The engine decides (statement kind) and the connector must ship the
    // decision verbatim: BE gates its duplicate-match validation on this field, so a connector that dropped
    // it would silently accept a MERGE whose source matches a target row twice — a wrong-result write, not
    // an error. Both polarities are pinned so neither a hardcoded true nor a dropped setter survives.
    @Test
    public void planWriteMergeSinkShipsTheEngineSqlMergeCardinalityRequirement() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1"))
                        .writeOperation(WriteOperation.MERGE)
                        .requireMergeCardinalityCheck(true));

        Assertions.assertTrue(sink.isSetRequireMergeCardinalityCheck(),
                "the field must always be set so BE never has to guess from an unset field");
        Assertions.assertTrue(sink.isRequireMergeCardinalityCheck(),
                "a SQL MERGE must ship require_merge_cardinality_check=true so BE validates duplicate matches");
    }

    @Test
    public void planWriteMergeSinkDoesNotRequireCardinalityCheckForUpdate() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1"))
                        .writeOperation(WriteOperation.UPDATE));

        Assertions.assertTrue(sink.isSetRequireMergeCardinalityCheck(),
                "the field must always be set so BE never has to guess from an unset field");
        Assertions.assertFalse(sink.isRequireMergeCardinalityCheck(),
                "UPDATE shares this sink dialect but has no SQL cardinality rule; validating it would reject"
                        + " legal UPDATEs whose predicate matches a row through several source rows");
    }

    @Test
    public void planWriteMergeSinkShipsWhetherTheWriteProducesDataFiles() {
        // BE opens the data-file writer only when writes_data_files is true. A delete-only SQL MERGE must
        // ship false so it neither builds a writer it never uses nor trips the writer-side schema checks
        // (an Iceberg VARIANT target is deletable but not writable). MUTATION: dropping setWritesDataFiles,
        // or shipping a constant -> one of the two plans below carries the wrong value -> red.
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergMergeSink deleteOnlyMerge = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1"))
                        .writeOperation(WriteOperation.MERGE)
                        .writesDataFiles(false)
                        .requireMergeCardinalityCheck(true));
        Assertions.assertTrue(deleteOnlyMerge.isSetWritesDataFiles(),
                "the field must always be set so BE never has to guess from an unset field");
        Assertions.assertFalse(deleteOnlyMerge.isWritesDataFiles());

        TIcebergMergeSink update = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1"))
                        .writeOperation(WriteOperation.UPDATE));
        Assertions.assertTrue(update.isSetWritesDataFiles());
        Assertions.assertTrue(update.isWritesDataFiles());
    }

    // BE writes position-delete and deletion-vector files under output_path and commits them under
    // table_location (VIcebergDeleteSink::_commit_path_of), so both must name the same directory, the
    // data location, differing only in its normalized vs raw form. If table_location became the table
    // root, BE would record delete files without the data directory, i.e. files that do not exist,
    // and nothing would fail. See TIcebergDeleteSink.table_location in DataSinks.thrift.
    @Test
    public void planWriteDeleteAndMergeSinksKeepTableLocationInTheOutputDirectory() {
        Table table = partitionedSortedTable(freshCatalog());
        TIcebergDeleteSink deleteSink = planDeleteSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.DELETE));
        TIcebergMergeSink mergeSink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.UPDATE));

        Assertions.assertEquals(pathAfterScheme(deleteSink.getOutputPath()),
                pathAfterScheme(deleteSink.getTableLocation()),
                "delete table_location must be the raw form of output_path's directory");
        Assertions.assertEquals(pathAfterScheme(mergeSink.getOutputPath()),
                pathAfterScheme(mergeSink.getTableLocation()),
                "merge table_location must be the raw form of output_path's directory");
    }

    // "s3://bucket/wh/db1/t1/data" and "oss://bucket/wh/db1/t1/data" -> "bucket/wh/db1/t1/data".
    private static String pathAfterScheme(String location) {
        return location.substring(location.indexOf("://") + 3);
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
    public void planWriteMergeSinkUsesCompleteStatementPinnedWriterMetadata() {
        // Every merge-sink field that labels written files comes from the statement-pinned writer metadata, so a
        // property, spec or sort-order change after the pin cannot make files claim metadata they were not
        // written with. The bound write-metadata identity fence for spec and sort order is covered separately.
        InMemoryCatalog catalog = freshCatalog();
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("write.parquet.compression-codec", "zstd");
        tableProps.put("write.data.path", "oss://bucket/wh/db1/pinned/data");
        tableProps.put("format-version", "3");
        Table table = catalog.createTable(TableIdentifier.of("db1", "pinned"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("id").build(), tableProps);
        table.replaceSortOrder().asc("id", NullOrder.NULLS_FIRST).commit();
        RecordingConnectorContext ctx = contextWithStorage();
        IcebergWritePlanProvider provider = providerFor(table, ctx);
        WriteSession session = sessionFor(table, ctx);
        IcebergTableHandle tableHandle = new IcebergTableHandle("db1", "pinned");
        provider.getWriteColumns(session, tableHandle, Optional.empty());
        IcebergWriteSchemaContext pinned = IcebergStatementScope.activeWriteSchema(session, "db1", "pinned")
                .orElseThrow(AssertionError::new);
        int pinnedSpecId = pinned.getPartitionSpec().specId();

        table.updateProperties()
                .set("write.format.default", "orc")
                .set("write.orc.compression-codec", "zlib")
                .set("write.data.path", "oss://bucket/wh/db1/pinned/moved")
                .set("write.metadata.metrics.default", "none")
                .commit();
        table.updateSpec().removeField("id").addField("name").commit();
        table.replaceSortOrder().desc("name").commit();
        TIcebergMergeSink sink = provider.planWrite(session,
                new WriteHandle(tableHandle).writeOperation(WriteOperation.MERGE))
                .getDataSink().getIcebergMergeSink();

        Assertions.assertNotEquals(pinnedSpecId, table.spec().specId(), "the live spec must have moved on");
        Assertions.assertEquals(pinned.getMergeSchemaJson(), sink.getSchemaJson());
        Assertions.assertTrue(sink.getSchemaJson().contains("_row_id"));
        Assertions.assertEquals(pinnedSpecId, sink.getPartitionSpecId());
        Assertions.assertEquals(Collections.singletonMap(pinnedSpecId, pinned.getPartitionSpecJson()),
                sink.getPartitionSpecsJson());
        Assertions.assertEquals(pinnedSpecId, sink.getPartitionSpecIdForDelete());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, sink.getFileFormat());
        Assertions.assertEquals(TFileCompressType.ZSTD, sink.getCompressionType());
        Assertions.assertEquals("oss://bucket/wh/db1/pinned/data", sink.getOriginalOutputPath());
        Assertions.assertEquals("oss://bucket/wh/db1/pinned/data", sink.getTableLocation());
        Assertions.assertTrue(sink.isCollectColumnStats(), "the pinned metrics policy still collects stats");
        Assertions.assertEquals(1, sink.getSortFieldsSize());
        Assertions.assertEquals(table.schema().findField("id").fieldId(),
                sink.getSortFields().get(0).getSourceColumnId());
        Assertions.assertTrue(sink.getSortFields().get(0).isAscending());
    }

    @Test
    public void planWriteMergeSinkPinsUnpartitionedSpecIdForDelete() {
        // Once the last partition field is dropped, position deletes are written under the current unpartitioned
        // spec. Its id is not the initial spec 0, so the delete side must carry the pinned id, not a default.
        Table table = partitionedSortedTable(freshCatalog());
        table.updateSpec().removeField("id").commit();

        TIcebergMergeSink sink = planMergeSink(table, contextWithStorage(),
                new WriteHandle(new IcebergTableHandle("db1", "t1")).writeOperation(WriteOperation.MERGE));

        Assertions.assertFalse(table.spec().isPartitioned());
        Assertions.assertNotEquals(0, table.spec().specId());
        Assertions.assertFalse(sink.isSetPartitionSpecId());
        Assertions.assertFalse(sink.isSetPartitionSpecsJson());
        Assertions.assertEquals(table.spec().specId(), sink.getPartitionSpecIdForDelete());
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
                WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE, WriteOperation.REWRITE),
                provider.supportedOperations());
        Assertions.assertEquals(ConnectorRowChangeStyle.POSITION_DELETE, provider.getRowChangeStyle());
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
        private boolean requireMergeCardinalityCheck;
        private boolean writesDataFiles = true;
        private List<ConnectorColumn> columns = Collections.emptyList();
        private List<ConnectorColumn> boundTargetColumns;
        private String boundWriteMetadataIdentity;
        private int beExecVersion = Integer.MAX_VALUE;

        WriteHandle(ConnectorTableHandle tableHandle) {
            this.tableHandle = tableHandle;
        }

        WriteHandle branch(String v) {
            this.branchName = Optional.ofNullable(v);
            return this;
        }

        WriteHandle columns(List<ConnectorColumn> v) {
            this.columns = v;
            return this;
        }

        WriteHandle boundTargetColumns(List<ConnectorColumn> v) {
            this.boundTargetColumns = v;
            return this;
        }

        WriteHandle boundWriteMetadataIdentity(String v) {
            this.boundWriteMetadataIdentity = v;
            return this;
        }

        WriteHandle beExecVersion(int v) {
            this.beExecVersion = v;
            return this;
        }

        @Override
        public int getBeExecVersion() {
            return beExecVersion;
        }

        @Override
        public String getBoundWriteMetadataIdentity() {
            return boundWriteMetadataIdentity;
        }

        @Override
        public List<ConnectorColumn> getBoundTargetColumns() {
            return boundTargetColumns == null ? columns : boundTargetColumns;
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

        WriteHandle requireMergeCardinalityCheck(boolean v) {
            this.requireMergeCardinalityCheck = v;
            return this;
        }

        @Override
        public boolean isRequireMergeCardinalityCheck() {
            return requireMergeCardinalityCheck;
        }

        WriteHandle writesDataFiles(boolean v) {
            this.writesDataFiles = v;
            return this;
        }

        @Override
        public boolean isWritesDataFiles() {
            return writesDataFiles;
        }

        private Set<String> staticPartitionNullKeys = Collections.emptySet();

        @Override
        public Set<String> getStaticPartitionNullKeys() {
            return staticPartitionNullKeys;
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
            return columns;
        }

        @Override
        public boolean isOverwrite() {
            return overwrite;
        }

        @Override
        public Map<String, String> getStaticPartitionSpec() {
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
        // Default to a live memoizing scope so a v3 row-level DML does not trip planWrite's NONE fail-loud; the
        // dedicated fail-loud test forces NONE via noScope().
        private ConnectorStatementScope statementScope = new TestStatementScope();
        private int beExecVersion = Integer.MAX_VALUE;

        WriteSession(ConnectorTransaction txn) {
            this(txn, "q");
        }

        WriteSession(ConnectorTransaction txn, String queryId) {
            this.txn = txn;
            this.queryId = queryId;
        }

        WriteSession beExecVersion(int version) {
            this.beExecVersion = version;
            return this;
        }

        @Override
        public int getBeExecVersion() {
            return beExecVersion;
        }

        /** Shares an explicit scope (so a test can pre-fill the rewritable-delete supply the write then reads). */
        WriteSession withScope(ConnectorStatementScope scope) {
            this.statementScope = scope;
            return this;
        }

        /** Forces ConnectorStatementScope.NONE (for the v3-DML-under-NONE fail-loud test). */
        WriteSession noScope() {
            this.statementScope = ConnectorStatementScope.NONE;
            return this;
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return statementScope;
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

    /** Table operations over replaceable metadata, so a test can advance or replace a table without a catalog. */
    private static final class MutableMetadataOperations implements TableOperations {
        private TableMetadata metadata;

        MutableMetadataOperations(TableMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIO io() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocationProvider locationProvider() {
            throw new UnsupportedOperationException();
        }
    }
}
