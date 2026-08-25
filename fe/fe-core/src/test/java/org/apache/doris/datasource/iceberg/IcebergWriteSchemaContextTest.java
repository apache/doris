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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class IcebergWriteSchemaContextTest {

    @Test
    public void testPrimitiveWriteDefaultsUseTypedValues() {
        Schema schema = new Schema(17, Arrays.asList(
                defaultField(1, "boolean_col", Types.BooleanType.get(), Literal.of(true), Literal.of(false), false),
                defaultField(2, "int_col", Types.IntegerType.get(), Literal.of(34), Literal.of(35), false),
                defaultField(3, "long_col", Types.LongType.get(),
                        Literal.of(4_900_000_000L), Literal.of(4_900_000_001L), false),
                defaultField(4, "float_col", Types.FloatType.get(),
                        Literal.of(12.25F), Literal.of(13.5F), false),
                defaultField(5, "double_col", Types.DoubleType.get(),
                        Literal.of(-123.5D), Literal.of(456.75D), false),
                defaultField(6, "decimal_col", Types.DecimalType.of(20, 4),
                        Literal.of(new BigDecimal("12345.6789")),
                        Literal.of(new BigDecimal("98765.4321")), false),
                defaultField(7, "date_col", Types.DateType.get(),
                        Literal.of(DateTimeUtil.isoDateToDays("2024-12-17")),
                        Literal.of(DateTimeUtil.isoDateToDays("2025-01-18")), false),
                defaultField(8, "timestamp_col", Types.TimestampType.withoutZone(),
                        Literal.of(DateTimeUtil.isoTimestampToMicros("2024-12-17T23:59:59.123456")),
                        Literal.of(DateTimeUtil.isoTimestampToMicros("2025-01-18T01:02:03.654321")), false),
                defaultField(9, "timestamptz_col", Types.TimestampType.withZone(),
                        Literal.of(DateTimeUtil.isoTimestamptzToMicros(
                                "2024-12-17T23:59:59.123456+00:00")),
                        Literal.of(DateTimeUtil.isoTimestamptzToMicros(
                                "2025-01-18T01:02:03.654321+00:00")), false),
                defaultField(10, "string_col", Types.StringType.get(),
                        Literal.of("initial-default"), Literal.of("write-default"), true),
                defaultField(11, "uuid_col", Types.UUIDType.get(),
                        Literal.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
                        Literal.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174001")), false),
                defaultField(12, "fixed_col", Types.FixedType.ofLength(4),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03, 0x04})),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})), false),
                defaultField(13, "binary_col", Types.BinaryType.get(),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x05, 0x06})),
                        Literal.of(ByteBuffer.wrap(new byte[] {0x0e, 0x0f})), false),
                Types.NestedField.optional(14, "optional_col", Types.IntegerType.get()),
                Types.NestedField.required(15, "required_col", Types.IntegerType.get())));

        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(schema, 3, true, true);
        Map<String, Column> columns = context.getColumns().stream()
                .collect(Collectors.toMap(Column::getName, column -> column));

        Assertions.assertEquals("35", stringValue(context.resolveWriteDefault(columns.get("int_col"))));
        Assertions.assertEquals("false",
                stringValue(context.resolveWriteDefault(columns.get("boolean_col"))));
        Assertions.assertEquals("4900000001",
                stringValue(context.resolveWriteDefault(columns.get("long_col"))));
        Assertions.assertEquals("13.5",
                stringValue(context.resolveWriteDefault(columns.get("float_col"))));
        Assertions.assertEquals("456.75",
                stringValue(context.resolveWriteDefault(columns.get("double_col"))));
        Assertions.assertEquals("98765.4321",
                stringValue(context.resolveWriteDefault(columns.get("decimal_col"))));
        Assertions.assertEquals("write-default",
                stringValue(context.resolveWriteDefault(columns.get("string_col"))));
        Assertions.assertEquals("2025-01-18",
                stringValue(context.resolveWriteDefault(columns.get("date_col"))));
        Assertions.assertEquals("2025-01-18 01:02:03.654321",
                stringValue(context.resolveWriteDefault(columns.get("timestamp_col"))));
        Assertions.assertEquals("2025-01-18 01:02:03.654321+00:00",
                stringValue(context.resolveWriteDefault(columns.get("timestamptz_col"))));
        Assertions.assertArrayEquals(
                ByteBuffer.allocate(16)
                        .putLong(UUID.fromString("123e4567-e89b-12d3-a456-426614174001")
                                .getMostSignificantBits())
                        .putLong(UUID.fromString("123e4567-e89b-12d3-a456-426614174001")
                                .getLeastSignificantBits())
                        .array(),
                (byte[]) ((VarBinaryLiteral) context.resolveWriteDefault(
                        columns.get("uuid_col"))).getValue());
        Assertions.assertArrayEquals(new byte[] {0x0a, 0x0b, 0x0c, 0x0d},
                (byte[]) ((VarBinaryLiteral) context.resolveWriteDefault(
                        columns.get("fixed_col"))).getValue());
        Assertions.assertArrayEquals(new byte[] {0x0e, 0x0f},
                (byte[]) ((VarBinaryLiteral) context.resolveWriteDefault(
                        columns.get("binary_col"))).getValue());
        Assertions.assertTrue(context.resolveWriteDefault(columns.get("optional_col")) instanceof NullLiteral);
        Assertions.assertThrows(AnalysisException.class,
                () -> context.resolveWriteDefault(columns.get("required_col")));
        Assertions.assertEquals(17, context.getSchemaId());
        Assertions.assertEquals(schema.asStruct(), context.getSchema().asStruct());
        Assertions.assertEquals(SchemaParser.toJson(schema), context.getSchemaJson());
        Assertions.assertTrue(context.getMergeSchemaJson().contains("_row_id"));
        for (Column column : context.getColumns()) {
            if (!column.getName().equals("required_col")) {
                Expression defaultExpression = context.resolveWriteDefault(column);
                Assertions.assertEquals(DataType.fromCatalogType(column.getType()),
                        defaultExpression.getDataType(), column.getName());
                if (defaultExpression instanceof VarBinaryLiteral) {
                    Assertions.assertEquals(column.getType(),
                            ((VarBinaryLiteral) defaultExpression).toLegacyLiteral().getType(),
                            column.getName());
                }
            }
        }
    }

    @Test
    public void testLegacyTimestamptzWriteDefaultUsesSessionLocalWallTime() {
        long instantMicros = DateTimeUtil.isoTimestamptzToMicros(
                "2025-01-18T01:02:03.654321+00:00");
        Types.NestedField field = defaultField(
                1, "event_time", Types.TimestampType.withZone(),
                Literal.of(instantMicros), Literal.of(instantMicros), false);
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("Asia/Shanghai");
        context.setThreadLocalInfo();
        try {
            IcebergWriteSchemaContext writeContext = IcebergWriteSchemaContext.forSchema(
                    new Schema(field), 3, false, false);
            Assertions.assertEquals("2025-01-18 09:02:03.654321",
                    stringValue(writeContext.resolveWriteDefault(
                            writeContext.getColumns().get(0))));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testLegacyBinaryDefaultsDecodeRawBytesOnBackend() {
        byte[] bytes = new byte[] {(byte) 0x80, 0x00, (byte) 0xff};
        DataType binaryTarget = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                Types.BinaryType.get(), false, false));
        Expression binary = IcebergWriteSchemaContext.toDorisExpression(
                Types.BinaryType.get(), ByteBuffer.wrap(bytes), binaryTarget, false, false);
        assertUnhexBytes(binary, "8000FF");

        DataType uuidTarget = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                Types.UUIDType.get(), false, false));
        Expression uuid = IcebergWriteSchemaContext.toDorisExpression(
                Types.UUIDType.get(), UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                uuidTarget, false, false);
        assertUnhexBytes(uuid, "123E4567E89B12D3A456426614174000");
    }

    @Test
    public void testWriteDefaultResolutionDoesNotFallBackToReusedName() {
        Schema pinnedSchema = new Schema(18, ImmutableList.of(defaultField(
                7, "reused_name", Types.IntegerType.get(),
                Literal.of(7), Literal.of(9), false)));
        Schema replacementSchema = new Schema(19, ImmutableList.of(defaultField(
                8, "reused_name", Types.IntegerType.get(),
                Literal.of(70), Literal.of(90), false)));
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                pinnedSchema, 3, true, true);
        Column replacementColumn = IcebergUtils.parseField(
                replacementSchema.columns().get(0), true, true);

        Assertions.assertFalse(context.findField(replacementColumn).isPresent());
        Assertions.assertThrows(AnalysisException.class,
                () -> context.resolveWriteDefault(replacementColumn));
    }

    @Test
    public void testPinnedSinkContextIsReusedAcrossOverwriteDelegation() {
        Schema schema = new Schema(20, ImmutableList.of(defaultField(
                7, "value", Types.IntegerType.get(),
                Literal.of(7), Literal.of(9), false)));
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                schema, 3, true, true);
        UnboundOneRowRelation child = new UnboundOneRowRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of());
        UnboundIcebergTableSink<UnboundOneRowRelation> sink =
                new UnboundIcebergTableSink<>(
                        ImmutableList.of("catalog", "database", "table"), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), child)
                        .withWriteSchemaContext(context);
        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(targetTable.getId()).thenReturn(-1L);
        Mockito.when(targetTable.getName()).thenReturn("table");
        StatementContext statementContext = new StatementContext();

        LogicalPlan reused = InsertUtils.pinIcebergWriteSchema(
                sink, targetTable, Optional.empty(), statementContext);

        Assertions.assertSame(sink, reused);
        Assertions.assertSame(context, statementContext.getIcebergWriteSchemaContext().get());
        Assertions.assertThrows(IllegalStateException.class,
                () -> InsertUtils.pinIcebergWriteSchema(
                        sink, targetTable, Optional.of("other_branch"), statementContext));
    }

    @Test
    public void testRewriteSinkDoesNotPinOrConsumeWriteDefaults() {
        UnboundOneRowRelation child = new UnboundOneRowRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of());
        UnboundIcebergTableSink<UnboundOneRowRelation> rewriteSink =
                new UnboundIcebergTableSink<>(
                        ImmutableList.of("catalog", "database", "table"),
                        ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), child, true);
        StatementContext statementContext = new StatementContext();

        Assertions.assertSame(rewriteSink, InsertUtils.pinIcebergWriteSchema(
                rewriteSink, Mockito.mock(IcebergExternalTable.class), Optional.empty(), statementContext));
        Assertions.assertFalse(statementContext.getIcebergWriteSchemaContext().isPresent());
        Assertions.assertFalse(rewriteSink.getWriteSchemaContext().isPresent());
    }

    @Test
    public void testComplexWriteDefaultBuildsRecursiveLiteralTree() {
        Types.StructType structType = Types.StructType.of(
                Types.NestedField.required(101, "count", Types.IntegerType.get()),
                Types.NestedField.optional(102, "items",
                        Types.ListType.ofOptional(103, Types.StringType.get())),
                Types.NestedField.optional(104, "attributes",
                        Types.MapType.ofOptional(105, 106,
                                Types.StringType.get(), Types.IntegerType.get())));
        Map<String, Integer> attributes = new LinkedHashMap<>();
        attributes.put("one", 1);
        StructLike value = new ArrayStructLike(7, Arrays.asList("a", null), attributes);
        DataType targetType = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                structType, true, true));

        Expression expression = IcebergWriteSchemaContext.toDorisExpression(
                structType, value, targetType, true, true);
        Assertions.assertTrue(expression instanceof StructLiteral);
        List<?> fields = ((StructLiteral) expression).getValue();
        Assertions.assertEquals(7, ((IntegerLiteral) fields.get(0)).getValue());
        Assertions.assertTrue(fields.get(1) instanceof ArrayLiteral);
        List<?> items = ((ArrayLiteral) fields.get(1)).getValue();
        Assertions.assertEquals("a", ((StringLiteral) items.get(0)).getValue());
        Assertions.assertTrue(items.get(1) instanceof NullLiteral);
        Assertions.assertTrue(fields.get(2) instanceof MapLiteral);
        Assertions.assertEquals(1, ((MapLiteral) fields.get(2)).getValue().size());
    }

    @Test
    public void testComplexLegacyBinaryWriteDefaultKeepsRawLiteralBytes() {
        byte[] bytes = new byte[] {(byte) 0x80, 0x00, (byte) 0xff};
        Types.StructType structType = Types.StructType.of(
                Types.NestedField.required(101, "payload", Types.BinaryType.get()));
        StructLike value = new ArrayStructLike(ByteBuffer.wrap(bytes));
        DataType targetType = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                structType, false, false));

        Expression expression = IcebergWriteSchemaContext.toDorisExpression(
                structType, value, targetType, false, false);

        Assertions.assertEquals(targetType, expression.getDataType());
        Assertions.assertTrue(expression.anyMatch(node -> node instanceof Cast));
        Assertions.assertTrue(expression.anyMatch(node -> node instanceof Unhex));
        Unhex unhex = expression.collect(Unhex.class::isInstance).stream()
                .map(Unhex.class::cast).findFirst().orElseThrow(AssertionError::new);
        assertUnhexBytes(unhex, "8000FF");
        Expr legacyExpression = ExpressionTranslator.translate(
                expression, new PlanTranslatorContext());
        Assertions.assertEquals(targetType.toCatalogDataType(), legacyExpression.getType());
        TExpr thriftExpression = legacyExpression.treeToThrift();
        Assertions.assertTrue(thriftExpression.nodes.stream()
                .anyMatch(node -> node.node_type == TExprNodeType.FUNCTION_CALL));
        Assertions.assertFalse(thriftExpression.nodes.stream()
                .anyMatch(node -> node.node_type == TExprNodeType.VARBINARY_LITERAL));
    }

    @Test
    public void testTransactionPreflightRejectsSchemaSkew() {
        Schema pinned = new Schema(20,
                ImmutableList.of(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        Schema changed = new Schema(21,
                ImmutableList.of(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(pinned, 3, true, true);
        Table table = Mockito.mock(Table.class);
        stubUnpartitionedWriterMetadata(table);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
        Mockito.when(table.schema()).thenReturn(pinned);
        Assertions.assertDoesNotThrow(() -> context.validateCurrentSchema(table));
        Mockito.when(table.schema()).thenReturn(changed);
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> context.validateCurrentSchema(table));
        Assertions.assertTrue(exception.getMessage().contains("retry the statement"));
        Mockito.when(table.schema()).thenReturn(pinned);
        Mockito.when(table.specs()).thenReturn(ImmutableMap.of());
        Assertions.assertThrows(AnalysisException.class, () -> context.validateCurrentSchema(table));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Mockito.when(table.specs()).thenReturn(ImmutableMap.of(spec.specId(), spec));
        Mockito.when(table.sortOrders()).thenReturn(ImmutableMap.of());
        Assertions.assertThrows(AnalysisException.class, () -> context.validateCurrentSchema(table));
        Mockito.verify(table, Mockito.never()).refresh();
    }

    @Test
    public void testTransactionPreflightRejectsWriterContractSkew() {
        Schema schema = new Schema(20,
                ImmutableList.of(Types.NestedField.optional(
                        1, "id", Types.IntegerType.get())));
        Map<String, String> pinnedProperties = ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "3",
                TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                schema, 3, PartitionSpec.unpartitioned(), SortOrder.unsorted(),
                org.apache.iceberg.FileFormat.PARQUET,
                org.apache.iceberg.MetricsConfig.getDefault(),
                TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                "file:///tmp/test_table/data", pinnedProperties, true, true);
        Table table = Mockito.mock(Table.class);
        stubUnpartitionedWriterMetadata(table);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.properties()).thenReturn(pinnedProperties);
        Assertions.assertDoesNotThrow(() -> context.validateCurrentSchema(table));

        Mockito.when(table.properties()).thenReturn(ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "3",
                TableProperties.DEFAULT_FILE_FORMAT, "orc"));
        AnalysisException propertyException = Assertions.assertThrows(
                AnalysisException.class, () -> context.validateCurrentSchema(table));
        Assertions.assertTrue(
                propertyException.getMessage().contains("writer properties"),
                propertyException::getMessage);

        Mockito.when(table.properties()).thenReturn(pinnedProperties);
        Mockito.when(table.location()).thenReturn("file:///tmp/moved_table");
        AnalysisException locationException = Assertions.assertThrows(
                AnalysisException.class, () -> context.validateCurrentSchema(table));
        Assertions.assertTrue(
                locationException.getMessage().contains("data location"),
                locationException::getMessage);
    }

    @Test
    public void testTransactionPreflightRejectsRecreatedTableUuid() {
        Schema schema = new Schema(22,
                ImmutableList.of(Types.NestedField.optional(
                        1, "id", Types.IntegerType.get())));
        UUID pinnedUuid = UUID.fromString("00000000-0000-0000-0000-000000000001");
        IcebergWriteSchemaContext context =
                IcebergWriteSchemaContext.forSchemaWithUuidIdentity(schema, 3, pinnedUuid);
        Table table = Mockito.mock(Table.class);
        stubUnpartitionedWriterMetadata(table);
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.uuid()).thenReturn(pinnedUuid);
        Assertions.assertDoesNotThrow(() -> context.validateCurrentSchema(table));

        Mockito.when(table.uuid()).thenReturn(
                UUID.fromString("00000000-0000-0000-0000-000000000002"));
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> context.validateCurrentSchema(table));
        Assertions.assertTrue(
                exception.getMessage().contains("identity changed"), exception::getMessage);
    }

    @Test
    public void testV1MetadataIdentityAcceptsAncestorAndRejectsReplacement() {
        Schema schema = new Schema(0,
                ImmutableList.of(Types.NestedField.optional(
                        1, "id", Types.IntegerType.get())));
        String pinnedLocation = "file:///tmp/test_table/metadata/v1.metadata.json";
        long pinnedTimestamp = 1234L;
        Table table = Mockito.mock(
                Table.class, Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(((HasTableOperations) table).operations()).thenReturn(operations);
        stubUnpartitionedWriterMetadata(table);
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "1"));
        Mockito.when(table.schema()).thenReturn(schema);

        TableMetadata unchanged = Mockito.mock(TableMetadata.class);
        Mockito.when(unchanged.formatVersion()).thenReturn(1);
        Mockito.when(unchanged.metadataFileLocation()).thenReturn(pinnedLocation);
        Mockito.when(unchanged.lastUpdatedMillis()).thenReturn(pinnedTimestamp);
        Mockito.when(unchanged.previousFiles()).thenReturn(ImmutableList.of());
        Mockito.when(operations.current()).thenReturn(unchanged);

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(catalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(catalog.getEnableMappingTimestampTz()).thenReturn(true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getIcebergTable()).thenReturn(table);
        Mockito.when(dorisTable.getId()).thenReturn(9L);
        Mockito.when(dorisTable.getName()).thenReturn("v1_table");
        IcebergWriteSchemaContext context =
                IcebergWriteSchemaContext.create(dorisTable, Optional.empty());
        Assertions.assertDoesNotThrow(() -> context.validateCurrentSchema(table));

        TableMetadata.MetadataLogEntry pinnedEntry =
                Mockito.mock(TableMetadata.MetadataLogEntry.class);
        Mockito.when(pinnedEntry.file()).thenReturn(pinnedLocation);
        Mockito.when(pinnedEntry.timestampMillis()).thenReturn(pinnedTimestamp);
        TableMetadata advanced = Mockito.mock(TableMetadata.class);
        Mockito.when(advanced.formatVersion()).thenReturn(1);
        Mockito.when(advanced.metadataFileLocation()).thenReturn(
                "file:///tmp/test_table/metadata/v2.metadata.json");
        Mockito.when(advanced.lastUpdatedMillis()).thenReturn(2345L);
        Mockito.when(advanced.previousFiles()).thenReturn(ImmutableList.of(pinnedEntry));
        Mockito.when(operations.current()).thenReturn(advanced);
        Assertions.assertDoesNotThrow(() -> context.validateCurrentSchema(table));

        TableMetadata replacement = Mockito.mock(TableMetadata.class);
        Mockito.when(replacement.formatVersion()).thenReturn(1);
        Mockito.when(replacement.metadataFileLocation()).thenReturn(pinnedLocation);
        Mockito.when(replacement.lastUpdatedMillis()).thenReturn(3456L);
        Mockito.when(replacement.previousFiles()).thenReturn(ImmutableList.of());
        Mockito.when(operations.current()).thenReturn(replacement);
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> context.validateCurrentSchema(table));
        Assertions.assertTrue(
                exception.getMessage().contains("identity changed"), exception::getMessage);
    }

    @Test
    public void testCreatePinsSchemaFromStatementMvccSnapshot() {
        Schema pinnedSchema = new Schema(24,
                ImmutableList.of(defaultField(1, "value", Types.IntegerType.get(),
                        Literal.of(23), Literal.of(24), false)));
        Schema cachedTableSchema = new Schema(25,
                ImmutableList.of(defaultField(1, "value", Types.IntegerType.get(),
                        Literal.of(24), Literal.of(25), false)));
        UUID pinnedUuid = UUID.fromString("00000000-0000-0000-0000-000000000003");
        Table table = Mockito.mock(
                Table.class, Mockito.withSettings().extraInterfaces(HasTableOperations.class));
        TableOperations operations = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(((HasTableOperations) table).operations()).thenReturn(operations);
        Mockito.when(operations.current()).thenReturn(metadata);
        Mockito.when(metadata.uuid()).thenReturn(pinnedUuid.toString());
        Mockito.when(table.schema()).thenReturn(cachedTableSchema);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                pinnedSchema.schemaId(), pinnedSchema,
                cachedTableSchema.schemaId(), cachedTableSchema));
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
        stubUnpartitionedWriterMetadata(table);

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(catalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(catalog.getEnableMappingTimestampTz()).thenReturn(true);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getFullName()).thenReturn("test_db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getDatabase()).thenReturn(database);
        Mockito.when(dorisTable.getIcebergTable()).thenReturn(table);
        Mockito.when(dorisTable.getId()).thenReturn(8L);
        Mockito.when(dorisTable.getName()).thenReturn("mvcc_table");

        ConnectContext connectContext = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        connectContext.setStatementContext(statementContext);
        connectContext.setThreadLocalInfo();
        statementContext.setSnapshot(new MvccTableInfo(dorisTable), new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(),
                        new IcebergSnapshot(101L, pinnedSchema.schemaId()))));
        try {
            IcebergWriteSchemaContext context = IcebergWriteSchemaContext.create(
                    dorisTable, Optional.empty());
            Assertions.assertEquals(pinnedSchema.schemaId(), context.getSchemaId());
            Assertions.assertEquals("24",
                    stringValue(context.resolveWriteDefault(context.getColumns().get(0))));
            Mockito.verify(table, Mockito.never()).refresh();

            Table replacement = Mockito.mock(Table.class);
            stubUnpartitionedWriterMetadata(replacement);
            Mockito.when(replacement.properties()).thenReturn(
                    ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
            Mockito.when(replacement.schema()).thenReturn(pinnedSchema);
            Mockito.when(replacement.uuid()).thenReturn(
                    UUID.fromString("00000000-0000-0000-0000-000000000004"));
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> context.validateCurrentSchema(replacement));
            Assertions.assertTrue(
                    exception.getMessage().contains("identity changed"), exception::getMessage);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testBranchPinsSnapshotSchemaWithoutRefreshingSharedTable() {
        Schema mainSchema = new Schema(30, ImmutableList.of(
                defaultField(1, "value", Types.IntegerType.get(),
                        Literal.of(30), Literal.of(31), false),
                defaultField(2, "main_only_required", Types.IntegerType.get(),
                        Literal.of(40), Literal.of(41), true)));
        Schema branchSchema = new Schema(29,
                ImmutableList.of(defaultField(1, "value", Types.IntegerType.get(),
                        Literal.of(28), Literal.of(29), false)));
        Snapshot branchSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(branchSnapshot.schemaId()).thenReturn(branchSchema.schemaId());
        SnapshotRef branchRef = SnapshotRef.branchBuilder(101L).build();

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(mainSchema);
        Mockito.when(table.refs()).thenReturn(ImmutableMap.of("audit", branchRef));
        Mockito.when(table.snapshot(branchRef.snapshotId())).thenReturn(branchSnapshot);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                mainSchema.schemaId(), mainSchema,
                branchSchema.schemaId(), branchSchema));
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
        stubUnpartitionedWriterMetadata(table);

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(catalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(catalog.getEnableMappingTimestampTz()).thenReturn(true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getIcebergTable()).thenReturn(table);
        Mockito.when(dorisTable.getId()).thenReturn(7L);
        Mockito.when(dorisTable.getName()).thenReturn("branch_table");

        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.create(
                dorisTable, Optional.of("audit"));
        Assertions.assertEquals(branchSchema.schemaId(), context.getSchemaId());
        Assertions.assertEquals("29",
                stringValue(context.resolveWriteDefault(context.getColumns().get(0))));
        Assertions.assertEquals(Optional.of("audit"), context.getBranchName());
        Assertions.assertDoesNotThrow(() -> context.validateCurrentSchema(table));
        Mockito.verify(table, Mockito.never()).refresh();

        UnboundOneRowRelation child = new UnboundOneRowRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of());
        UnboundIcebergTableSink<UnboundOneRowRelation> sink =
                new UnboundIcebergTableSink<>(
                        ImmutableList.of("catalog", "database", "branch_table"),
                        ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), child).withWriteSchemaContext(context);
        StatementContext statementContext = new StatementContext();
        Assertions.assertSame(sink, InsertUtils.pinIcebergWriteSchema(
                sink, dorisTable, Optional.of("audit"), statementContext));
        Assertions.assertSame(context, statementContext.getIcebergWriteSchemaContext().get());
    }

    @Test
    public void testBranchRejectsConcurrentCurrentRequiredFieldBeforeCommit() {
        // The branch field can contain an explicit NULL even though the current required field
        // retains a non-null initial default.
        Schema branchSchema = new Schema(32,
                ImmutableList.of(Types.NestedField.optional(
                        1, "branch_value", Types.IntegerType.get())));
        Schema currentSchema = new Schema(33, ImmutableList.of(defaultField(
                1, "branch_value", Types.IntegerType.get(),
                Literal.of(7), Literal.of(7), true)));
        Schema missingRequiredCurrentSchema = new Schema(36, ImmutableList.of(
                Types.NestedField.optional(1, "branch_value", Types.IntegerType.get()),
                Types.NestedField.required(
                        2, "required_current", Types.IntegerType.get())));
        Snapshot branchSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(branchSnapshot.schemaId()).thenReturn(branchSchema.schemaId());
        SnapshotRef branchRef = SnapshotRef.branchBuilder(103L).build();

        Table table = Mockito.mock(Table.class);
        AtomicReference<Schema> tableSchema = new AtomicReference<>(branchSchema);
        Mockito.when(table.schema()).thenAnswer(invocation -> tableSchema.get());
        Mockito.when(table.refs()).thenReturn(ImmutableMap.of("audit", branchRef));
        Mockito.when(table.snapshot(branchRef.snapshotId())).thenReturn(branchSnapshot);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                branchSchema.schemaId(), branchSchema,
                currentSchema.schemaId(), currentSchema,
                missingRequiredCurrentSchema.schemaId(), missingRequiredCurrentSchema));
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
        stubUnpartitionedWriterMetadata(table);

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(catalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(catalog.getEnableMappingTimestampTz()).thenReturn(true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getIcebergTable()).thenReturn(table);
        Mockito.when(dorisTable.getId()).thenReturn(10L);
        Mockito.when(dorisTable.getName()).thenReturn("branch_table");

        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.create(
                dorisTable, Optional.of("audit"));
        tableSchema.set(currentSchema);
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> context.validateCurrentSchema(table));
        Assertions.assertTrue(
                exception.getMessage().contains("branch_value"), exception::getMessage);
        Assertions.assertTrue(
                exception.getMessage().contains("explicit nulls"), exception::getMessage);
        Assertions.assertTrue(
                exception.getMessage().contains("current schema 33"), exception::getMessage);
        Assertions.assertTrue(
                exception.getMessage().contains("pinned branch audit schema 32"),
                exception::getMessage);

        tableSchema.set(missingRequiredCurrentSchema);
        AnalysisException missingFieldException = Assertions.assertThrows(
                AnalysisException.class, () -> context.validateCurrentSchema(table));
        Assertions.assertTrue(
                missingFieldException.getMessage().contains("required_current"),
                missingFieldException::getMessage);
        Assertions.assertTrue(
                missingFieldException.getMessage().contains("no initial default"),
                missingFieldException::getMessage);
        Assertions.assertTrue(
                missingFieldException.getMessage().contains("current schema 36"),
                missingFieldException::getMessage);
    }

    @Test
    public void testBranchRejectsCurrentRequiredFieldDuringPlanning() {
        // The branch field can contain an explicit NULL even though the current required field
        // retains a non-null initial default.
        Schema branchSchema = new Schema(34,
                ImmutableList.of(Types.NestedField.optional(
                        1, "branch_value", Types.IntegerType.get())));
        Schema currentSchema = new Schema(35, ImmutableList.of(defaultField(
                1, "branch_value", Types.IntegerType.get(),
                Literal.of(7), Literal.of(7), true)));
        Schema missingRequiredCurrentSchema = new Schema(37, ImmutableList.of(
                Types.NestedField.optional(1, "branch_value", Types.IntegerType.get()),
                Types.NestedField.required(
                        2, "required_current", Types.IntegerType.get())));
        Snapshot branchSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(branchSnapshot.schemaId()).thenReturn(branchSchema.schemaId());
        SnapshotRef branchRef = SnapshotRef.branchBuilder(104L).build();

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(currentSchema);
        Mockito.when(table.refs()).thenReturn(ImmutableMap.of("audit", branchRef));
        Mockito.when(table.snapshot(branchRef.snapshotId())).thenReturn(branchSnapshot);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                branchSchema.schemaId(), branchSchema,
                currentSchema.schemaId(), currentSchema,
                missingRequiredCurrentSchema.schemaId(), missingRequiredCurrentSchema));
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
        stubUnpartitionedWriterMetadata(table);

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(catalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(catalog.getEnableMappingTimestampTz()).thenReturn(true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getIcebergTable()).thenReturn(table);
        Mockito.when(dorisTable.getId()).thenReturn(11L);
        Mockito.when(dorisTable.getName()).thenReturn("branch_table");

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> IcebergWriteSchemaContext.create(
                        dorisTable, Optional.of("audit")));
        Assertions.assertTrue(
                exception.getMessage().contains("branch_value"), exception::getMessage);
        Assertions.assertTrue(
                exception.getMessage().contains("explicit nulls"), exception::getMessage);
        Assertions.assertTrue(
                exception.getMessage().contains("current schema 35"), exception::getMessage);
        Assertions.assertTrue(
                exception.getMessage().contains("pinned branch audit schema 34"),
                exception::getMessage);

        Mockito.when(table.schema()).thenReturn(missingRequiredCurrentSchema);
        AnalysisException missingFieldException = Assertions.assertThrows(
                AnalysisException.class,
                () -> IcebergWriteSchemaContext.create(
                        dorisTable, Optional.of("audit")));
        Assertions.assertTrue(
                missingFieldException.getMessage().contains("required_current"),
                missingFieldException::getMessage);
        Assertions.assertTrue(
                missingFieldException.getMessage().contains("no initial default"),
                missingFieldException::getMessage);
        Assertions.assertTrue(
                missingFieldException.getMessage().contains("current schema 37"),
                missingFieldException::getMessage);
    }

    @Test
    public void testBranchRejectsCurrentPartitionSourceOutsidePinnedSchema() {
        Schema mainSchema = new Schema(31,
                ImmutableList.of(Types.NestedField.optional(1, "main_partition", Types.IntegerType.get())));
        Schema branchSchema = new Schema(30,
                ImmutableList.of(Types.NestedField.required(2, "branch_value", Types.IntegerType.get())));
        PartitionSpec mainSpec = PartitionSpec.builderFor(mainSchema).identity("main_partition").build();
        Snapshot branchSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(branchSnapshot.schemaId()).thenReturn(branchSchema.schemaId());
        SnapshotRef branchRef = SnapshotRef.branchBuilder(102L).build();

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(mainSchema);
        Mockito.when(table.refs()).thenReturn(ImmutableMap.of("audit", branchRef));
        Mockito.when(table.snapshot(branchRef.snapshotId())).thenReturn(branchSnapshot);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                mainSchema.schemaId(), mainSchema,
                branchSchema.schemaId(), branchSchema));
        Mockito.when(table.spec()).thenReturn(mainSpec);
        Mockito.when(table.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
        Mockito.when(table.location()).thenReturn("file:///tmp/branch_table");
        Mockito.when(table.uuid()).thenReturn(
                UUID.fromString("00000000-0000-0000-0000-000000000001"));

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(catalog.getEnableMappingVarbinary()).thenReturn(true);
        Mockito.when(catalog.getEnableMappingTimestampTz()).thenReturn(true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getIcebergTable()).thenReturn(table);
        Mockito.when(dorisTable.getId()).thenReturn(9L);
        Mockito.when(dorisTable.getName()).thenReturn("branch_table");

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class,
                () -> IcebergWriteSchemaContext.create(dorisTable, Optional.of("audit")));
        Assertions.assertTrue(exception.getMessage().contains("pinned"), exception::getMessage);
        Assertions.assertTrue(exception.getMessage().contains("source field 1"), exception::getMessage);
    }

    private static void stubUnpartitionedWriterMetadata(Table table) {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        SortOrder sortOrder = SortOrder.unsorted();
        Mockito.when(table.spec()).thenReturn(spec);
        Mockito.when(table.specs()).thenReturn(ImmutableMap.of(spec.specId(), spec));
        Mockito.when(table.sortOrder()).thenReturn(sortOrder);
        Mockito.when(table.sortOrders()).thenReturn(ImmutableMap.of(sortOrder.orderId(), sortOrder));
        Mockito.when(table.location()).thenReturn("file:///tmp/test_table");
        Mockito.when(table.uuid()).thenReturn(
                UUID.fromString("00000000-0000-0000-0000-000000000001"));
    }

    private static Types.NestedField defaultField(int id, String name, Type type,
            Literal<?> initialDefault, Literal<?> writeDefault, boolean required) {
        return Types.NestedField.builder()
                .withId(id)
                .withName(name)
                .isOptional(!required)
                .ofType(type)
                .withInitialDefault(initialDefault)
                .withWriteDefault(writeDefault)
                .build();
    }

    private static String stringValue(Expression expression) {
        return ((org.apache.doris.nereids.trees.expressions.literal.Literal) expression).getStringValue();
    }

    private static void assertUnhexBytes(Expression expression, String expectedHex) {
        Expression rawBytes = expression instanceof Cast ? expression.child(0) : expression;
        Assertions.assertTrue(rawBytes instanceof Unhex);
        Assertions.assertEquals(expectedHex, ((StringLiteral) rawBytes.child(0)).getValue());
    }

    private static final class ArrayStructLike implements StructLike {
        private final Object[] values;

        private ArrayStructLike(Object... values) {
            this.values = values;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public <T> void set(int pos, T value) {
            values[pos] = value;
        }
    }
}
