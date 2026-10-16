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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.TimeStampNsType;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Set;

public class IcebergTimestampNsTest {
    private Table createTable(Schema schema, PartitionSpec spec) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        return catalog.createTable(TableIdentifier.of("db", "events"), schema, spec,
                Collections.singletonMap("format-version", "3"));
    }

    @Test
    public void roundTripMappingAndNestedTypes() {
        Assertions.assertEquals(Type.TIMESTAMP_NS,
                IcebergUtils.icebergTypeToDorisType(Types.TimestampNanoType.withoutZone(), false, false));
        Assertions.assertEquals(Type.UNSUPPORTED,
                IcebergUtils.icebergTypeToDorisType(Types.TimestampNanoType.withZone(), false, false));
        Assertions.assertEquals(Types.TimestampNanoType.withoutZone(),
                new DorisTypeToIcebergType().atomic(Type.TIMESTAMP_NS));
        org.apache.doris.catalog.ArrayType array = (org.apache.doris.catalog.ArrayType)
                IcebergUtils.icebergTypeToDorisType(
                        Types.ListType.ofOptional(1, Types.TimestampNanoType.withoutZone()), false, false);
        Assertions.assertEquals(Type.TIMESTAMP_NS, array.getItemType());
    }

    @Test
    public void partitionsDefaultsAndLiteralsPreserveSignedNanoseconds() {
        for (long nanos : new long[] {Long.MIN_VALUE, -1, 0, 1, 123456789, Long.MAX_VALUE}) {
            String text = IcebergUtils.serializePartitionValue(Types.TimestampNanoType.withoutZone(), nanos, "UTC");
            Assertions.assertEquals(nanos, IcebergTimestampUtils.parseNanos(text, ZoneOffset.UTC));
            Assertions.assertEquals(nanos, IcebergUtils.parseIcebergLiteral(
                    text, Types.TimestampNanoType.withoutZone()).value());
            Assertions.assertEquals(nanos, IcebergUtils.parseIcebergLiteral(
                    Long.toString(nanos), Types.TimestampNanoType.withoutZone()).value());
            org.apache.doris.nereids.trees.expressions.Expression literal = IcebergWriteSchemaContext.toDorisExpression(
                    Types.TimestampNanoType.withoutZone(), nanos, TimeStampNsType.INSTANCE, false, false);
            Assertions.assertEquals(nanos, IcebergTimestampUtils.parseNanos(
                    ((org.apache.doris.nereids.trees.expressions.literal.Literal) literal).getStringValue(),
                    ZoneOffset.UTC));
            Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampNanoType.withoutZone()));
            Object converted = IcebergUtils.extractDorisLiteral(
                    Types.TimestampNanoType.withoutZone(), new StringLiteral(text));
            org.apache.iceberg.expressions.Expression bound = Binder.bind(schema.asStruct(),
                    org.apache.iceberg.expressions.Expressions.equal("ts", converted), true);
            Assertions.assertEquals(nanos, ((BoundPredicate<?>) bound).asLiteralPredicate().literal().value());
        }
        Assertions.assertThrows(ArithmeticException.class, () -> IcebergTimestampUtils.parseNanos(
                "2262-04-11 23:47:16.854775808", ZoneOffset.UTC));
    }

    @Test
    public void offsetAndDateOnlyPartitionsMatchSinkCasts() {
        Assertions.assertEquals(IcebergTimestampUtils.parseNanos("2024-01-01 00:00:00", ZoneOffset.UTC),
                IcebergTimestampUtils.parseNanos("2024-01-01", ZoneOffset.UTC));
        Assertions.assertEquals(IcebergTimestampUtils.parseNanos(
                "2023-12-31 16:00:00.123456789", ZoneOffset.UTC), IcebergTimestampUtils.parseNanos(
                        "2024-01-01 00:00:00.123456789+08:00", ZoneOffset.UTC));
        Assertions.assertEquals(IcebergTimestampUtils.parseNanos(
                "2024-01-01 00:00:00.123456789", ZoneOffset.UTC), IcebergTimestampUtils.parseNanos(
                        "2024-01-01 00:00:00.123456789+08:00", ZoneId.of("Asia/Shanghai")));
    }

    @Test
    public void historicalDateMetricsAndNegationCannotLoseRows() throws Exception {
        Schema date = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts", Types.DateType.get()));
        Table table = createTable(date, PartitionSpec.unpartitioned());
        TableOperations ops = ((HasTableOperations) table).operations();
        TableMetadata old = ops.current();
        ops.commit(old, TableMetadata.buildFrom(old).setCurrentSchema(new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withoutZone())), 2).build());
        table.refresh();
        Set<Integer> unsafe = IcebergTimestampUtils.unsafeNanosFields(table, table.schema());
        Assertions.assertEquals(Collections.singleton(2), unsafe);
        BinaryPredicate ts = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "ts"), new StringLiteral("2024-01-02"));
        BinaryPredicate id = new BinaryPredicate(BinaryPredicate.Operator.GT,
                new SlotRef(null, "id"), new IntLiteral(0));
        CompoundPredicate not = new CompoundPredicate(CompoundPredicate.Operator.NOT,
                new CompoundPredicate(CompoundPredicate.Operator.AND, ts, id), null);
        // Negating an unsafe conjunct must not turn NOT(ts=... AND id>0) into NOT(id>0).
        Assertions.assertNull(IcebergUtils.convertToIcebergExpr(not, table.schema(), unsafe));
        Assertions.assertNotNull(IcebergUtils.convertToIcebergExpr(id, table.schema(), unsafe));
        Table cleaned = org.mockito.Mockito.spy(table);
        org.mockito.Mockito.doReturn(Collections.singletonMap(1, table.schema())).when(cleaned).schemas();
        Assertions.assertEquals(unsafe, IcebergTimestampUtils.unsafeNanosFields(cleaned, table.schema()));
    }

    @Test
    public void failedNanosLiteralCannotBeNegatedAfterPartialConversion() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withoutZone()));
        BinaryPredicate invalid = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "ts"), new StringLiteral("2262-04-11 23:47:16.854775808"));
        BinaryPredicate id = new BinaryPredicate(BinaryPredicate.Operator.GT,
                new SlotRef(null, "id"), new IntLiteral(0));
        CompoundPredicate partial = new CompoundPredicate(CompoundPredicate.Operator.AND, invalid, id);
        Assertions.assertNotNull(IcebergUtils.convertToIcebergExpr(partial, schema));
        Assertions.assertNull(IcebergUtils.convertToIcebergExpr(
                new CompoundPredicate(CompoundPredicate.Operator.NOT, partial, null), schema));
        Assertions.assertNull(IcebergUtils.extractDorisLiteral(
                Types.TimestampNanoType.withoutZone(), new StringLiteral("invalid")));
        Assertions.assertNull(IcebergUtils.extractDorisLiteral(
                Types.TimestampNanoType.withoutZone(), new org.apache.doris.analysis.NullLiteral()));
    }

    @Test
    public void staticOverwriteUsesNanosAndRejectsHistoricalDateBounds() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampNanoType.withoutZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("ts").build();
        Table table = createTable(schema, spec);
        IcebergTransaction transaction = new IcebergTransaction(null);
        Field field = IcebergTransaction.class.getDeclaredField("table");
        field.setAccessible(true);
        field.set(transaction, table);
        org.apache.iceberg.expressions.Expression filter = transaction.buildPartitionFilter(
                Collections.singletonMap("ts", "2024-01-02 03:04:05.123456789"), spec, table.schema());
        Assertions.assertEquals(1704164645123456789L,
                ((UnboundPredicate<?>) filter).literal().value());
        TableOperations ops = ((HasTableOperations) table).operations();
        TableMetadata old = ops.current();
        ops.commit(old, TableMetadata.buildFrom(old).setCurrentSchema(new Schema(
                Types.NestedField.optional(1, "ts", Types.DateType.get())), 1).build());
        old = ops.current();
        ops.commit(old, TableMetadata.buildFrom(old).setCurrentSchema(schema, 1).build());
        table.refresh();
        Assertions.assertThrows(IllegalArgumentException.class, () -> transaction.buildPartitionFilter(
                Collections.singletonMap("ts", "2024-01-02"), spec, schema));
    }

    @Test
    public void calendarPartitionsAvoidOutOfRangeMvBoundaries() {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampNanoType.withoutZone()));
        for (PartitionSpec spec : new PartitionSpec[] {PartitionSpec.builderFor(schema).year("ts").build(),
                PartitionSpec.builderFor(schema).month("ts").build(),
                PartitionSpec.builderFor(schema).day("ts").build(),
                PartitionSpec.builderFor(schema).hour("ts").build()}) {
            Assertions.assertFalse(IcebergUtils.isValidRelatedTable(createTable(schema, spec)));
        }
    }
}
