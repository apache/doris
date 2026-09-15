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

package org.apache.doris.paimon;

import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PaimonWriteSchemaTest {

    @Test
    public void testChangelogOperationsSetPaimonRowKind() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "id", "name", "score", "region"},
                true);

        Assertions.assertEquals(RowKind.INSERT,
                changelogRow(schema, PaimonWriteSchema.INSERT_OPERATION).getRowKind());
        Assertions.assertEquals(RowKind.UPDATE_AFTER,
                changelogRow(schema, PaimonWriteSchema.UPDATE_OPERATION).getRowKind());
        Assertions.assertEquals(RowKind.DELETE,
                changelogRow(schema, PaimonWriteSchema.DELETE_OPERATION).getRowKind());
    }

    @Test
    public void testUnknownChangelogOperationRejected() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType(),
                new String[] {PaimonWriteSchema.ROW_KIND_COLUMN, "id"},
                true);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> tableRow(schema, (byte) 9, 1));
    }

    @Test
    public void testReorderedInputProducesTableSchemaRow() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType(),
                new String[] {"region", "score", "name", "id"});
        Object[] values = new Object[] {
                BinaryString.fromString("south"),
                86.5D,
                BinaryString.fromString("erin"),
                5
        };

        InternalRow tableRow = tableRow(schema, values);

        Assertions.assertEquals(5, tableRow.getInt(0));
        Assertions.assertEquals("erin", tableRow.getString(1).toString());
        Assertions.assertEquals(86.5D, tableRow.getDouble(2));
        Assertions.assertEquals("south", tableRow.getString(3).toString());
    }

    @Test
    public void testPartialInputLeavesMissingTableFieldsNull() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType(),
                new String[] {"region", "id"});
        Object[] values = new Object[] {
                BinaryString.fromString("east"),
                6
        };

        InternalRow tableRow = tableRow(schema, values);

        Assertions.assertEquals(6, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
        Assertions.assertTrue(tableRow.isNullAt(2));
        Assertions.assertEquals("east", tableRow.getString(3).toString());
    }

    @Test
    public void testUnknownColumnRejectedDuringInitialization() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonWriteSchema.create(tableType(), new String[] {"unknown"}));

        Assertions.assertTrue(exception.getMessage().contains("unknown"));
    }

    @Test
    public void testOmittedNotNullDefaultIsAppliedBeforeTableWriter() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType(false, VarCharType.MAX_LENGTH),
                        null, "unknown")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"id"});

        InternalRow tableRow = tableRow(schema, 7);

        Assertions.assertEquals(7, tableRow.getInt(0));
        Assertions.assertEquals("unknown", tableRow.getString(1).toString());
    }

    @Test
    public void testOmittedNotNullWithoutDefaultIsLeftForTableWriterValidation() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", DataTypes.STRING().notNull())));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"id"});

        InternalRow tableRow = tableRow(schema, 7);

        Assertions.assertEquals(7, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
    }

    @Test
    public void testReorderedInputOverridesDefaultsWhileOmittedFieldUsesDefault() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", DataTypes.STRING(), null, "unknown"),
                new DataField(2, "score", new DoubleType()),
                new DataField(3, "region", DataTypes.STRING(), null, "north")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType,
                new String[] {"region", "score", "id"});

        InternalRow tableRow = tableRow(schema,
                BinaryString.fromString("south"),
                92.5D,
                8);

        Assertions.assertEquals(8, tableRow.getInt(0));
        Assertions.assertEquals("unknown", tableRow.getString(1).toString());
        Assertions.assertEquals(92.5D, tableRow.getDouble(2));
        Assertions.assertEquals("south", tableRow.getString(3).toString());
    }

    @Test
    public void testExplicitNullIsNotReplacedByOmittedFieldDefault() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", DataTypes.STRING(), null, "unknown")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType,
                new String[] {"name", "id"});

        InternalRow tableRow = tableRow(schema,
                null,
                9);

        Assertions.assertEquals(9, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
    }

    @Test
    public void testExplicitNullForRequiredFieldReachesWriterValidation() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "payload", DataTypes.VARIANT().notNull())));
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType, new String[] {"id", "payload"});

        Assertions.assertTrue(schema.inputType().getTypeAt(1).isNullable());
        InternalRow tableRow = tableRow(schema, 9, null);

        Assertions.assertEquals(9, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
    }

    @Test
    public void testPaimonWriterDefaultsExplicitNullRouteFields() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "bucket_key", new IntType(), null, "1"),
                new DataField(1, "partition_key", new VarCharType(VarCharType.MAX_LENGTH),
                        null, "default-partition")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(
                tableType, new String[] {"bucket_key", "partition_key"});

        InternalRow tableRow = tableRow(schema, null, null);
        Assertions.assertTrue(tableRow.isNullAt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));

        DefaultValueRow defaultValueRow = DefaultValueRow.create(tableType);
        Assertions.assertNotNull(defaultValueRow);
        InternalRow writerRow = defaultValueRow.replaceRow(tableRow);
        Assertions.assertEquals(1, writerRow.getInt(0));
        Assertions.assertEquals("default-partition", writerRow.getString(1).toString());
    }

    @Test
    public void testOmittedComplexDefaultsUsePaimonInternalValues() {
        RowType nestedType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.INT()),
                new DataField(1, "numbers", DataTypes.ARRAY(DataTypes.INT()), null, "[1, 2, 3]"),
                new DataField(2, "properties",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), null, "{one -> 1, two -> 2}"),
                new DataField(3, "nested", nestedType, null, "{42, default-value}")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"id"});

        InternalRow tableRow = tableRow(schema, 10);

        InternalArray numbers = tableRow.getArray(1);
        Assertions.assertEquals(3, numbers.size());
        Assertions.assertEquals(1, numbers.getInt(0));
        Assertions.assertEquals(3, numbers.getInt(2));

        InternalMap properties = tableRow.getMap(2);
        Assertions.assertEquals(2, properties.size());
        Map<String, Integer> actualProperties = new HashMap<>();
        for (int i = 0; i < properties.size(); i++) {
            actualProperties.put(
                    properties.keyArray().getString(i).toString(),
                    properties.valueArray().getInt(i));
        }
        Assertions.assertEquals(1, actualProperties.get("one"));
        Assertions.assertEquals(2, actualProperties.get("two"));

        InternalRow nested = tableRow.getRow(3, 2);
        Assertions.assertEquals(42, nested.getInt(0));
        Assertions.assertEquals("default-value", nested.getString(1).toString());
    }

    @Test
    public void testDuplicateColumnRejectedDuringInitialization() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonWriteSchema.create(tableType(), new String[] {"id", "id"}));

        Assertions.assertTrue(exception.getMessage().contains("Duplicate"));
    }

    private static RowType tableType() {
        return new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType()),
                new DataField(2, "score", new DoubleType()),
                new DataField(3, "region", new VarCharType())));
    }

    private static InternalRow changelogRow(PaimonWriteSchema schema, byte operation) {
        return tableRow(schema,
                operation,
                11,
                BinaryString.fromString("value"),
                1.0D,
                BinaryString.fromString("east"));
    }

    private static InternalRow tableRow(PaimonWriteSchema schema, Object... values) {
        return schema.tableRow(GenericRow.of(values));
    }
}
