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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class PaimonWriteSchemaTest {

    @Test
    public void testReorderedInputProducesTableSchemaRow() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType(),
                new String[] {"region", "score", "name", "id"});
        Object[][] values = new Object[][] {
                {BinaryString.fromString("south")},
                {86.5D},
                {BinaryString.fromString("erin")},
                {5}
        };

        InternalRow tableRow = schema.tableRow(values, 0);

        Assertions.assertEquals(5, tableRow.getInt(0));
        Assertions.assertEquals("erin", tableRow.getString(1).toString());
        Assertions.assertEquals(86.5D, tableRow.getDouble(2));
        Assertions.assertEquals("south", tableRow.getString(3).toString());
        Assertions.assertFalse(schema.isPartial());
    }

    @Test
    public void testPartialInputLeavesMissingTableFieldsNull() {
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType(),
                new String[] {"region", "id"});
        Object[][] values = new Object[][] {
                {BinaryString.fromString("east")},
                {6}
        };

        InternalRow tableRow = schema.tableRow(values, 0);

        Assertions.assertEquals(6, tableRow.getInt(0));
        Assertions.assertTrue(tableRow.isNullAt(1));
        Assertions.assertTrue(tableRow.isNullAt(2));
        Assertions.assertEquals("east", tableRow.getString(3).toString());
        Assertions.assertTrue(schema.isPartial());
    }

    @Test
    public void testUnknownColumnRejectedDuringInitialization() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonWriteSchema.create(tableType(), new String[] {"unknown"}));

        Assertions.assertTrue(exception.getMessage().contains("unknown"));
    }

    @Test
    public void testPartialInputMaterializesOmittedDefaultValue() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType(false, VarCharType.MAX_LENGTH),
                        null, "unknown")));
        PaimonWriteSchema schema = PaimonWriteSchema.create(tableType, new String[] {"id"});

        InternalRow tableRow = schema.tableRow(new Object[][] {{7}}, 0);

        Assertions.assertEquals(7, tableRow.getInt(0));
        Assertions.assertEquals("unknown", tableRow.getString(1).toString());
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
}
