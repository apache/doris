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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PaimonUtilTest {
    @Test
    public void testSchemaForVarcharAndChar() {
        DataField c1 = new DataField(1, "c1", new VarCharType(32));
        DataField c2 = new DataField(2, "c2", new CharType(14));
        Type type1 = PaimonUtil.paimonTypeToDorisType(c1.type(), true, true);
        Type type2 = PaimonUtil.paimonTypeToDorisType(c2.type(), true, true);
        Assert.assertTrue(type1.isVarchar());
        Assert.assertEquals(32, type1.getLength());
        Assert.assertEquals(14, type2.getLength());
    }

    @Test
    public void testParseSchemaPreservesNonLowercaseColumnNames() {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "mIxEd_COL", DataTypes.INT()),
                DataTypes.FIELD(1, "PART", DataTypes.STRING()));

        List<Column> columns = PaimonUtil.parseSchema(rowType, Collections.singletonList("PART"), false, false);

        Assert.assertEquals("mIxEd_COL", columns.get(0).getName());
        Assert.assertEquals("PART", columns.get(1).getName());
        Assert.assertTrue(columns.get(1).isKey());
    }

    @Test
    public void testGetPartitionInfoMapPreservesNonLowercaseKeys() {
        DataField mixedCasePartition = DataTypes.FIELD(0, "Dt", DataTypes.STRING());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.name()).thenReturn("mock_table");
        Mockito.when(table.partitionKeys()).thenReturn(Collections.singletonList("Dt"));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(mixedCasePartition));

        BinaryRow partitionValues = BinaryRow.singleColumn(BinaryString.fromString("2026-05-26"));

        Map<String, String> partitionInfoMap = PaimonUtil.getPartitionInfoMap(table, partitionValues, "UTC");

        Assert.assertFalse(partitionInfoMap.containsKey("dt"));
        Assert.assertEquals("2026-05-26", partitionInfoMap.get("Dt"));
    }

    @Test
    public void testGetPartitionInfoMapSupportsFloatingPointPartitions() {
        DataField floatPartition = DataTypes.FIELD(0, "float_partition", DataTypes.FLOAT());
        DataField doublePartition = DataTypes.FIELD(1, "double_partition", DataTypes.DOUBLE());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.name()).thenReturn("mock_table");
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.asList("float_partition", "double_partition"));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(floatPartition, doublePartition));

        float floatValue = Math.nextUp(0.1F);
        double doubleValue = Math.nextUp(0.1D);
        BinaryRow partitionValues = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partitionValues);
        writer.writeFloat(0, floatValue);
        writer.writeDouble(1, doubleValue);
        writer.complete();

        Map<String, String> partitionInfoMap = PaimonUtil.getPartitionInfoMap(
                table, partitionValues, "UTC");

        String serializedFloat = partitionInfoMap.get("float_partition");
        String serializedDouble = partitionInfoMap.get("double_partition");
        Assert.assertEquals(Float.toString(floatValue), serializedFloat);
        Assert.assertEquals(Double.toString(doubleValue), serializedDouble);
        Assert.assertEquals(Float.floatToIntBits(floatValue),
                Float.floatToIntBits(Float.parseFloat(serializedFloat)));
        Assert.assertEquals(Double.doubleToLongBits(doubleValue),
                Double.doubleToLongBits(Double.parseDouble(serializedDouble)));
    }
}
