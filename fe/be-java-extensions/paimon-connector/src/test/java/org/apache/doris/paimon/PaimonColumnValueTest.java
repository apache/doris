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

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaimonColumnValueTest {
    private final RowType paimonStructType = RowType.of(
            new DataType[] {new IntType(), new VarCharType(), new BigIntType()},
            new String[] {"a", "b", "c"});
    private final ColumnType dorisStructType =
            ColumnType.parseType("s", "struct<a:int,b:string,c:bigint>");

    @Test
    public void testUnpackNonLeadingStructField() {
        PaimonColumnValue structValue = createStructValue();
        List<ColumnValue> values = new ArrayList<>();

        structValue.unpackStruct(Collections.singletonList(2), values);

        Assert.assertEquals(1, values.size());
        Assert.assertEquals(100L, values.get(0).getLong());
    }

    @Test
    public void testUnpackNonContiguousStructFields() {
        PaimonColumnValue structValue = createStructValue();
        List<ColumnValue> values = new ArrayList<>();

        structValue.unpackStruct(Arrays.asList(0, 2), values);

        Assert.assertEquals(2, values.size());
        Assert.assertEquals(10, values.get(0).getInt());
        Assert.assertEquals(100L, values.get(1).getLong());
    }

    @Test
    public void testUnpackCompleteStruct() {
        PaimonColumnValue structValue = createStructValue();
        List<ColumnValue> values = new ArrayList<>();

        structValue.unpackStruct(Arrays.asList(0, 1, 2), values);

        Assert.assertEquals(3, values.size());
        Assert.assertEquals(10, values.get(0).getInt());
        Assert.assertEquals("x", values.get(1).getString());
        Assert.assertEquals(100L, values.get(2).getLong());
    }

    private PaimonColumnValue createStructValue() {
        GenericRow nestedRow = GenericRow.of(10, BinaryString.fromString("x"), 100L);
        RowType outerType = RowType.of(new DataType[] {paimonStructType}, new String[] {"s"});
        InternalRow outerRow = new InternalRowSerializer(outerType).toBinaryRow(GenericRow.of(nestedRow));
        return new PaimonColumnValue(outerRow, 0, dorisStructType, paimonStructType, "UTC");
    }
}
