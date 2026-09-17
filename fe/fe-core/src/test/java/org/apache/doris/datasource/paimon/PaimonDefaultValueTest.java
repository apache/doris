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

import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Test;

public class PaimonDefaultValueTest {

    @Test
    public void testStringDefaultUsesPaimonParser() {
        DataField field = new DataField(
                0, "name", DataTypes.STRING(), null, "'unknown'");

        StringLiteral literal = (StringLiteral) PaimonDefaultValue.toDorisExpression(
                field, StringType.INSTANCE);

        Assert.assertEquals("unknown", literal.getValue());
    }

    @Test
    public void testComplexDefaultsUsePaimonInternalValues() {
        DataField arrayField = new DataField(
                0, "numbers", DataTypes.ARRAY(DataTypes.INT()), null, "[1, 2, 3]");
        ArrayLiteral array = (ArrayLiteral) PaimonDefaultValue.toDorisExpression(
                arrayField, ArrayType.of(IntegerType.INSTANCE));
        Assert.assertEquals(ImmutableList.of(
                new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)), array.getValue());

        DataField mapField = new DataField(1, "properties",
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), null,
                "{one -> 1, two -> 2}");
        MapLiteral map = (MapLiteral) PaimonDefaultValue.toDorisExpression(
                mapField, MapType.of(StringType.INSTANCE, IntegerType.INSTANCE));
        Assert.assertEquals(new IntegerLiteral(1), map.getValue().get(new StringLiteral("one")));
        Assert.assertEquals(new IntegerLiteral(2), map.getValue().get(new StringLiteral("two")));

        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        StructType structType = new StructType(ImmutableList.of(
                new StructField("id", IntegerType.INSTANCE, true, ""),
                new StructField("value", StringType.INSTANCE, true, "")));
        DataField rowField = new DataField(
                2, "nested", rowType, null, "{42, default-value}");
        StructLiteral struct = (StructLiteral) PaimonDefaultValue.toDorisExpression(
                rowField, structType);
        Assert.assertEquals(new IntegerLiteral(42), struct.getValue().get(0));
        Assert.assertEquals(new StringLiteral("default-value"), struct.getValue().get(1));
    }
}
