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

package org.apache.doris.trinoconnector;

import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.ColumnValue;

import io.airlift.slice.Slices;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.JsonType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TrinoJsonColumnValueTest {
    private static final String OBJECT = "{\"text\":\"中文-é\\n\\u0000\",\"items\":[1,true,null]}";

    private static TrinoConnectorColumnValue value(Block block, Type type, String carrier, int position) {
        TrinoConnectorColumnValue value = new TrinoConnectorColumnValue();
        value.setBlock(block);
        value.setTrinoType(type);
        value.setColumnType(ColumnType.parseType("value", carrier));
        value.setPosition(position);
        return value;
    }

    @Test
    public void preservesJsonUtf8AndDistinguishesSqlNullAcrossEncodedBlocks() {
        BlockBuilder builder = JsonType.JSON.createBlockBuilder(null, 3);
        JsonType.JSON.writeSlice(builder, Slices.utf8Slice(OBJECT));
        builder.appendNull();
        JsonType.JSON.writeSlice(builder, Slices.utf8Slice("null"));
        Block block = builder.build();
        Assertions.assertEquals(OBJECT, value(block, JsonType.JSON, "string", 0).getString());
        Assertions.assertArrayEquals(OBJECT.getBytes(StandardCharsets.UTF_8),
                value(block, JsonType.JSON, "string", 0).getStringAsBytes());
        Assertions.assertTrue(value(block, JsonType.JSON, "string", 1).isNull());
        Assertions.assertFalse(value(block, JsonType.JSON, "string", 2).isNull());
        Assertions.assertEquals("null", value(block, JsonType.JSON, "string", 2).getString());
        Block dictionary = DictionaryBlock.create(4, block, new int[] {2, 1, 0, 2});
        Assertions.assertEquals("null", value(dictionary, JsonType.JSON, "string", 0).getString());
        Assertions.assertTrue(value(dictionary, JsonType.JSON, "string", 1).isNull());
        Assertions.assertEquals(OBJECT, value(dictionary, JsonType.JSON, "string", 2).getString());
        Block repeated = RunLengthEncodedBlock.create(block.getSingleValueBlock(0), 3);
        Assertions.assertEquals(OBJECT, value(repeated, JsonType.JSON, "string", 2).getString());
    }

    @Test
    public void preservesJsonInsideNullableArray() {
        ArrayType type = new ArrayType(JsonType.JSON);
        ArrayBlockBuilder builder = (ArrayBlockBuilder) type.createBlockBuilder(null, 3);
        builder.buildEntry(elements -> {
            JsonType.JSON.writeSlice(elements, Slices.utf8Slice(OBJECT));
            elements.appendNull();
            JsonType.JSON.writeSlice(elements, Slices.utf8Slice("null"));
        });
        builder.appendNull();
        builder.buildEntry(elements -> JsonType.JSON.writeSlice(elements, Slices.utf8Slice("[]")));
        Block block = builder.build();
        List<ColumnValue> elements = new ArrayList<>();
        value(block, type, "array<string>", 0).unpackArray(elements);
        Assertions.assertEquals(OBJECT, elements.get(0).getString());
        Assertions.assertTrue(elements.get(1).isNull());
        Assertions.assertFalse(elements.get(2).isNull());
        Assertions.assertEquals("null", elements.get(2).getString());
        elements.clear();
        value(block, type, "array<string>", 2).unpackArray(elements);
        Assertions.assertEquals("[]", elements.get(0).getString());
    }

    @Test
    public void preservesJsonInRowAfterSqlNullRow() {
        RowType type = RowType.rowType(RowType.field("payload", JsonType.JSON));
        RowBlockBuilder builder = (RowBlockBuilder) type.createBlockBuilder(null, 3);
        builder.buildEntry(fields -> JsonType.JSON.writeSlice(fields.get(0), Slices.utf8Slice(OBJECT)));
        builder.appendNull();
        builder.buildEntry(fields -> JsonType.JSON.writeSlice(fields.get(0), Slices.utf8Slice("null")));
        List<ColumnValue> fields = new ArrayList<>();
        value(builder.build(), type, "struct<payload:string>", 2).unpackStruct(Collections.singletonList(0), fields);
        Assertions.assertFalse(fields.get(0).isNull());
        Assertions.assertEquals("null", fields.get(0).getString());
    }
}
