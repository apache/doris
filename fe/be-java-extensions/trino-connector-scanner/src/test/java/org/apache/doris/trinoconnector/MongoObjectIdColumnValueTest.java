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
import io.trino.plugin.mongodb.ObjectIdType;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MongoObjectIdColumnValueTest {
    private static final byte[] FIRST = {0, -1, -128, 127, 1, 0, 2, 3, 4, 5, 6, 7};
    private static final byte[] SECOND = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};

    private static TrinoConnectorColumnValue value(Block block, Type type, String dorisType, int position) {
        TrinoConnectorColumnValue value = new TrinoConnectorColumnValue();
        value.setBlock(block);
        value.setTrinoType(type);
        value.setColumnType(ColumnType.parseType("value", dorisType));
        value.setPosition(position);
        return value;
    }

    @Test
    public void preservesEveryByteAndNullAcrossEncodedBlocks() {
        BlockBuilder builder = ObjectIdType.OBJECT_ID.createBlockBuilder(null, 3);
        ObjectIdType.OBJECT_ID.writeSlice(builder, Slices.wrappedBuffer(FIRST));
        builder.appendNull();
        ObjectIdType.OBJECT_ID.writeSlice(builder, Slices.wrappedBuffer(SECOND));
        Block block = builder.build();
        Assertions.assertArrayEquals(FIRST, value(block, ObjectIdType.OBJECT_ID, "varbinary(12)", 0).getBytes());
        Assertions.assertTrue(value(block, ObjectIdType.OBJECT_ID, "varbinary(12)", 1).isNull());
        Assertions.assertArrayEquals(SECOND, value(block, ObjectIdType.OBJECT_ID, "varbinary(12)", 2).getBytes());
        Block dictionary = DictionaryBlock.create(4, block, new int[] {2, 1, 0, 2});
        Assertions.assertArrayEquals(SECOND,
                value(dictionary, ObjectIdType.OBJECT_ID, "varbinary(12)", 0).getBytes());
        Assertions.assertTrue(value(dictionary, ObjectIdType.OBJECT_ID, "varbinary(12)", 1).isNull());
        Assertions.assertArrayEquals(FIRST,
                value(dictionary, ObjectIdType.OBJECT_ID, "varbinary(12)", 2).getBytes());
        Block repeated = RunLengthEncodedBlock.create(block.getSingleValueBlock(0), 3);
        Assertions.assertArrayEquals(FIRST,
                value(repeated, ObjectIdType.OBJECT_ID, "varbinary(12)", 2).getBytes());
    }

    @Test
    public void preservesObjectIdsInsideNullableArrays() {
        ArrayType type = new ArrayType(ObjectIdType.OBJECT_ID);
        ArrayBlockBuilder builder = (ArrayBlockBuilder) type.createBlockBuilder(null, 3);
        builder.buildEntry(elements -> {
            ObjectIdType.OBJECT_ID.writeSlice(elements, Slices.wrappedBuffer(FIRST));
            elements.appendNull();
            ObjectIdType.OBJECT_ID.writeSlice(elements, Slices.wrappedBuffer(SECOND));
        });
        builder.appendNull();
        builder.buildEntry(elements -> ObjectIdType.OBJECT_ID.writeSlice(elements, Slices.wrappedBuffer(SECOND)));
        Block block = builder.build();
        List<ColumnValue> elements = new ArrayList<>();
        value(block, type, "array<varbinary(12)>", 0).unpackArray(elements);
        Assertions.assertArrayEquals(FIRST, elements.get(0).getBytes());
        Assertions.assertTrue(elements.get(1).isNull());
        Assertions.assertArrayEquals(SECOND, elements.get(2).getBytes());
        elements.clear();
        value(block, type, "array<varbinary(12)>", 2).unpackArray(elements);
        Assertions.assertArrayEquals(SECOND, elements.get(0).getBytes());
    }

    @Test
    public void preservesObjectIdInRowAfterNullRow() {
        RowType type = RowType.rowType(RowType.field("owner", ObjectIdType.OBJECT_ID));
        RowBlockBuilder builder = (RowBlockBuilder) type.createBlockBuilder(null, 3);
        builder.buildEntry(fields -> ObjectIdType.OBJECT_ID.writeSlice(fields.get(0), Slices.wrappedBuffer(FIRST)));
        builder.appendNull();
        builder.buildEntry(fields -> ObjectIdType.OBJECT_ID.writeSlice(fields.get(0), Slices.wrappedBuffer(SECOND)));
        Block block = builder.build();
        List<ColumnValue> fields = new ArrayList<>();
        value(block, type, "struct<owner:varbinary(12)>", 2).unpackStruct(Collections.singletonList(0), fields);
        Assertions.assertArrayEquals(SECOND, fields.get(0).getBytes());
    }
}
