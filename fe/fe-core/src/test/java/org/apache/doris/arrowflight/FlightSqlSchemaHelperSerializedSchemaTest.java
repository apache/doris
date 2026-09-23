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

package org.apache.doris.arrowflight;

import org.apache.doris.arrow.DorisArrowTypeMapping;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TPrimitiveType;

import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The client does not see the {@link Field} objects {@link DorisArrowTypeMapping} builds, it sees the
 * serialized schema in the {@code table_schema} column of {@code GetTables}. Asserting after a round
 * trip through that encoding is what proves the types, nested ones included, actually reach it.
 */
public class FlightSqlSchemaHelperSerializedSchemaTest {

    private static final String DB = "test_db";
    private static final String TABLE = "test_tbl";

    private static TColumnDesc desc(String name, TPrimitiveType type) {
        TColumnDesc columnDesc = new TColumnDesc(name, type);
        columnDesc.setIsAllowNull(true);
        return columnDesc;
    }

    private static TColumnDesc desc(String name, TPrimitiveType type, TColumnDesc... children) {
        TColumnDesc columnDesc = desc(name, type);
        columnDesc.setChildren(Arrays.asList(children));
        return columnDesc;
    }

    private static Field buildField(TColumnDesc columnDesc) {
        return DorisArrowTypeMapping.toField(DB, TABLE, columnDesc);
    }

    private static Schema deserialize(byte[] serialized) throws IOException {
        return MessageSerializer.deserializeSchema(
                new ReadChannel(Channels.newChannel(new ByteArrayInputStream(serialized))));
    }

    @Test
    public void theSerializedSchemaCarriesTheChildren() throws IOException {
        byte[] serialized = FlightSqlSchemaHelper.getSerializedSchema(Collections.singletonList(
                buildField(desc("a", TPrimitiveType.ARRAY, desc("item", TPrimitiveType.INT)))));

        Field array = deserialize(serialized).getFields().get(0);
        Assertions.assertEquals(ArrowType.ArrowTypeID.List, array.getType().getTypeID());
        Assertions.assertEquals(new ArrowType.Int(32, true), array.getChildren().get(0).getType());
    }

    @Test
    public void serializedSchemaDescribesScalarAndNestedTimestampNs() throws IOException {
        byte[] serialized = FlightSqlSchemaHelper.getSerializedSchema(Arrays.asList(
                buildField(desc("ts", TPrimitiveType.TIMESTAMP_NS)),
                buildField(desc("items", TPrimitiveType.ARRAY,
                        desc("item", TPrimitiveType.TIMESTAMP_NS))),
                buildField(desc("by_name", TPrimitiveType.MAP,
                        desc("key", TPrimitiveType.VARCHAR),
                        desc("value", TPrimitiveType.TIMESTAMP_NS))),
                buildField(desc("record", TPrimitiveType.STRUCT,
                        desc("ts", TPrimitiveType.TIMESTAMP_NS)))));

        Schema schema = deserialize(serialized);
        ArrowType.Timestamp timestampNs = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
        Assertions.assertEquals(timestampNs, schema.getFields().get(0).getType());
        Assertions.assertEquals(timestampNs,
                schema.getFields().get(1).getChildren().get(0).getType());
        Assertions.assertEquals(timestampNs,
                schema.getFields().get(2).getChildren().get(0).getChildren().get(1).getType());
        Assertions.assertEquals(timestampNs,
                schema.getFields().get(3).getChildren().get(0).getType());
    }

    @Test
    public void serializedSchemaPreservesUuidAndStringTypes() throws IOException {
        byte[] serialized = FlightSqlSchemaHelper.getSerializedSchema(Arrays.asList(
                buildField(desc("u", TPrimitiveType.UUID)),
                buildField(desc("items", TPrimitiveType.ARRAY, desc("item", TPrimitiveType.UUID))),
                buildField(desc("mapping", TPrimitiveType.MAP,
                        desc("key", TPrimitiveType.UUID), desc("value", TPrimitiveType.UUID))),
                buildField(desc("record", TPrimitiveType.STRUCT, desc("u", TPrimitiveType.UUID))),
                buildField(desc("text", TPrimitiveType.STRING))));
        Schema schema = MessageSerializer.deserializeSchema(
                new ReadChannel(Channels.newChannel(new ByteArrayInputStream(serialized))));
        Assertions.assertEquals(UuidType.INSTANCE, schema.getFields().get(0).getType());
        Assertions.assertEquals(new ArrowType.FixedSizeBinary(16), UuidType.INSTANCE.storageType());
        Assertions.assertEquals(UuidType.INSTANCE,
                schema.getFields().get(1).getChildren().get(0).getType());
        List<Field> pair = schema.getFields().get(2).getChildren().get(0).getChildren();
        Assertions.assertEquals(UuidType.INSTANCE, pair.get(0).getType());
        Assertions.assertFalse(pair.get(0).isNullable());
        Assertions.assertEquals(UuidType.INSTANCE, pair.get(1).getType());
        Assertions.assertEquals(UuidType.INSTANCE,
                schema.getFields().get(3).getChildren().get(0).getType());
        Assertions.assertEquals(new ArrowType.Utf8(), schema.getFields().get(4).getType());
    }
}
