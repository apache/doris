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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TPrimitiveType;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;

public class FlightSqlNativeVariantTest {
    @Test
    public void executionCannotPublishUtf8ForVariant() {
        Assertions.assertThrows(org.apache.arrow.flight.FlightRuntimeException.class,
                () -> FlightSqlSchemaHelper.withDorisTypeMetadata(Field.nullable("v", new ArrowType.Utf8()),
                        Type.VARIANT));
    }

    @Test
    public void schemaKeepsExtensionAcrossIpc() throws Exception {
        TColumnDesc variant = new TColumnDesc("item", TPrimitiveType.VARIANT);
        variant.setIsAllowNull(true);
        TColumnDesc array = new TColumnDesc("a", TPrimitiveType.ARRAY);
        array.setChildren(Collections.singletonList(variant));
        Field field = Deencapsulation.invoke(FlightSqlSchemaHelper.class, "buildField",
                "test_db", "test_table", array);
        Field child = field.getChildren().get(0);
        Assertions.assertEquals(new ArrowType.Struct(), child.getType());
        Assertions.assertEquals("arrow.parquet.variant", child.getMetadata().get("ARROW:extension:name"));
        Assertions.assertEquals("metadata", child.getChildren().get(0).getName());
        Assertions.assertEquals("value", child.getChildren().get(1).getName());
        for (Field storage : child.getChildren()) {
            Assertions.assertFalse(storage.isNullable());
            Assertions.assertEquals(new ArrowType.Binary(), storage.getType());
        }
        Schema schema = new Schema(Collections.singletonList(field));
        try (ReadChannel channel = new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(schema.serializeAsMessage())))) {
            Assertions.assertEquals(schema, MessageSerializer.deserializeSchema(channel));
        }
    }

    @Test
    public void nestedDiscoveryPreservesNativeVariantAndUuid() {
        TColumnDesc variant = new TColumnDesc("item", TPrimitiveType.VARIANT);
        variant.setIsAllowNull(true);
        TColumnDesc array = new TColumnDesc("a", TPrimitiveType.ARRAY);
        array.setChildren(Collections.singletonList(variant));
        TColumnDesc map = new TColumnDesc("m", TPrimitiveType.MAP);
        map.setChildren(Arrays.asList(new TColumnDesc("key", TPrimitiveType.STRING), variant));
        TColumnDesc uuid = new TColumnDesc("id", TPrimitiveType.UUID);
        TColumnDesc struct = new TColumnDesc("s", TPrimitiveType.STRUCT);
        struct.setChildren(Arrays.asList(array, map, uuid));
        Field result = Deencapsulation.invoke(FlightSqlSchemaHelper.class, "buildField",
                "test_db", "test_table", struct);
        for (Field leaf : Arrays.asList(result.getChildren().get(0).getChildren().get(0),
                result.getChildren().get(1).getChildren().get(0).getChildren().get(1))) {
            Assertions.assertEquals("arrow.parquet.variant", leaf.getMetadata().get("ARROW:extension:name"));
            Assertions.assertEquals(new ArrowType.Struct(), leaf.getType());
        }
        // Flight's Variant specialization must retain master's UUID extension mapping.
        Assertions.assertEquals(org.apache.arrow.vector.extension.UuidType.INSTANCE,
                result.getChildren().get(2).getType());
        Assertions.assertFalse(result.getChildren().get(1).getChildren().get(0)
                .getChildren().get(0).isNullable());
    }

}
