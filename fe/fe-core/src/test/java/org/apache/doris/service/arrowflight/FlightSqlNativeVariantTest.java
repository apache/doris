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

package org.apache.doris.service.arrowflight;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TPrimitiveType;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class FlightSqlNativeVariantTest {
    private boolean originalVariantV2;

    @Before
    public void enableVariantV2() {
        originalVariantV2 = Config.enable_variant_v2;
        Config.enable_variant_v2 = true;
    }

    @After
    public void restoreVariantV2() {
        Config.enable_variant_v2 = originalVariantV2;
    }

    @Test
    public void legacyVariantIsRejectedWithoutAFormatSwitch() {
        Config.enable_variant_v2 = false;
        Assert.assertThrows(org.apache.arrow.flight.FlightRuntimeException.class,
                () -> FlightSqlSchemaHelper.nativeVariantField("v", true, Collections.emptyMap()));
    }

    @Test
    public void executionCannotPublishUtf8ForVariant() {
        Assert.assertThrows(org.apache.arrow.flight.FlightRuntimeException.class,
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
        Assert.assertEquals(new ArrowType.Struct(), child.getType());
        Assert.assertEquals("arrow.parquet.variant", child.getMetadata().get("ARROW:extension:name"));
        Assert.assertEquals("metadata", child.getChildren().get(0).getName());
        Assert.assertEquals("value", child.getChildren().get(1).getName());
        for (Field storage : child.getChildren()) {
            Assert.assertFalse(storage.isNullable());
            Assert.assertEquals(new ArrowType.Binary(), storage.getType());
        }
        Schema schema = new Schema(Collections.singletonList(field));
        try (ReadChannel channel = new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(schema.serializeAsMessage())))) {
            Assert.assertEquals(schema, MessageSerializer.deserializeSchema(channel));
        }
    }

    @Test
    public void querySchemaPreservesNativeVariantInNestedFields() {
        Type nested = new StructType(new ArrayList<>(Arrays.asList(
                new StructField("scalar", Type.VARIANT),
                new StructField("array", new ArrayType(Type.VARIANT, true)),
                new StructField("map", new MapType(Type.STRING, Type.VARIANT)))));
        Field result = Deencapsulation.invoke(FlightSqlQuerySchema.class, "field",
                "s", nested, true, true, "UTC");
        Field scalar = result.getChildren().get(0);
        Field item = result.getChildren().get(1).getChildren().get(0);
        Field value = result.getChildren().get(2).getChildren().get(0).getChildren().get(1);
        for (Field leaf : Arrays.asList(scalar, item, value)) {
            Assert.assertEquals(new ArrowType.Struct(), leaf.getType());
            Assert.assertEquals("arrow.parquet.variant", leaf.getMetadata().get("ARROW:extension:name"));
            Assert.assertEquals("", leaf.getMetadata().get("ARROW:extension:metadata"));
            Assert.assertEquals(Arrays.asList(Field.notNullable("metadata", new ArrowType.Binary()),
                    Field.notNullable("value", new ArrowType.Binary())), leaf.getChildren());
            Assert.assertEquals("VARIANT", leaf.getMetadata().get("doris_type"));
        }
        // Model execution's metadata enrichment to catch Prepare/DoGet schema mismatches.
        Field execution = FlightSqlSchemaHelper.withDorisTypeMetadata(result, nested);
        Assert.assertTrue(FlightSqlQuerySchema.matchesExecutionSchema(
                new Schema(Collections.singletonList(result)),
                new Schema(Collections.singletonList(execution)), Collections.singletonList("s")));
    }

}
