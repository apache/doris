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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;

public class LegacyTimeTypeGsonCompatibilityTest {
    @Test
    public void testLegacyPrimitiveType() {
        PrimitiveType legacy = GsonUtils.GSON.fromJson("\"TIME\"", PrimitiveType.class);
        Assert.assertEquals(PrimitiveType.TIMEV2, legacy);
        Assert.assertEquals(PrimitiveType.TIMEV2,
                GsonUtils.GSON.fromJson("\"TIMEV2\"", PrimitiveType.class));
        Assert.assertEquals("\"TIMEV2\"", GsonUtils.GSON.toJson(legacy));
    }

    @Test
    public void testLegacyScalarType() {
        String json = "{\"clazz\":\"ScalarType\",\"type\":\"TIME\",\"len\":-1,\"precision\":0,\"scale\":0}";
        ScalarType restored = (ScalarType) GsonUtils.GSON.fromJson(json, Type.class);
        Assert.assertEquals(PrimitiveType.TIMEV2, restored.getPrimitiveType());
        Assert.assertEquals(0, restored.getScalarScale());
        Assert.assertEquals(TPrimitiveType.TIMEV2, restored.getPrimitiveType().toThrift());
        Assert.assertEquals("TIMEV2", JsonParser.parseString(
                GsonUtils.GSON.toJson(restored, Type.class)).getAsJsonObject().get("type").getAsString());
    }

    @Test
    public void testLegacyColumnAndCurrentPrecision() {
        Column column = new Column("legacy_time", ScalarType.createTimeV2Type(0));
        Column restored = GsonUtils.GSON.fromJson(legacyTimeJson(column, Column.class), Column.class);
        Assert.assertEquals(PrimitiveType.TIMEV2, restored.getDataType());
        Assert.assertEquals(TPrimitiveType.TIMEV2, restored.getDataType().toThrift());
        Assert.assertEquals(column, restored);

        Column preciseColumn = new Column("precise_time", ScalarType.createTimeV2Type(6));
        Column preciseRestored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(preciseColumn), Column.class);
        Assert.assertEquals(preciseColumn, preciseRestored);
        Assert.assertEquals(6, preciseRestored.getScale());
    }

    @Test
    public void testLegacyNestedColumn() {
        Column column = new Column("nested_time", new StructType(
                new StructField("times", new ArrayType(ScalarType.createTimeV2Type(0), true)),
                new StructField("lookup", new MapType(Type.STRING, ScalarType.createTimeV2Type(0)))));
        Column restored = GsonUtils.GSON.fromJson(legacyTimeJson(column, Column.class), Column.class);
        Assert.assertEquals(column, restored);
        Assert.assertEquals(PrimitiveType.TIMEV2,
                restored.getChildren().get(0).getChildren().get(0).getDataType());
        Assert.assertEquals(PrimitiveType.TIMEV2,
                restored.getChildren().get(1).getChildren().get(1).getDataType());
    }

    @Test
    public void testLegacyViewImage() throws Exception {
        View view = legacyTimeView();
        Table restored = Table.read(metadataInput(legacyTimeJson(view, Table.class)));
        assertRestoredView(restored);
    }

    @Test
    public void testLegacyViewJournal() throws Exception {
        CreateTableInfo info = new CreateTableInfo("test", 2L, legacyTimeView());
        Table restored = CreateTableInfo.read(metadataInput(legacyTimeJson(info, CreateTableInfo.class))).getTable();
        assertRestoredView(restored);
    }

    private static View legacyTimeView() {
        return new View(1L, "legacy_time_view", Collections.singletonList(
                new Column("legacy_time", ScalarType.createTimeV2Type(0))));
    }

    private static String legacyTimeJson(Object value, Class<?> type) {
        // Old FE metadata persisted the enum name TIME, which has since been removed.
        return GsonUtils.GSON.toJson(value, type).replace("\"TIMEV2\"", "\"TIME\"");
    }

    private static DataInputStream metadataInput(String json) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(bytes), json);
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private static void assertRestoredView(Table restored) {
        Assert.assertTrue(restored instanceof View);
        Assert.assertEquals(1, restored.getFullSchema().size());
        Column column = restored.getFullSchema().get(0);
        Assert.assertEquals("legacy_time", column.getName());
        Assert.assertEquals(TPrimitiveType.TIMEV2, column.getDataType().toThrift());
    }
}
