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

package org.apache.doris.persist;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FileType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class FileTypePersistenceTest {
    @Test
    public void testTypeJsonRoundTripPreservesFileIdentity() {
        for (Gson gson : Arrays.asList(GsonUtils.GSON)) {
            for (Type type : types()) {
                Type restored = gson.fromJson(gson.toJson(type, Type.class), Type.class);
                Assertions.assertEquals(type.getClass(), restored.getClass());
                Assertions.assertEquals(type, restored);
                Assertions.assertTrue(restored.typeContainsFile());
            }
        }
    }

    @Test
    public void testColumnJsonRoundTripPreservesCanonicalSchema() {
        for (Gson gson : Arrays.asList(GsonUtils.GSON)) {
            for (Type type : types()) {
                Column column = new Column("payload", type, true);
                Column restored = gson.fromJson(gson.toJson(column), Column.class);
                assertColumn(column, restored);
            }
        }
    }

    @Test
    public void testMalformedColumnSchemaRejectedOnWriteAndRead() {
        for (Gson gson : Arrays.asList(GsonUtils.GSON)) {
            Column column = new Column("files", new ArrayType(FileType.create()), true);
            JsonObject json = gson.toJsonTree(column).getAsJsonObject();
            json.getAsJsonArray("children").get(0).getAsJsonObject()
                    .getAsJsonArray("children").remove(5);
            Assertions.assertThrows(Exception.class,
                    () -> gson.fromJson(json, Column.class));
            column.getChildren().get(0).getChildren().remove(5);
            Assertions.assertThrows(Exception.class, () -> gson.toJson(column));
        }
    }

    private static List<Type> types() {
        return Arrays.asList(FileType.create(), new ArrayType(FileType.create(), true),
                new StructType(new StructField("payload", FileType.create()),
                        new StructField("grouped", new ArrayType(new MapType(Type.STRING, FileType.create())))),
                new MapType(Type.STRING, FileType.create()));
    }

    private static void assertColumn(Column expected, Column actual) {
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(expected.isAllowNull(), actual.isAllowNull());
        Assertions.assertEquals(expected.getType().getClass(), actual.getType().getClass());
        Assertions.assertEquals(expected.getType(), actual.getType());
        if (expected.getChildren() == null) {
            Assertions.assertNull(actual.getChildren());
            return;
        }
        Assertions.assertNotNull(actual.getChildren());
        Assertions.assertEquals(expected.getChildren().size(), actual.getChildren().size());
        if (expected.getType().isFileType()) {
            Assertions.assertEquals(6, actual.getChildren().size());
            Assertions.assertFalse(actual.getType().isStructType());
            List<StructField> fields = ((FileType) actual.getType()).getFields();
            for (int i = 0; i < fields.size(); i++) {
                Column child = actual.getChildren().get(i);
                Assertions.assertEquals(fields.get(i).getName(), child.getName());
                Assertions.assertEquals(fields.get(i).getType(), child.getType());
                Assertions.assertTrue(child.isAllowNull());
            }
        }
        for (int i = 0; i < expected.getChildren().size(); i++) {
            assertColumn(expected.getChildren().get(i), actual.getChildren().get(i));
        }
    }
}
