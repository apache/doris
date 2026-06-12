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

package org.apache.doris.persist.gson;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/*
 * Verifies that the streaming dispatch mode of RuntimeTypeAdapterFactory produces
 * byte-identical json with the legacy tree mode, and that both modes can read each
 * other's output (new-write-old-read / old-write-new-read).
 */
public class RuntimeTypeAdapterFactoryStreamingTest {

    public abstract static class Shape {
        @SerializedName("n")
        public String name;
    }

    public static class Circle extends Shape {
        @SerializedName("r")
        public double radius;
        @SerializedName("tags")
        public List<String> tags = Lists.newArrayList();
        @SerializedName("attrs")
        public Map<String, Long> attrs = Maps.newHashMap();
        @SerializedName("inner")
        public Shape inner;
        @SerializedName("tbl")
        @JsonAdapter(GsonUtils.GuavaTableTypeAdapterFactory.class)
        public Table<Long, Long, String> tbl = HashBasedTable.create();
    }

    public static class Rectangle extends Shape {
        @SerializedName("w")
        public long width;
        @SerializedName("h")
        public long height;
    }

    // subtype which illegally defines a field with the same name as the type field
    public static class BadShape extends Shape {
        @SerializedName("clazz")
        public String clazz = "boom";
    }

    private static RuntimeTypeAdapterFactory<Shape> newFactory(boolean streaming) {
        RuntimeTypeAdapterFactory<Shape> factory = RuntimeTypeAdapterFactory.of(Shape.class, "clazz");
        if (streaming) {
            factory.withStreamingDispatch();
        }
        return factory.registerSubtype(Circle.class, Circle.class.getSimpleName())
                .registerSubtype(Rectangle.class, Rectangle.class.getSimpleName())
                .registerSubtype(BadShape.class, BadShape.class.getSimpleName());
    }

    private static Gson newGson(boolean streaming) {
        // mirror the relevant GSON_BUILDER settings
        return new GsonBuilder()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization()
                .registerTypeAdapterFactory(newFactory(streaming))
                .create();
    }

    private static final Gson TREE_GSON = newGson(false);
    private static final Gson STREAMING_GSON = newGson(true);

    private static Circle buildCircle() {
        Circle circle = new Circle();
        circle.name = "special <html> &chars\" 中文";
        circle.radius = 3.25;
        circle.tags.add("a");
        circle.tags.add(null);
        circle.tags.add("b");
        circle.attrs.put("x", 1L);
        Rectangle inner = new Rectangle();
        inner.name = null; // omitted by default serializeNulls=false
        inner.width = 10;
        inner.height = 20;
        circle.inner = inner;
        circle.tbl.put(1L, 2L, "v12");
        circle.tbl.put(1L, 3L, "v13");
        circle.tbl.put(4L, 2L, "v42");
        return circle;
    }

    @Test
    public void testInternalAccessHookInstalledEagerly() {
        // withStreamingDispatch() (already called by STREAMING_GSON setup above) must
        // have replaced the gson built-in hook with the unwrapping version, instead of
        // deferring the installation to the first streaming read
        Assert.assertTrue(com.google.gson.internal.JsonReaderInternalAccess.INSTANCE.getClass().getName()
                .startsWith("org.apache.doris"));
    }

    @Test
    public void testStreamingWriteByteIdenticalWithTree() {
        Circle circle = buildCircle();
        String treeJson = TREE_GSON.toJson(circle, Shape.class);
        String streamingJson = STREAMING_GSON.toJson(circle, Shape.class);
        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertTrue(streamingJson.startsWith("{\"clazz\":\"Circle\","));

        Rectangle rectangle = new Rectangle();
        rectangle.width = 7;
        Assert.assertEquals(TREE_GSON.toJson(rectangle, Shape.class),
                STREAMING_GSON.toJson(rectangle, Shape.class));
    }

    @Test
    public void testCrossModeRoundTrip() {
        Circle circle = buildCircle();
        String treeJson = TREE_GSON.toJson(circle, Shape.class);
        String streamingJson = STREAMING_GSON.toJson(circle, Shape.class);

        // new-write-old-read and old-write-new-read
        Circle fromTreeByStreaming = (Circle) STREAMING_GSON.fromJson(treeJson, Shape.class);
        Circle fromStreamingByTree = (Circle) TREE_GSON.fromJson(streamingJson, Shape.class);
        Circle fromStreamingByStreaming = (Circle) STREAMING_GSON.fromJson(streamingJson, Shape.class);

        for (Circle restored : Lists.newArrayList(fromTreeByStreaming, fromStreamingByTree,
                fromStreamingByStreaming)) {
            Assert.assertEquals(circle.name, restored.name);
            Assert.assertEquals(circle.radius, restored.radius, 0);
            Assert.assertEquals(circle.tags, restored.tags);
            Assert.assertEquals(circle.attrs, restored.attrs);
            Assert.assertEquals(((Rectangle) circle.inner).width, ((Rectangle) restored.inner).width);
            Assert.assertEquals(circle.tbl, restored.tbl);
        }
    }

    @Test
    public void testStreamingReadRequiresTypeFieldFirst() {
        // legacy tree read tolerates the type field at any position, streaming read does not;
        // all Doris-generated data always has it first, so fail fast on mismatch
        String typeFieldSecond = "{\"w\":7,\"clazz\":\"Rectangle\",\"h\":8}";
        Rectangle byTree = (Rectangle) TREE_GSON.fromJson(typeFieldSecond, Shape.class);
        Assert.assertEquals(7, byTree.width);
        try {
            STREAMING_GSON.fromJson(typeFieldSecond, Shape.class);
            Assert.fail("expect JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("the first field is 'w'"));
        }
    }

    @Test
    public void testStreamingWriteRejectsConflictTypeField() {
        try {
            STREAMING_GSON.toJson(new BadShape(), Shape.class);
            Assert.fail("expect JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("already defines a field named clazz"));
        }
        // same behavior as the tree mode
        try {
            TREE_GSON.toJson(new BadShape(), Shape.class);
            Assert.fail("expect JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("already defines a field named clazz"));
        }
    }

    @Test
    public void testStreamingReadUnknownSubtype() {
        try {
            STREAMING_GSON.fromJson("{\"clazz\":\"Triangle\",\"n\":\"x\"}", Shape.class);
            Assert.fail("expect JsonParseException");
        } catch (JsonParseException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("subtype named Triangle"));
        }
    }

    @Test
    public void testNullValue() {
        Assert.assertEquals("null", STREAMING_GSON.toJson(null, Shape.class));
        Assert.assertNull(STREAMING_GSON.fromJson("null", Shape.class));
    }
}
