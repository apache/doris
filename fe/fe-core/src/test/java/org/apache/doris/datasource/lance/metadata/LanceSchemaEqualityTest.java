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

package org.apache.doris.datasource.lance.metadata;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class LanceSchemaEqualityTest {

    private static final String EXTENSION_NAME = "ARROW:extension:name";

    @Test
    public void testIdenticalSchemasHaveNoDifference() {
        Schema schema = schema(utf8("row_id"));
        Assert.assertEquals(Optional.empty(), LanceSchemaEquality.firstDifference(schema, schema));
    }

    @Test
    public void testFieldCountIsReported() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(utf8("a")), schema(utf8("a"), utf8("b")));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get().contains("field count"));
    }

    @Test
    public void testNamesCompareExactly() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(utf8("age")), schema(utf8("AGE")));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get().contains("name"));
    }

    @Test
    public void testNullabilityIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(utf8("a", true)), schema(utf8("a", false)));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get().contains("nullable"));
    }

    @Test
    public void testTypeIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(utf8("a")), schema(int64("a")));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get().contains("type"));
    }

    @Test
    public void testFixedSizeListDimensionIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(fixedSizeList(3)), schema(fixedSizeList(5)));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get(), difference.get().contains("3"));
        Assert.assertTrue(difference.get(), difference.get().contains("5"));
    }

    @Test
    public void testDictionaryEncodingIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(dictionaryEncodedUtf8("a")), schema(utf8("a")));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get(), difference.get().contains("dictionary encoding"));
    }

    @Test
    public void testDecimalPrecisionAndScaleIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(decimal("a", 10, 2)), schema(decimal("a", 12, 4)));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get(), difference.get().contains("type"));
    }

    @Test
    public void testTimestampUnitIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(timestamp("a", TimeUnit.SECOND, "UTC")),
                schema(timestamp("a", TimeUnit.MILLISECOND, "UTC")));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get(), difference.get().contains("type"));
    }

    @Test
    public void testTimestampTimezoneIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(timestamp("a", TimeUnit.SECOND, "UTC")),
                schema(timestamp("a", TimeUnit.SECOND, "Asia/Shanghai")));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get(), difference.get().contains("type"));
    }

    @Test
    public void testExtensionNameIsCompared() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(utf8("a", true, "arrow.json")), schema(utf8("a", true, null)));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get().contains("extension name"));
    }

    @Test
    public void testExtensionNameIgnoresCase() {
        Assert.assertEquals(Optional.empty(), LanceSchemaEquality.firstDifference(
                schema(utf8("a", true, "ARROW.JSON")), schema(utf8("a", true, "arrow.json"))));
    }

    @Test
    public void testNestedDifferenceNamesThePath() {
        Optional<String> difference = LanceSchemaEquality.firstDifference(
                schema(struct("s", utf8("c"))), schema(struct("s", int64("c"))));
        Assert.assertTrue(difference.isPresent());
        Assert.assertTrue(difference.get(), difference.get().contains("'s'.0"));
    }

    @Test
    public void testMissingSchemaIsReported() {
        Assert.assertTrue(LanceSchemaEquality.firstDifference(null, schema(utf8("a"))).isPresent());
        Assert.assertTrue(LanceSchemaEquality.firstDifference(schema(utf8("a")), null).isPresent());
    }

    private static Schema schema(Field... fields) {
        return new Schema(Arrays.asList(fields));
    }

    private static Field utf8(String name) {
        return utf8(name, true, null);
    }

    private static Field utf8(String name, boolean nullable) {
        return utf8(name, nullable, null);
    }

    private static Field utf8(String name, boolean nullable, String extensionName) {
        return new Field(name,
                new FieldType(nullable, ArrowType.Utf8.INSTANCE, null,
                        extensionName == null
                                ? null : Collections.singletonMap(EXTENSION_NAME, extensionName)),
                Collections.emptyList());
    }

    private static Field int64(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
    }

    private static Field fixedSizeList(int size) {
        return new Field("vec", FieldType.nullable(new ArrowType.FixedSizeList(size)),
                Collections.singletonList(int64("item")));
    }

    private static Field dictionaryEncodedUtf8(String name) {
        return new Field(name,
                new FieldType(true, ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(1L, false, new ArrowType.Int(32, true))),
                Collections.emptyList());
    }

    private static Field decimal(String name, int precision, int scale) {
        return new Field(name, FieldType.nullable(new ArrowType.Decimal(precision, scale, 128)),
                Collections.emptyList());
    }

    private static Field timestamp(String name, TimeUnit unit, String timezone) {
        return new Field(name, FieldType.nullable(new ArrowType.Timestamp(unit, timezone)),
                Collections.emptyList());
    }

    private static Field struct(String name, Field... children) {
        return new Field(name, FieldType.nullable(ArrowType.Struct.INSTANCE), Arrays.asList(children));
    }
}
