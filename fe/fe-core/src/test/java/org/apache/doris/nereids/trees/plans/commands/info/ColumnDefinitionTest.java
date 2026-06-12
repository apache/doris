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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class ColumnDefinitionTest {

    @Test
    public void testNameEquals() {
        ColumnDefinition columnDefinition = new ColumnDefinition("col1", null, false, null, false, null, null);
        String otherColName = "col1";
        boolean expected = true;
        Assertions.assertEquals(expected, columnDefinition.nameEquals(otherColName, false));

        String otherColName2 = "col2";
        boolean expected2 = false;
        Assertions.assertEquals(expected2, columnDefinition.nameEquals(otherColName2, false));
    }

    @Test
    public void testComplexTypeLiteralDefaultValue() {
        assertAllowsDefaultValue(ArrayType.of(IntegerType.INSTANCE), "[]");
        assertAllowsDefaultValue(ArrayType.of(IntegerType.INSTANCE), "[1, 2]");
        assertRejectsDefaultValue(ArrayType.of(IntegerType.INSTANCE), "{}",
                "only supports array literals or DEFAULT NULL");

        assertAllowsDefaultValue(MapType.of(StringType.INSTANCE, IntegerType.INSTANCE), "{}");
        assertAllowsDefaultValue(MapType.of(StringType.INSTANCE, IntegerType.INSTANCE), "{\"a\": 1}");
        assertRejectsDefaultValue(MapType.of(StringType.INSTANCE, IntegerType.INSTANCE), "[]",
                "only supports map literals or DEFAULT NULL");

        StructType structType = new StructType(Arrays.asList(
                new StructField("f1", IntegerType.INSTANCE, true, ""),
                new StructField("f2", StringType.INSTANCE, true, "")));
        assertAllowsDefaultValue(structType, "{}");
        assertAllowsDefaultValue(structType, "{1, \"a\"}");
        assertRejectsDefaultValue(structType, "{\"f1\": 1, \"f2\": \"a\"}",
                "only supports struct literals or DEFAULT NULL");

        assertRejectsDefaultValue(JsonType.INSTANCE, "{}", "only supports DEFAULT NULL");
        assertRejectsDefaultValue(VariantType.INSTANCE, "{}", "only supports DEFAULT NULL");

        Assertions.assertDoesNotThrow(() -> newColumnDefinition(
                ArrayType.of(IntegerType.INSTANCE), Optional.empty()).validate(
                        true, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS));
        Assertions.assertDoesNotThrow(() -> newColumnDefinition(
                MapType.of(StringType.INSTANCE, IntegerType.INSTANCE),
                Optional.of(DefaultValue.NULL_DEFAULT_VALUE)).validate(
                        true, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS));
    }

    private void assertAllowsDefaultValue(DataType type, String defaultValue) {
        Assertions.assertDoesNotThrow(() -> newColumnDefinition(
                type, Optional.of(new DefaultValue(defaultValue))).validate(
                        true, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS));
    }

    private void assertRejectsDefaultValue(DataType type, String defaultValue, String message) {
        ColumnDefinition columnDefinition = newColumnDefinition(type, Optional.of(new DefaultValue(defaultValue)));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> columnDefinition.validate(
                true, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS));
        Assertions.assertTrue(exception.getMessage().contains(message));
    }

    private ColumnDefinition newColumnDefinition(DataType type, Optional<DefaultValue> defaultValue) {
        return new ColumnDefinition("col1", type, false, null, true, defaultValue, "");
    }
}
