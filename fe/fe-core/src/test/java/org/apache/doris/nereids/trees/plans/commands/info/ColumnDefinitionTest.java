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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class ColumnDefinitionTest {

    @BeforeEach
    public void setUp() {
        Config.enable_non_aggregate_table_state_types = false;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_non_aggregate_table_state_types = false;
    }

    @Test
    public void testTimestampNsDefaultsAreNormalizedForExternalDdl() {
        String[][] values = {
                {"20240102", "2024-01-02 00:00:00.000000000"},
                {"20240102030405", "2024-01-02 03:04:05.000000000"},
                {"2024-01-02 03:04:05.123456789", "2024-01-02 03:04:05.123456789"}
        };
        for (String[] value : values) {
            ColumnDefinition column = new ColumnDefinition("ts", TimeStampNsType.INSTANCE,
                    false, null, true, Optional.of(new DefaultValue(value[0])), "");
            column.validate(false, ImmutableSet.of(), ImmutableSet.of(), false, KeysType.DUP_KEYS);
            Assertions.assertEquals(value[1], column.translateToCatalogStyle().getDefaultValue());
            Assertions.assertEquals(value[1], column.translateToCatalogStyleForSchemaChange().getDefaultValue());
        }
    }

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
    public void testToSqlHandlesNullComment() {
        ColumnDefinition columnDefinition = new ColumnDefinition("col1", StringType.INSTANCE, true, null);

        String sql = columnDefinition.toSql();
        Assertions.assertTrue(sql.endsWith("COMMENT \"\""));
    }

    @Test
    public void testStateTypesRequireAggregateKeyTableByDefault() {
        for (KeysType keysType : ImmutableList.of(KeysType.DUP_KEYS, KeysType.UNIQUE_KEYS)) {
            for (DataType type : aggregateTableOnlyTypes()) {
                ColumnDefinition column = new ColumnDefinition(
                        "v", type, false, null, false, Optional.empty(), "");

                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> validateColumn(column, keysType));
                Assertions.assertTrue(exception.getMessage().contains(
                        type.toSql() + " type is only supported in aggregate key tables"));
            }
        }
    }

    @Test
    public void testTemporaryConfigAllowsStateTypesInNonAggregateTable() {
        Config.enable_non_aggregate_table_state_types = true;

        for (KeysType keysType : ImmutableList.of(KeysType.DUP_KEYS, KeysType.UNIQUE_KEYS)) {
            for (DataType type : aggregateTableOnlyTypes()) {
                ColumnDefinition column = new ColumnDefinition(
                        "v", type, false, null, false, Optional.empty(), "");
                Assertions.assertDoesNotThrow(() -> validateColumn(column, keysType));
            }
        }
    }

    @Test
    public void testStateTypesRemainSupportedInAggregateKeyTable() {
        Assertions.assertDoesNotThrow(() -> validateColumn(new ColumnDefinition(
                "v", HllType.INSTANCE, false, AggregateType.HLL_UNION, false, Optional.empty(), ""),
                KeysType.AGG_KEYS));
        Assertions.assertDoesNotThrow(() -> validateColumn(new ColumnDefinition(
                "v", QuantileStateType.INSTANCE, false, AggregateType.QUANTILE_UNION, false, Optional.empty(), ""),
                KeysType.AGG_KEYS));
        Assertions.assertDoesNotThrow(() -> validateColumn(new ColumnDefinition(
                "v", aggStateType(), false, AggregateType.GENERIC, false, Optional.empty(), ""),
                KeysType.AGG_KEYS));
    }

    @Test
    public void testSystemGeneratedTableAllowsStateTypesInNonAggregateTable() {
        for (KeysType keysType : ImmutableList.of(KeysType.DUP_KEYS, KeysType.UNIQUE_KEYS)) {
            for (DataType type : aggregateTableOnlyTypes()) {
                ColumnDefinition column = new ColumnDefinition(
                        "v", type, false, null, false, Optional.empty(), "");
                Assertions.assertDoesNotThrow(() -> validateSystemGeneratedColumn(column, keysType));
            }
        }
    }

    private static ImmutableList<DataType> aggregateTableOnlyTypes() {
        return ImmutableList.of(HllType.INSTANCE, QuantileStateType.INSTANCE, aggStateType());
    }

    private static AggStateType aggStateType() {
        return new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(false), false);
    }

    private static void validateColumn(ColumnDefinition column, KeysType keysType) {
        column.validate(true, ImmutableSet.of("k"), ImmutableSet.of(), true, keysType);
    }

    private static void validateSystemGeneratedColumn(ColumnDefinition column, KeysType keysType) {
        column.validate(true, ImmutableSet.of("k"), ImmutableSet.of(), true, keysType, true);
    }

    @Test
    public void testAddColumnRejectsUuidDynamicDefaults() {
        for (String function : new String[] {"uuid_v4", "uuid_v7", "generateUUIDv4", "generate_uuid_v7"}) {
            ColumnDefinition column = new ColumnDefinition("u", UuidType.INSTANCE, false, null, false,
                    Optional.of(DefaultValue.uuidDefaultValue(function)), "");
            org.apache.doris.common.AnalysisException error = Assertions.assertThrows(
                    org.apache.doris.common.AnalysisException.class,
                    () -> AddColumnOp.validateColumnDef(null, column, null, null));
            Assertions.assertEquals("ADD COLUMN does not support UUID dynamic default values", error.getDetailMessage());
        }
        ColumnDefinition literal = new ColumnDefinition("u", UuidType.INSTANCE, false, null, false,
                Optional.of(new DefaultValue("00112233-4455-6677-8899-aabbccddeeff")), "");
        Assertions.assertFalse(literal.hasUuidDefaultValue());
    }

    @Test
    public void testComplexTypeLiteralDefaultValue() {
        ArrayType intArray = ArrayType.of(IntegerType.INSTANCE);
        assertCanonicalDefaultValue(intArray, "[]", "[]");
        assertCanonicalDefaultValue(intArray, "[1, 2]", "[1, 2]");
        // nested NULL is rendered in lower case: the BE JSON parser that fills the omitted columns of a
        // partial update only recognizes `null`, and `NULL` would be stored as the string "NULL"
        assertCanonicalDefaultValue(intArray, "[NULL, nUlL, 5]", "[null, null, 5]");
        assertCanonicalDefaultValue(intArray, "[\"7\", 1e3]", "[7, 1000]");
        assertRejectsDefaultValue(intArray, "{}", "only supports array literals or DEFAULT NULL");
        assertRejectsDefaultValue(intArray, "[1 + 1]", "only supports array literals or DEFAULT NULL");
        assertRejectsDefaultValue(intArray, "[\"bad\"]", "Invalid default value '[\"bad\"]' for ARRAY<INT>");
        assertRejectsDefaultValue(intArray, "[[1]]", "Invalid default value");

        assertCanonicalDefaultValue(ArrayType.of(DateV2Type.INSTANCE),
                "[DATEV2 \"2024-01-01\", \"2024-02-02\"]", "[\"2024-01-01\", \"2024-02-02\"]");
        // an item that cannot be cast to the type inferred from the other items is rejected instead of
        // being replaced by NULL, which the SQL literal parser does when enable_strict_cast is off
        Assertions.assertFalse(SessionVariable.enableStrictCast());
        assertRejectsDefaultValue(ArrayType.of(DateV2Type.INSTANCE), "[DATEV2 \"2024-01-01\", 1]",
                "Invalid default value '[DATEV2 \"2024-01-01\", 1]' for ARRAY<DATEV2> column");
        // the parser would infer DATEV2 for the items and turn 1 into NULL; each item is cast to INT instead
        assertCanonicalDefaultValue(intArray, "[DATEV2 \"2024-01-01\", 1]", "[20240101, 1]");
        assertRejectsDefaultValue(ArrayType.of(ArrayType.of(DateV2Type.INSTANCE)),
                "[[DATEV2 \"2024-01-01\"], [1]]", "Invalid default value");
        assertCanonicalDefaultValue(ArrayType.of(BooleanType.INSTANCE), "[true, false]", "[1, 0]");
        assertCanonicalDefaultValue(ArrayType.of(DoubleType.INSTANCE), "[1.5, 2]", "[1.5, 2.0]");
        // a SQL numeric literal has no negative zero, so the canonical text never contains -0.0
        assertCanonicalDefaultValue(ArrayType.of(DoubleType.INSTANCE), "[\"-0\", -0.0, \"-0.0\"]",
                "[0.0, 0.0, 0.0]");
        assertCanonicalDefaultValue(ArrayType.of(FloatType.INSTANCE), "[\"-0\", -0.0]", "[0.0, 0.0]");
        assertCanonicalDefaultValue(ArrayType.of(DoubleType.INSTANCE), "[\"-1.5\"]", "[-1.5]");
        assertCanonicalDefaultValue(ArrayType.of(DecimalV3Type.createDecimalV3Type(10, 2)), "[1.234]", "[1.23]");
        assertCanonicalDefaultValue(ArrayType.of(StringType.INSTANCE), "['x,y', \"[z]\", \"{k:v}\", '']",
                "[\"x,y\", \"[z]\", \"{k:v}\", \"\"]");
        assertRejectsDefaultValue(ArrayType.of(StringType.INSTANCE), "[\"a\"\"b\"]",
                "must not contain quote or backslash");
        assertRejectsDefaultValue(ArrayType.of(StringType.INSTANCE), "['it''s']",
                "must not contain quote or backslash");
        assertRejectsDefaultValue(ArrayType.of(StringType.INSTANCE), "['a\\\\b']",
                "must not contain quote or backslash");
        assertCanonicalDefaultValue(ArrayType.of(intArray), "[[1], [], NULL]", "[[1], [], null]");
        // a nested VARCHAR(n)/CHAR(n) value longer than n is rejected like a scalar default, instead of
        // being truncated by the cast to VARCHAR(n)
        assertCanonicalDefaultValue(ArrayType.of(VarcharType.createVarcharType(2)), "[\"ab\", 1]",
                "[\"ab\", \"1\"]");
        assertRejectsDefaultValue(ArrayType.of(VarcharType.createVarcharType(2)), "[\"abc\"]",
                "Default value is too long: abc");
        assertRejectsDefaultValue(ArrayType.of(VarcharType.createVarcharType(2)), "[123]",
                "Default value is too long: 123");
        assertCanonicalDefaultValue(ArrayType.of(CharType.createCharType(2)), "[\"ab\"]", "[\"ab\"]");
        assertRejectsDefaultValue(ArrayType.of(CharType.createCharType(2)), "[\"abc\"]",
                "Default value is too long: abc");
        // the declared CHAR/VARCHAR length is a byte limit: a multi-byte character fits in fewer UTF-16
        // code units than UTF-8 bytes, and a 4-byte character is only two UTF-16 code units
        assertRejectsDefaultValue(ArrayType.of(VarcharType.createVarcharType(2)), "[\"中\"]",
                "Default value is too long: 中");
        assertRejectsDefaultValue(ArrayType.of(CharType.createCharType(2)), "[\"中\"]",
                "Default value is too long: 中");
        assertCanonicalDefaultValue(ArrayType.of(VarcharType.createVarcharType(3)), "[\"中\"]", "[\"中\"]");
        assertRejectsDefaultValue(ArrayType.of(VarcharType.createVarcharType(2)), "[\"😀\"]",
                "Default value is too long: 😀");
        assertCanonicalDefaultValue(ArrayType.of(VarcharType.createVarcharType(4)), "[\"😀\"]", "[\"😀\"]");

        MapType stringIntMap = MapType.of(StringType.INSTANCE, IntegerType.INSTANCE);
        assertCanonicalDefaultValue(stringIntMap, "{}", "{}");
        assertCanonicalDefaultValue(stringIntMap, "{\"a\": 1, \"b\": NULL}", "{\"a\":1, \"b\":null}");
        assertCanonicalDefaultValue(stringIntMap, "{\"a\": 1, \"a\": 2}", "{\"a\":2}");
        assertRejectsDefaultValue(stringIntMap, "[]", "only supports map literals or DEFAULT NULL");
        assertRejectsDefaultValue(stringIntMap, "{\"a\": \"bad\"}", "Invalid default value");
        assertRejectsDefaultValue(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), "{\"bad\": 1}",
                "Invalid default value");
        assertRejectsDefaultValue(MapType.of(DateV2Type.INSTANCE, IntegerType.INSTANCE),
                "{DATEV2 \"2024-01-01\": 1, 1: 2}", "Invalid default value");
        assertRejectsDefaultValue(MapType.of(StringType.INSTANCE, DateV2Type.INSTANCE),
                "{\"a\": DATEV2 \"2024-01-01\", \"b\": 1}", "Invalid default value");
        // keys that are distinct literals but collide after the cast to the key type are rejected
        assertRejectsDefaultValue(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), "{\"01\": 1, \"1\": 2}",
                "map key 1 is repeated after casting to INT");
        assertCanonicalDefaultValue(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), "{\"01\": 1, 2: 2}",
                "{1:1, 2:2}");
        // MAP keys and values share the same byte-length check
        assertRejectsDefaultValue(MapType.of(VarcharType.createVarcharType(2), IntegerType.INSTANCE),
                "{\"中\": 1}", "Default value is too long: 中");
        assertRejectsDefaultValue(MapType.of(StringType.INSTANCE, VarcharType.createVarcharType(2)),
                "{\"k\": \"中\"}", "Default value is too long: 中");

        StructType structType = new StructType(Arrays.asList(
                new StructField("f1", IntegerType.INSTANCE, true, ""),
                new StructField("f2", StringType.INSTANCE, true, "")));
        assertCanonicalDefaultValue(structType, "{}", "{}");
        assertCanonicalDefaultValue(structType, "{1, \"a\"}", "{1, \"a\"}");
        assertCanonicalDefaultValue(structType, "{NULL, 2}", "{null, \"2\"}");
        assertRejectsDefaultValue(structType, "{\"f1\": 1, \"f2\": \"a\"}",
                "only supports struct literals or DEFAULT NULL");
        assertRejectsDefaultValue(structType, "{1}", "struct literal has 1 fields but the column has 2");
        assertRejectsDefaultValue(structType, "{\"bad\", \"a\"}", "Invalid default value");
        assertRejectsDefaultValue(new StructType(Arrays.asList(
                new StructField("f1", ArrayType.of(DateV2Type.INSTANCE), true, ""))),
                "{[DATEV2 \"2024-01-01\", 1]}", "Invalid default value");
        // STRUCT fields share the same byte-length check
        assertRejectsDefaultValue(new StructType(Arrays.asList(
                new StructField("f1", VarcharType.createVarcharType(2), true, ""))),
                "{\"中\"}", "Default value is too long: 中");
        assertRejectsDefaultValue(structType, "[]", "only supports struct literals or DEFAULT NULL");

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

    private void assertCanonicalDefaultValue(DataType type, String defaultValue, String canonical) {
        ColumnDefinition columnDefinition = newColumnDefinition(type, Optional.of(new DefaultValue(defaultValue)));
        columnDefinition.validate(true, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS);
        Assertions.assertEquals(canonical, columnDefinition.getDefaultValueString());
        // the canonical text is itself a valid default and is stable
        ColumnDefinition canonicalDefinition = newColumnDefinition(type, Optional.of(new DefaultValue(canonical)));
        canonicalDefinition.validate(true, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS);
        Assertions.assertEquals(canonical, canonicalDefinition.getDefaultValueString());
    }

    private void assertRejectsDefaultValue(DataType type, String defaultValue, String message) {
        ColumnDefinition columnDefinition = newColumnDefinition(type, Optional.of(new DefaultValue(defaultValue)));
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> columnDefinition.validate(
                true, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS));
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }

    private ColumnDefinition newColumnDefinition(DataType type, Optional<DefaultValue> defaultValue) {
        return new ColumnDefinition("col1", type, false, null, true, defaultValue, "");
    }
}
