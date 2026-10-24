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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    public void testRejectsSpatialTypesInInternalTables() {
        for (DataType type : ImmutableList.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"),
                ArrayType.of(new GeometryType("EPSG:3857")))) {
            ColumnDefinition column = new ColumnDefinition("shape", type, false, null, false,
                    Optional.empty(), "");
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> validateColumn(column, KeysType.DUP_KEYS));
            Assertions.assertEquals("GEOMETRY and GEOGRAPHY are not supported for Doris internal tables",
                    error.getMessage());
        }
    }

    @Test
    public void testNestedSpatialTypesRequireConnectorDdlAdmission() {
        for (DataType spatial : ImmutableList.of(new GeometryType("EPSG:3857"),
                new GeographyType("OGC:CRS84", "spherical"))) {
            StructType structure = new StructType(ImmutableList.of(new StructField("shape", spatial, true, "")));
            for (DataType type : ImmutableList.of(ArrayType.of(spatial), MapType.of(StringType.INSTANCE, spatial),
                    structure, MapType.of(StringType.INSTANCE, ArrayType.of(structure)))) {
                ColumnDefinition column = new ColumnDefinition("shape", type, false, null, true,
                        Optional.empty(), "");
                Assertions.assertDoesNotThrow(() -> column.validate(false, ImmutableSet.of(), ImmutableSet.of(),
                        false, KeysType.DUP_KEYS, false, true));
                Assertions.assertDoesNotThrow(() -> column.validateNestedColumn(false, ImmutableSet.of(),
                        ImmutableSet.of(), false, KeysType.DUP_KEYS, true));
                Assertions.assertThrows(AnalysisException.class, type::validateDataType);
                Assertions.assertThrows(AnalysisException.class,
                        () -> column.validate(false, ImmutableSet.of(), ImmutableSet.of(), false, KeysType.DUP_KEYS));
                AnalysisException internal = Assertions.assertThrows(AnalysisException.class,
                        () -> column.validate(true, ImmutableSet.of(), ImmutableSet.of(), false,
                                KeysType.DUP_KEYS, false, true));
                Assertions.assertTrue(internal.getMessage().contains("not supported for Doris internal tables"));
            }
            // Do not admit spatial keys or unrelated unsupported ordinary fields with the new flag.
            Assertions.assertThrows(AnalysisException.class,
                    () -> MapType.of(spatial, IntegerType.INSTANCE).validateDataTypeWithSpatialLeaves());
            StructType unsupportedSibling = new StructType(ImmutableList.of(
                    new StructField("shape", spatial, true, ""),
                    new StructField("payload", JsonType.INSTANCE, true, "")));
            Assertions.assertThrows(AnalysisException.class, unsupportedSibling::validateDataTypeWithSpatialLeaves);
        }
        Assertions.assertDoesNotThrow(() -> ArrayType.of(IntegerType.INSTANCE).validateDataType());
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
}
