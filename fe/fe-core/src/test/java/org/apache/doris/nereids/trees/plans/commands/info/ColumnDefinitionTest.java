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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToProtobuf;
import org.apache.doris.catalog.ColumnToThrift;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TColumn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
    public void testTimestampTzSchemaChangeCurrentTimestampDefaults() throws Exception {
        TimeStampTzType type = TimeStampTzType.of(6);
        DefaultValue[] defaults = {
                DefaultValue.CURRENT_TIMESTAMP_DEFAULT_VALUE,
                DefaultValue.currentTimeStampDefaultValueWithPrecision(0L, type),
                DefaultValue.currentTimeStampDefaultValueWithPrecision(3L, type),
                DefaultValue.currentTimeStampDefaultValueWithPrecision(6L, type)
        };
        for (DefaultValue defaultValue : defaults) {
            ColumnDefinition definition = new ColumnDefinition("ts", type,
                    false, null, true, Optional.of(defaultValue), "");
            definition.validate(false, ImmutableSet.of(), ImmutableSet.of(), false, KeysType.DUP_KEYS);
            LocalDateTime before = LocalDateTime.now(ZoneOffset.UTC).withNano(0);
            Column column = definition.translateToCatalogStyleForSchemaChange();
            LocalDateTime after = LocalDateTime.now(ZoneOffset.UTC);

            Assertions.assertEquals(defaultValue.getValue(), column.getDefaultValue());
            Assertions.assertNotNull(column.getDefaultValueExprDef());
            String rawValue = column.getRealDefaultValue();
            Assertions.assertTrue(rawValue.endsWith("+00:00"));
            TColumn thriftColumn = ColumnToThrift.toThrift(column);
            Assertions.assertEquals(rawValue, thriftColumn.getDefaultValue());
            Assertions.assertEquals(defaultValue.getValue(), thriftColumn.getDefaultValueExpr());
            OlapFile.ColumnPB protobufColumn = ColumnToProtobuf.toPb(column, null, null);
            Assertions.assertEquals(rawValue, protobufColumn.getDefaultValue().toStringUtf8());
            Assertions.assertEquals(defaultValue.getValue(), protobufColumn.getDefaultValueExpr().toStringUtf8());
            LocalDateTime instant = new TimestampTzLiteral(type, rawValue).toJavaDateType();
            Assertions.assertFalse(instant.isBefore(before));
            Assertions.assertFalse(instant.isAfter(after));
            int precision = (int) defaultValue.getCurrentTimeStampPrecision();
            Assertions.assertEquals(0, instant.getNano() % (int) Math.pow(10, 9 - precision));
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
}
