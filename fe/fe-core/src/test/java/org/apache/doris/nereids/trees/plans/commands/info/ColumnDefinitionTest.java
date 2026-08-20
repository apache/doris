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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.StringType;

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
        Config.allow_non_aggregate_table_state_types = false;
    }

    @AfterEach
    public void tearDown() {
        Config.allow_non_aggregate_table_state_types = false;
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
        Config.allow_non_aggregate_table_state_types = true;

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
    public void testInternalSessionAllowsStateTypesInNonAggregateTable() {
        for (KeysType keysType : ImmutableList.of(KeysType.DUP_KEYS, KeysType.UNIQUE_KEYS)) {
            for (DataType type : aggregateTableOnlyTypes()) {
                ColumnDefinition column = new ColumnDefinition(
                        "v", type, false, null, false, Optional.empty(), "");
                Assertions.assertDoesNotThrow(() -> validateInternalColumn(column, keysType));
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

    private static void validateInternalColumn(ColumnDefinition column, KeysType keysType) {
        column.validate(true, ImmutableSet.of("k"), ImmutableSet.of(), true, keysType, true);
    }
}
