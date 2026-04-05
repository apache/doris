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

package org.apache.doris.common.util;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HMSPartitionsUtilTest {

    private SlotReference slotRef;
    private IntegerLiteral intLiteral;
    private StringLiteral stringLiteral;
    private List<Column> partitionColumns;
    private Map<String, DataType> nameToType;
    private List<NamedExpression> unionOutputs;
    private Collection<PartitionItem> partitionItems;

    @BeforeEach
    public void setUp() throws AnalysisException {
        // Initialize common test objects
        slotRef = new SlotReference("col1", IntegerType.INSTANCE);
        intLiteral = new IntegerLiteral(100);
        stringLiteral = new StringLiteral("test");

        // Setup partition columns
        partitionColumns = Arrays.asList(
            new Column("year", PrimitiveType.INT),
            new Column("month", PrimitiveType.VARCHAR),
            new Column("date", PrimitiveType.DATEV2),
            new Column("datetime", PrimitiveType.DATETIMEV2)
        );

        // Setup name to type mapping
        nameToType = Maps.newHashMap();
        nameToType.put("year", IntegerType.INSTANCE);
        nameToType.put("month", VarcharType.createVarcharType(10));
        nameToType.put("date", DateV2Type.INSTANCE);
        nameToType.put("datetime", DateTimeV2Type.SYSTEM_DEFAULT);

        // Setup union outputs
        unionOutputs = Arrays.asList(
            new SlotReference("year", IntegerType.INSTANCE),
            new SlotReference("month", VarcharType.createVarcharType(10)),
            new SlotReference("date", DateV2Type.INSTANCE),
            new SlotReference("datetime", DateTimeV2Type.SYSTEM_DEFAULT)
        );

        // Setup partition items
        PartitionKey pk1 = PartitionKey.createPartitionKey(
                Arrays.asList(new PartitionValue("2023"), new PartitionValue("01"), new PartitionValue("2023-01-01"),
                    new PartitionValue("2023-01-01 10:00:00")), partitionColumns);
        PartitionKey pk2 = PartitionKey.createPartitionKey(
                Arrays.asList(new PartitionValue("2023"), new PartitionValue("02"), new PartitionValue("2023-02-01"),
                    new PartitionValue("2023-02-01 10:00:00")), partitionColumns);

        PartitionKey pk3 = PartitionKey.createPartitionKey(
                Arrays.asList(new PartitionValue("", true), new PartitionValue("02"),
                    new PartitionValue("2023-03-01"), new PartitionValue("2023-03-01 10:00:00")), partitionColumns);

        partitionItems = Arrays.asList(
            new ListPartitionItem(Arrays.asList(pk1)),
            new ListPartitionItem(Arrays.asList(pk2)),
            new ListPartitionItem(Arrays.asList(pk3))
        );
    }

    @Test
    public void testCheckSelectedPartitionNumLimit(@Injectable HMSExternalTable table)  {
        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HUDI;
                table.getDbName();
                result = "test";
                table.getName();
                result = "test";
            }
        };
        try {
            Config.max_selected_partition_num_for_lakehouse_table = 5;
            HMSPartitionsUtil.checkSelectedPartitionNumLimit(table, 10);
            Assertions.fail("check selected partition num should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertEquals("errCode = 2, detailMessage = the selected partition num: 10 for"
                    + " test.test has exceed max selected partition num for single hudi table: 5", e.getMessage());
        }

        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HIVE;
            }
        };

        try {
            Config.max_selected_partition_num_for_hive_table = 5;
            HMSPartitionsUtil.checkSelectedPartitionNumLimit(table, 10);
            Assertions.fail("check selected partition num should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertEquals("errCode = 2, detailMessage = the selected partition num: 10 for"
                    + " test.test has exceed max selected partition num for single hive table: 5", e.getMessage());
        }
    }

    @Test
    public void testCheckSelectedFileNumLimit(@Injectable HMSExternalTable table)  {
        new Expectations() {
            {
                table.getDbName();
                result = "test";
                table.getName();
                result = "test";
            }
        };
        try {
            Config.max_selected_total_file_num_for_hive_table = 1000;
            HMSPartitionsUtil.checkSelectedFileNumLimit(table, 2000);
            Assertions.fail("check selected file num should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertEquals("errCode = 2, detailMessage = the selected file num: 2000 for test.test"
                    + " has exceed max selected file num for single hive table: 1000", e.getMessage());
        }
    }

    @Test
    public void testCheckSelectedSplitNumLimit(@Injectable HMSExternalTable table)  {
        new Expectations() {
            {
                table.getDbName();
                result = "test";
                table.getName();
                result = "test";
            }
        };
        try {
            Config.max_selected_total_split_num_for_hms_table = 1000;
            HMSPartitionsUtil.checkSelectedSplitNumLimit(table, 2000);
            Assertions.fail("check selected file num should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertEquals("errCode = 2, detailMessage = the selected split num: 2000 for test.test"
                    + " has exceed max selected split num for single hms table: 1000", e.getMessage());
        }
    }

    // ===== POSITIVE TEST CASES FOR VALIDATION METHODS =====

    @Test
    public void testCheckSelectedPartitionNumLimitSuccess(@Injectable HMSExternalTable table) {
        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HUDI;
            }
        };

        Config.max_selected_partition_num_for_lakehouse_table = 100;
        // Should not throw exception when within limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedPartitionNumLimit(table, 50));

        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HIVE;
            }
        };

        Config.max_selected_partition_num_for_hive_table = 200;
        // Should not throw exception when within limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedPartitionNumLimit(table, 100));
    }

    @Test
    public void testCheckSelectedPartitionNumLimitBoundary(@Injectable HMSExternalTable table) {
        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HUDI;
            }
        };

        Config.max_selected_partition_num_for_lakehouse_table = 100;
        // Should not throw exception when exactly at limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedPartitionNumLimit(table, 100));

        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HIVE;
            }
        };

        Config.max_selected_partition_num_for_hive_table = 200;
        // Should not throw exception when exactly at limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedPartitionNumLimit(table, 200));
    }

    @Test
    public void testCheckSelectedFileNumLimitSuccess(@Injectable HMSExternalTable table) {
        new Expectations() {
            {
                table.getDbName();
                result = "test";
                minTimes = 0;

                table.getName();
                result = "test";
                minTimes = 0;
            }
        };

        Config.max_selected_total_file_num_for_hive_table = 1000;
        // Should not throw exception when within limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedFileNumLimit(table, 500));

        // Should not throw exception when exactly at limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedFileNumLimit(table, 1000));
    }

    @Test
    public void testCheckSelectedSplitNumLimitSuccess(@Injectable HMSExternalTable table) {
        new Expectations() {
            {
                table.getDbName();
                result = "test";
                minTimes = 0;

                table.getName();
                result = "test";
                minTimes = 0;
            }
        };

        Config.max_selected_total_split_num_for_hms_table = 2000;
        // Should not throw exception when within limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedSplitNumLimit(table, 1000));

        // Should not throw exception when exactly at limit
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedSplitNumLimit(table, 2000));
    }

    // ===== COMPREHENSIVE TESTS FOR isFilterSupportedByListPartitions =====

    @Test
    public void testIsFilterSupportedByListPartitions_BasicExpressions() {
        // Test SlotReference - should return true
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(slotRef));

        // Test Literal - should return true
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(intLiteral));
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(stringLiteral));
    }

    @Test
    public void testIsFilterSupportedByListPartitions_BinaryOperators() {
        // Test EqualTo
        EqualTo equalTo = new EqualTo(slotRef, intLiteral);
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(equalTo));

        // Test GreaterThan
        GreaterThan greaterThan = new GreaterThan(slotRef, intLiteral);
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(greaterThan));

        // Test GreaterThanEqual
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotRef, intLiteral);
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(greaterThanEqual));

        // Test LessThan
        LessThan lessThan = new LessThan(slotRef, intLiteral);
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(lessThan));

        // Test LessThanEqual
        LessThanEqual lessThanEqual = new LessThanEqual(slotRef, intLiteral);
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(lessThanEqual));

        // Test And
        And and = new And(equalTo, greaterThan);
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(and));

        // Test Or
        Or or = new Or(equalTo, lessThan);
        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(or));
    }

    @Test
    public void testIsFilterSupportedByListPartitions_ComplexNested() {
        // Create complex nested expression: (col1 = 100 AND col1 > 50) OR (col1 < 200)
        EqualTo equalTo = new EqualTo(slotRef, intLiteral);
        GreaterThan greaterThan = new GreaterThan(slotRef, new IntegerLiteral(50));
        LessThan lessThan = new LessThan(slotRef, new IntegerLiteral(200));

        And and = new And(equalTo, greaterThan);
        Or complexOr = new Or(and, lessThan);

        Assertions.assertTrue(HMSPartitionsUtil.isFilterSupportedByListPartitions(complexOr));
    }

    @Test
    public void testIsFilterSupportedByListPartitions_UnsupportedExpressions() {
        // Test unsupported expression (Sum function)
        Sum sumExpr = new Sum(slotRef);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(sumExpr));
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(new NullLiteral()));
    }

    @Test
    public void testIsFilterSupportedByListPartitions_MixedSupported() {
        // Test binary operator with one unsupported child
        Sum sumExpr = new Sum(slotRef);
        EqualTo mixedEqual = new EqualTo(sumExpr, intLiteral);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(mixedEqual));

        // Test And with one unsupported child
        EqualTo supportedEqual = new EqualTo(slotRef, intLiteral);
        And mixedAnd = new And(supportedEqual, mixedEqual);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(mixedAnd));
    }

    @Test
    public void testIsFilterSupportedByListPartitions_ColumnToColumnComparison() {
        // Test column = column (not supported by HMS)
        SlotReference col1 = new SlotReference("dp", StringType.INSTANCE);
        SlotReference col2 = new SlotReference("dt", StringType.INSTANCE);

        EqualTo columnEqualColumn = new EqualTo(col1, col2);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(columnEqualColumn));

        GreaterThan columnGtColumn = new GreaterThan(col1, col2);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(columnGtColumn));

        GreaterThanEqual columnGteColumn = new GreaterThanEqual(col1, col2);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(columnGteColumn));

        LessThan columnLtColumn = new LessThan(col1, col2);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(columnLtColumn));

        LessThanEqual columnLteColumn = new LessThanEqual(col1, col2);
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(columnLteColumn));

        GreaterThanEqual dtGteLiteral = new GreaterThanEqual(
                new SlotReference("dt", StringType.INSTANCE),
                new StringLiteral("2023-03-01")
        );
        And complexAnd = new And(dtGteLiteral, columnEqualColumn);
        // Since one part is not supported, the whole expression is not supported
        Assertions.assertFalse(HMSPartitionsUtil.isFilterSupportedByListPartitions(complexAnd));
    }

    // ===== COMPREHENSIVE TESTS FOR buildConstantExpressionsFromPartitions =====

    @Test
    public void testBuildConstantExpressionsFromPartitions_Basic() {
        List<List<NamedExpression>> result = HMSPartitionsUtil.buildConstantExpressionsFromPartitions(
                partitionItems, partitionColumns, nameToType, unionOutputs);

        // Should have 3 partition items
        Assertions.assertEquals(3, result.size());

        // Each partition should have 2 columns
        Assertions.assertEquals(4, result.get(0).size());
        Assertions.assertEquals(4, result.get(1).size());

        // Verify first partition expressions
        NamedExpression yearExpr = result.get(0).get(0);
        NamedExpression monthExpr = result.get(0).get(1);

        Assertions.assertTrue(yearExpr instanceof Alias);
        Assertions.assertTrue(monthExpr instanceof Alias);
        Assertions.assertEquals("year", yearExpr.getName());
        Assertions.assertEquals("month", monthExpr.getName());
    }

    @Test
    public void testBuildConstantExpressionsFromPartitions_MissingColumn() throws AnalysisException {
        // Create name to type map missing one column
        Map<String, DataType> partialNameToType = Maps.newHashMap();
        partialNameToType.put("year", IntegerType.INSTANCE);
        // "month" is missing

        List<List<NamedExpression>> result = HMSPartitionsUtil.buildConstantExpressionsFromPartitions(
                partitionItems, partitionColumns, partialNameToType, unionOutputs);

        // Should still have 3 partition items
        Assertions.assertEquals(3, result.size());

        // Each partition should have only 1 expression (only year)
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertEquals(1, result.get(1).size());

        // Verify that only year column is present
        Assertions.assertEquals("year", result.get(0).get(0).getName());
    }

    @Test
    public void testBuildConstantExpressionsFromPartitions_SingleColumn() throws AnalysisException {
        // Test with single partition column
        List<Column> singleColumn = Arrays.asList(new Column("id", PrimitiveType.INT));
        Map<String, DataType> singleNameToType = Maps.newHashMap();
        singleNameToType.put("id", IntegerType.INSTANCE);
        List<NamedExpression> singleUnionOutputs = Arrays.asList(
            new SlotReference("id", IntegerType.INSTANCE)
        );

        PartitionKey pk = PartitionKey.createPartitionKey(
                Arrays.asList(new PartitionValue("123")), singleColumn);
        Collection<PartitionItem> singlePartitionItems = Arrays.asList(
            new ListPartitionItem(Arrays.asList(pk))
        );

        List<List<NamedExpression>> result = HMSPartitionsUtil.buildConstantExpressionsFromPartitions(
                singlePartitionItems, singleColumn, singleNameToType, singleUnionOutputs);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertEquals("id", result.get(0).get(0).getName());
    }

    @Test
    public void testBuildConstantExpressionsFromPartitions_EmptyPartitions() {
        List<List<NamedExpression>> result = HMSPartitionsUtil.buildConstantExpressionsFromPartitions(
                ImmutableList.of(), partitionColumns, nameToType, unionOutputs);

        // Should return empty list for empty partitions
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testBuildConstantExpressionsFromPartitions_TypeCoercion() throws AnalysisException {
        // Test type coercion by having different types in nameToType vs unionOutputs
        Map<String, DataType> differentNameToType = Maps.newHashMap();
        differentNameToType.put("year", StringType.INSTANCE); // Different from unionOutputs (IntegerType)
        differentNameToType.put("month", VarcharType.createVarcharType(10));

        List<List<NamedExpression>> result = HMSPartitionsUtil.buildConstantExpressionsFromPartitions(
                partitionItems, partitionColumns, differentNameToType, unionOutputs);

        // Should still work with type coercion
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(2, result.get(0).size());

        // Verify expressions are created
        NamedExpression yearExpr = result.get(0).get(0);
        Assertions.assertTrue(yearExpr instanceof Alias);
        Assertions.assertEquals("year", yearExpr.getName());
    }

    // ===== EDGE CASES AND ERROR CONDITIONS =====

    @Test
    public void testValidationMethods_EdgeCases(@Injectable HMSExternalTable table) {
        new Expectations() {
            {
                table.getDlaType();
                result = HMSExternalTable.DLAType.HUDI;
                minTimes = 0;

                table.getDbName();
                result = "edge_test";
                minTimes = 0;

                table.getName();
                result = "edge_table";
                minTimes = 0;
            }
        };

        // Test with zero values - should succeed for 0 partitions
        Config.max_selected_partition_num_for_lakehouse_table = 1;
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedPartitionNumLimit(table, 0));

        // Test file limit with zero - should succeed for 0 files
        Config.max_selected_total_file_num_for_hive_table = 1;
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedFileNumLimit(table, 0));

        // Test split limit with zero - should succeed for 0 splits
        Config.max_selected_total_split_num_for_hms_table = 1;
        Assertions.assertDoesNotThrow(() ->
                HMSPartitionsUtil.checkSelectedSplitNumLimit(table, 0));
    }
}
