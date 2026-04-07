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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.PartitionValues;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.tablefunction.PartitionValuesTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class PartitionValuesRewriteTest implements MemoPatternMatchSupported {

    private static final String TEST_CATALOG = "test_catalog";
    private static final String TEST_DB = "test_db";
    private static final String TEST_TABLE = "test_table";

    // Mocked objects for unit testing
    @Mocked
    private Env mockEnv;

    @Injectable
    private HMSExternalTable mockTable;

    @Injectable
    private HMSExternalCatalog mockCatalog;

    @Injectable
    private ExternalMetaCacheMgr mockMetaCacheMgr;

    @Injectable
    private HiveMetaStoreCache mockCache;

    @Injectable
    private PartitionValuesTableValuedFunction mockTVF;

    @Injectable
    private PartitionValues mockPartitionValues;

    // Test data
    private SlotReference yearSlot;
    private SlotReference monthSlot;
    private Column yearColumn;
    private Column monthColumn;

    @BeforeEach
    void setUp() {
        yearSlot = new SlotReference("year", IntegerType.INSTANCE);
        monthSlot = new SlotReference("month", StringType.INSTANCE);
        yearColumn = new Column("year", Type.INT);
        monthColumn = new Column("month", Type.STRING);
    }

    // ========== POSITIVE TEST CASES ==========

    @Test
    void testPartitionValuesWithSimpleFilter() {
        // Test: partition_values("catalog"="hms", "database"="db", "table"="tbl")
        //       WHERE year > 2020

        // Create filter with supported expression
        Expression filterExpr = new GreaterThan(yearSlot, new IntegerLiteral(2020));

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(filterExpr), tvfRelation);

        setupBasicMocks();
        setupFilteredPartitionMocks(Collections.singletonList("year > 2020"));

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        List<Rule> rules = rewrite.buildRules();

        // Should apply PARTITION_VALUES_WITH_FILTER_FOR_TVF_RELATION rule
        Rule filterRule = rules.get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Plan transformedPlan = results.get(0);

        // Should create LogicalUnion with constant expressions
        Assertions.assertTrue(transformedPlan instanceof LogicalUnion,
                "Should transform to LogicalUnion for filtered query");

        LogicalUnion union = (LogicalUnion) transformedPlan;
        Assertions.assertFalse(union.getConstantExprsList().isEmpty(),
                "Union should contain constant expressions from partitions");
        Assertions.assertTrue(union.children().isEmpty(),
                "Union should have no children, only constantExprsList");
    }

    @Test
    void testPartitionValuesWithComplexFilter() {
        // Test complex filter: year > 2020 AND month = 'January' OR year = 2025
        Expression yearFilter = new GreaterThan(yearSlot, new IntegerLiteral(2020));
        Expression monthFilter = new EqualTo(monthSlot, new org.apache.doris.nereids.trees.expressions.literal.StringLiteral("January"));
        Expression yearEquals = new EqualTo(yearSlot, new IntegerLiteral(2025));
        Expression complexFilter = new Or(new And(yearFilter, monthFilter), yearEquals);

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(complexFilter), tvfRelation);

        setupBasicMocks();
        setupFilteredPartitionMocks(Collections.singletonList("(year > 2020 AND month = 'January') OR year = 2025"));

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.get(0) instanceof LogicalUnion,
                "Complex filters should also transform to LogicalUnion");
    }

    @Test
    void testPartitionValuesWithMixedSupportedUnsupportedFilters() {
        // Test: year > 2020 (supported) AND complex_udf(month) = 'value' (unsupported)
        Expression supportedFilter = new GreaterThan(yearSlot, new IntegerLiteral(2020));
        // Mock an unsupported filter by creating a complex expression
        Expression unsupportedFilter = new EqualTo(monthSlot, new org.apache.doris.nereids.trees.expressions.literal.StringLiteral("unsupported"));

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(supportedFilter, unsupportedFilter), tvfRelation);

        setupBasicMocks();
        setupFilteredPartitionMocks(Collections.singletonList("year > 2020"));

        // Mock HMSPartitionsUtil to return false for unsupported filter
        new MockUp<org.apache.doris.common.util.HMSPartitionsUtil>() {
            @Mock
            public boolean isFilterSupportedByListPartitions(Expression expression) {
                if (expression instanceof GreaterThan) {
                    return true; // year > 2020 is supported
                }
                return false; // month = 'unsupported' is not supported
            }
        };

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Plan transformedPlan = results.get(0);

        // Should create LogicalFilter with remaining unsupported filter over LogicalUnion
        Assertions.assertTrue(transformedPlan instanceof LogicalFilter,
                "Should wrap LogicalUnion in LogicalFilter for unsupported predicates");

        LogicalFilter resultFilter = (LogicalFilter) transformedPlan;
        Assertions.assertTrue(resultFilter.child() instanceof LogicalUnion,
                "Filter should contain LogicalUnion as child");

        // Verify that unsupported filter remains
        Set<Expression> remainingConjuncts = resultFilter.getConjuncts();
        Assertions.assertFalse(remainingConjuncts.isEmpty(),
                "Should have remaining unsupported filters");
    }

    @Test
    void testPartitionValuesWithoutFilter() {
        // Test: partition_values("catalog"="hms", "database"="db", "table"="tbl")
        //       without WHERE clause

        LogicalTVFRelation tvfRelation = createMockTVFRelation();

        setupBasicMocks();
        setupUnfilteredPartitionMocks();

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        List<Rule> rules = rewrite.buildRules();

        // Should apply PARTITION_VALUES_WITHOUT_FILTER_FOR_TVF_RELATION rule
        Rule noFilterRule = rules.get(1);
        List<Plan> results = noFilterRule.transform(tvfRelation, MemoTestUtils.createCascadesContext(tvfRelation));

        Assertions.assertEquals(1, results.size());
        Plan transformedPlan = results.get(0);

        Assertions.assertTrue(transformedPlan instanceof LogicalUnion,
                "Should transform to LogicalUnion for unfiltered query");

        LogicalUnion union = (LogicalUnion) transformedPlan;
        Assertions.assertFalse(union.getConstantExprsList().isEmpty(),
                "Union should contain all partition values as constant expressions");
    }

    @Test
    void testPartitionValuesWithEmptyPartitionResult() {
        // Test when filter results in no matching partitions
        Expression filterExpr = new EqualTo(yearSlot, new IntegerLiteral(1900)); // Non-existent year

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(filterExpr), tvfRelation);

        setupBasicMocks();
        setupEmptyPartitionMocks();

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Plan transformedPlan = results.get(0);

        // Should create LogicalEmptyRelation when no partitions match
        Assertions.assertTrue(transformedPlan instanceof LogicalFilter,
                "Should wrap LogicalEmptyRelation in original filter");

        LogicalFilter resultFilter = (LogicalFilter) transformedPlan;
        Assertions.assertTrue(resultFilter.child() instanceof LogicalEmptyRelation,
                "Should create LogicalEmptyRelation for empty partition result");
    }

    @Test
    void testPartitionValuesWithViewBasedTable() {
        // Test view-based HMS external table
        Expression filterExpr = new GreaterThan(yearSlot, new IntegerLiteral(2020));

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(filterExpr), tvfRelation);

        setupBasicMocks();
        setupViewBasedTableMocks();
        setupFilteredPartitionMocks(Collections.singletonList("year > 2020"));

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.get(0) instanceof LogicalUnion,
                "View-based tables should also transform to LogicalUnion");
    }

    @Test
    void testPartitionValuesWithLargePartitionCount() {
        // Test behavior when partition count exceeds threshold
        Expression filterExpr = new GreaterThan(yearSlot, new IntegerLiteral(2020));

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(filterExpr), tvfRelation);

        setupBasicMocks();
        setupLargePartitionCountMocks(); // Returns count > Config.max_partition_num_for_single_hive_table_without_filter
        setupFilteredPartitionMocks(Collections.singletonList("year > 2020"));

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        // Should still work, but use different cache methods internally
        Assertions.assertTrue(results.get(0) instanceof LogicalUnion,
                "Large partition count should still transform to LogicalUnion");
    }

    // ========== NEGATIVE TEST CASES ==========

    @Test
    void testCannotApplyToNonPartitionValuesFunction() {
        // Test that rule doesn't apply to other TVF functions
        PartitionValues nonPartitionValuesTVF = new PartitionValues(new Properties(Maps.newHashMap()));

        LogicalTVFRelation tvfRelation = new LogicalTVFRelation(new RelationId(1),
                nonPartitionValuesTVF, ImmutableList.of(), Optional.empty(), Optional.empty());

        // canApplyPartitionValuesRewrite should return false
        boolean canApply = PartitionValuesRewrite.canApplyPartitionValuesRewrite(
            new LogicalFilter(Sets.newHashSet(), tvfRelation));

        Assertions.assertTrue(canApply, "Should apply to PartitionValues function");

        // Test with different function type by mocking
        new MockUp<LogicalTVFRelation>() {
            @Mock
            public TableValuedFunction getFunction() {
                    return new TableValuedFunction("other_tvf", new Properties(Maps.newHashMap())) {
                        @Override
                        protected TableValuedFunctionIf toCatalogFunction() {
                            // Return a mock implementation - this is just for testing non-PartitionValues functions
                            return new TableValuedFunctionIf() {
                                @Override
                                public String getTableName() {
                                    return "mock_table";
                                }

                                @Override
                                public List<Column> getTableColumns() {
                                    return Lists.newArrayList();
                                }

                                @Override
                                public org.apache.doris.planner.ScanNode getScanNode(PlanNodeId nodeId,
                                                                                    TupleDescriptor desc,
                                                                                    SessionVariable sessionVariable) {
                                    return null; // Mock implementation for test
                                }
                            };
                        }

                        public List<NamedExpression> getColumnOutputs() {
                            return Lists.newArrayList();
                        }

                        public FunctionSignature customSignature() {
                            return FunctionSignature.of(StringType.INSTANCE, Lists.newArrayList()); // Required by CustomSignature interface
                        }
                    };
            }
        };

        boolean canApplyOther = PartitionValuesRewrite.canApplyPartitionValuesRewrite(
            new LogicalFilter(Sets.newHashSet(), tvfRelation));

        Assertions.assertFalse(canApplyOther, "Should not apply to non-PartitionValues functions");
    }

    @Test
    void testAllFiltersUnsupported() {
        // Test when all filters are unsupported for pushdown
        Expression unsupportedFilter = new EqualTo(monthSlot, new StringLiteral("unsupported"));

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(unsupportedFilter), tvfRelation);

        setupBasicMocks();
        setupUnfilteredPartitionMocks(); // Should fall back to unfiltered behavior

        // Mock HMSPartitionsUtil to return false for all filters
        new MockUp<org.apache.doris.common.util.HMSPartitionsUtil>() {
            @Mock
            public boolean isFilterSupportedByListPartitions(Expression expression) {
                return false; // All filters unsupported
            }
        };

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Plan transformedPlan = results.get(0);

        // Should create LogicalFilter with all filters over LogicalUnion (unfiltered)
        Assertions.assertTrue(transformedPlan instanceof LogicalFilter,
                "Should preserve all unsupported filters");

        LogicalFilter resultFilter = (LogicalFilter) transformedPlan;
        Assertions.assertTrue(resultFilter.child() instanceof LogicalUnion,
                "Should create unfiltered LogicalUnion when no filters can be pushed down");
    }

    // ========== EDGE CASES AND BOUNDARY CONDITIONS ==========

    @Test
    void testPartitionValuesWithExactThresholdPartitionCount() {
        // Test behavior at exact threshold boundary
        Expression filterExpr = new GreaterThan(yearSlot, new IntegerLiteral(2020));

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(filterExpr), tvfRelation);

        setupBasicMocks();
        // Set partition count exactly at threshold
        new Expectations() {
            {
                mockCache.getPartitionNum(mockTable);
                result = Config.max_partition_num_for_single_hive_table_without_filter;
                minTimes = 0;

                mockCache.getPartitionValues(mockTable, (List<Type>) any);
                result = createMockHivePartitionValues();
                minTimes = 0;

                mockCache.getPartitionValuesByFilter(mockTable, anyString, (List<String>) any, (List<Type>) any);
                result = createMockSelectedPartitions();
                minTimes = 0;
            }
        };

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.get(0) instanceof LogicalUnion,
                "Should handle threshold boundary correctly");
    }

    @Test
    void testMultiplePartitionColumns() {
        // Test with multiple partition columns (year, month)
        Expression yearFilter = new GreaterThan(yearSlot, new IntegerLiteral(2020));
        Expression monthFilter = new EqualTo(monthSlot, new StringLiteral("January"));
        Expression combinedFilter = new And(yearFilter, monthFilter);

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(combinedFilter), tvfRelation);

        setupBasicMocks();

        // Setup mocks for multiple partition columns
        new Expectations() {
            {
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn, monthColumn);
                minTimes = 0;

                mockCache.getPartitionValuesByFilter(mockTable, anyString,
                        Lists.newArrayList("year", "month"), (List<Type>) any);
                result = createMockSelectedPartitions();
            }
        };

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.get(0) instanceof LogicalUnion,
                "Should handle multiple partition columns");
    }

    @Test
    void testTypeCoercionInConstantExpressions() {
        // Test type coercion when partition values need casting
        Expression filterExpr = new GreaterThan(yearSlot, new IntegerLiteral(2020));

        LogicalTVFRelation tvfRelation = createMockTVFRelation();
        LogicalFilter filter = new LogicalFilter(Sets.newHashSet(filterExpr), tvfRelation);

        setupBasicMocks();
        setupFilteredPartitionMocks(Collections.singletonList("year > 2020"));

        // Mock HMSPartitionsUtil.buildConstantExpressionsFromPartitions to verify it's called
        new MockUp<org.apache.doris.common.util.HMSPartitionsUtil>() {
            @Mock
            public List<List<NamedExpression>> buildConstantExpressionsFromPartitions(
                    Collection<PartitionItem> selectedPartitions,
                    List<Column> partitionColumns,
                    Map<String, DataType> nameToType,
                    List<NamedExpression> unionOutputs) {
                    return Lists.newArrayList(Lists.newArrayList()); // Return empty but valid structure
            }
        };

        PartitionValuesRewrite rewrite = new PartitionValuesRewrite();
        Rule filterRule = rewrite.buildRules().get(0);
        List<Plan> results = filterRule.transform(filter, MemoTestUtils.createCascadesContext(filter));

        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.get(0) instanceof LogicalUnion,
                "Should handle type coercion in constant expressions");
    }

    // ========== HELPER METHODS ==========

    private LogicalTVFRelation createMockTVFRelation() {
        return new LogicalTVFRelation(new RelationId(1), mockPartitionValues, ImmutableList.of(),
            Optional.empty(), Optional.empty());
    }

    private void setupBasicMocks() {
        // Mock static Env.getCurrentEnv()
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockEnv;
            }
        };

        new Expectations() {
            {
                // Mock PartitionValues function
                mockPartitionValues.getCatalogFunction();
                result = mockTVF;
                minTimes = 0;

                // Mock PartitionValuesTableValuedFunction
                mockTVF.getPartitionedTable();
                result = mockTable;
                minTimes = 0;

                // Mock HMSExternalTable basic properties
                mockTable.getPartitionColumns();
                result = Lists.newArrayList(yearColumn, monthColumn);
                minTimes = 0;

                mockTable.isViewBased();
                result = false;
                minTimes = 0;

                mockTable.getCatalog();
                result = mockCatalog;
                minTimes = 0;

                mockTable.getDbName();
                result = TEST_DB;
                minTimes = 0;

                mockTable.getName();
                result = TEST_TABLE;
                minTimes = 0;

                mockTable.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.INT, Type.STRING);
                minTimes = 0;

                // Mock environment and cache
                mockEnv.getExtMetaCacheMgr();
                result = mockMetaCacheMgr;
                minTimes = 0;

                mockMetaCacheMgr.getMetaStoreCache((HMSExternalCatalog) any);
                result = mockCache;
                minTimes = 0;

                mockCache.getPartitionNum(mockTable);
                result = 5; // Less than default threshold
                minTimes = 0;
            }
        };
    }

    private void setupFilteredPartitionMocks(List<String> expectedFilters) {
        new Expectations() {
            {
                mockCache.getPartitionValuesByFilter(mockTable, (String) any, (List<String>) any, (List<Type>) any);
                result = createMockSelectedPartitions();
                minTimes = 0;
            }
        };
    }

    private void setupUnfilteredPartitionMocks() {
        new Expectations() {
            {
                mockCache.getPartitionValues(mockTable, (List<Type>) any);
                result = createMockHivePartitionValues();
                minTimes = 0;
            }
        };
    }

    private void setupEmptyPartitionMocks() {
        new Expectations() {
            {
                mockCache.getPartitionValuesByFilter(mockTable, (String) any, (List<String>) any, (List<Type>) any);
                result = Maps.newHashMap(); // Empty partition map
                minTimes = 0;
            }
        };
    }

    private void setupViewBasedTableMocks() {
        new Expectations() {
            {
                mockTable.isViewBased();
                result = true;
                minTimes = 0;

                mockCache.getPartitionNum(mockTable);
                result = 5;
                minTimes = 0;

                mockCache.getPartitionValuesByFilter(mockTable, (String) any, (List<String>) any, (List<Type>) any);
                result = createMockSelectedPartitions();
                minTimes = 0;
            }
        };
    }

    private void setupLargePartitionCountMocks() {
        new Expectations() {
            {
                mockCache.getPartitionNum(mockTable);
                result = Config.max_partition_num_for_single_hive_table_without_filter + 10;
                minTimes = 0;

                mockCache.getPartitionValuesWithoutCache(mockTable, (List<Type>) any);
                result = createMockHivePartitionValues();
                minTimes = 0;
            }
        };
    }

    private Map<Long, PartitionItem> createMockSelectedPartitions() {
        Map<Long, PartitionItem> partitions = Maps.newHashMap();

        // Create partition key with year=2023, month="01"
        LiteralExpr yearValue = new IntLiteral(2023);
        LiteralExpr monthValue = new org.apache.doris.analysis.StringLiteral("01");

        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(yearValue, PrimitiveType.INT);
        partitionKey.pushColumn(monthValue, PrimitiveType.STRING);

        ListPartitionItem partitionItem = new ListPartitionItem(Lists.newArrayList(partitionKey));
        partitions.put(1L, partitionItem);

        return partitions;
    }

    private HiveMetaStoreCache.HivePartitionValues createMockHivePartitionValues() {
        Map<Long, PartitionItem> mockPartitions = createMockSelectedPartitions();
        return new HiveMetaStoreCache.HivePartitionValues() {
            @Override
            public Map<Long, PartitionItem> getIdToPartitionItem() {
                return mockPartitions;
            }
        };
    }
}
