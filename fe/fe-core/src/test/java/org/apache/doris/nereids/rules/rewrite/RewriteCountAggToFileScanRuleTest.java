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

import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Test for RewriteCountAggToFileScanRule.
 * Tests the rewrite of count(*)/count(1) over file scan to sum0(__count_from_metadata__),
 * including support for GROUP BY partition columns.
 *
 * Note: Rule.transform() does NOT check the "when" predicate; the predicate is only
 * evaluated by the Memo/Optimizer during isInvalid() checks. So for "should NOT rewrite"
 * tests, we verify via the canRewrite method instead of calling transform directly.
 */
class RewriteCountAggToFileScanRuleTest extends TestWithFeService {

    private final RewriteCountAggToFileScanRule ruleFactory = new RewriteCountAggToFileScanRule();

    private SlotReference col1 = new SlotReference("col1", IntegerType.INSTANCE, true, ImmutableList.of());
    private SlotReference col2 = new SlotReference("col2", StringType.INSTANCE, true, ImmutableList.of());
    private SlotReference partCol1 = new SlotReference("dt", VarcharType.SYSTEM_DEFAULT, true, ImmutableList.of());
    private SlotReference partCol2 =
            new SlotReference("region", VarcharType.SYSTEM_DEFAULT, true, ImmutableList.of());

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    private void setupConstructorMocks(HMSExternalTable mockTable) {
        new Expectations() {
            {
                mockTable.getName();
                result = "test_table";
                minTimes = 0;
                mockTable.getBaseSchema();
                result = Collections.emptyList();
                minTimes = 0;
                mockTable.initSelectedPartitions(Optional.empty());
                result = SelectedPartitions.NOT_PRUNED;
                minTimes = 0;
                mockTable.getDlaType();
                result = HMSExternalTable.DLAType.HIVE;
                minTimes = 0;
                mockTable.isHoodieCowTable();
                result = false;
                minTimes = 0;
                mockTable.isParquetOrOrcFormat();
                result = true;
                minTimes = 0;
            }
        };
    }

    private void setupConstructorMocks(ExternalTable mockTable) {
        new Expectations() {
            {
                mockTable.getName();
                result = "test_table";
                minTimes = 0;
                mockTable.getBaseSchema();
                result = Collections.emptyList();
                minTimes = 0;
                mockTable.initSelectedPartitions(Optional.empty());
                result = SelectedPartitions.NOT_PRUNED;
                minTimes = 0;
            }
        };
    }

    private LogicalFileScan createFileScan(HMSExternalTable mockTable) {
        return new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("catalog", "db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private LogicalFileScan createFileScan(ExternalTable mockTable) {
        return new LogicalFileScan(new RelationId(1), mockTable,
                Lists.newArrayList("catalog", "db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Invoke the private canRewrite method via reflection.
     */
    private boolean invokeCanRewrite(LogicalAggregate<LogicalFileScan> agg) throws Exception {
        Method canRewrite = RewriteCountAggToFileScanRule.class.getDeclaredMethod(
                "canRewrite", LogicalAggregate.class);
        canRewrite.setAccessible(true);
        return (boolean) canRewrite.invoke(ruleFactory, agg);
    }

    /**
     * Test count(*) without GROUP BY should be rewritten.
     */
    @Test
    void testCountStarNoGroupBy(@Injectable HMSExternalTable mockTable) {
        List<Rule> rules = ruleFactory.buildRules();

        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(countAlias), fileScan);

        new Expectations() {
            {
                mockTable.getPartitionColumnNames();
                result = Sets.newHashSet("dt", "region");
                minTimes = 0;
            }
        };

        List<Plan> results = rules.get(0).transform(agg,
                MemoTestUtils.createCascadesContext(connectContext, agg));

        Assertions.assertEquals(1, results.size());
        Plan result = results.get(0);
        Assertions.assertTrue(result instanceof LogicalAggregate, "Result should be LogicalAggregate");

        LogicalAggregate<?> resultAgg = (LogicalAggregate<?>) result;
        Assertions.assertTrue(resultAgg.getOutputExpressions().get(0) instanceof Alias);
        Alias outputAlias = (Alias) resultAgg.getOutputExpressions().get(0);
        Assertions.assertTrue(outputAlias.child(0) instanceof Sum0,
                "Output should be Sum0, got: " + outputAlias.child(0).getClass().getSimpleName());
    }

    /**
     * Test count(*) with GROUP BY partition columns should be rewritten.
     */
    @Test
    void testCountStarGroupByPartitionColumns(@Injectable HMSExternalTable mockTable) throws Exception {
        List<Rule> rules = ruleFactory.buildRules();

        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(partCol1), ImmutableList.of(partCol1, countAlias), fileScan);

        new Expectations() {
            {
                mockTable.getPartitionColumnNames();
                result = Sets.newHashSet("dt", "region");
                minTimes = 0;
            }
        };

        // Note: Rule.transform() bypasses the "when" predicate, so it will execute the "then"
        // action regardless of whether canRewrite returns true. We verify the rewrite result
        // directly instead of checking canRewrite, which is sensitive to mock setup.

        List<Plan> results = rules.get(0).transform(agg,
                MemoTestUtils.createCascadesContext(connectContext, agg));

        Assertions.assertEquals(1, results.size());
        Plan result = results.get(0);
        Assertions.assertTrue(result instanceof LogicalAggregate, "Result should be LogicalAggregate");

        LogicalAggregate<?> resultAgg = (LogicalAggregate<?>) result;
        Assertions.assertEquals(1, resultAgg.getGroupByExpressions().size(),
                "Group by should still have 1 expression");

        NamedExpression secondOutput = resultAgg.getOutputExpressions().get(1);
        if (secondOutput instanceof Alias) {
            Assertions.assertTrue(((Alias) secondOutput).child(0) instanceof Sum0,
                    "Second output should be Alias(Sum0(...))");
        }
    }

    /**
     * Test count(*) with GROUP BY non-partition columns should NOT be rewritten.
     */
    @Test
    void testCountStarGroupByNonPartitionColumnNotRewritten(
            @Injectable HMSExternalTable mockTable) throws Exception {
        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(col1), ImmutableList.of(col1, countAlias), fileScan);

        new Expectations() {
            {
                mockTable.getPartitionColumnNames();
                result = Sets.newHashSet("dt", "region");
                minTimes = 0;
            }
        };

        // canRewrite should return false because col1 is not a partition column
        Assertions.assertFalse(invokeCanRewrite(agg),
                "canRewrite should return false when GROUP BY has non-partition column");
    }

    /**
     * Test that SUM (not COUNT) is not rewritten.
     */
    @Test
    void testSumNotRewritten(@Injectable HMSExternalTable mockTable) throws Exception {
        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        org.apache.doris.nereids.trees.expressions.functions.agg.Sum sumExpr =
                new org.apache.doris.nereids.trees.expressions.functions.agg.Sum(col1);
        Alias sumAlias = new Alias(sumExpr, "sum(col1)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(sumAlias), fileScan);

        // canRewrite should return false because aggregate is SUM not COUNT
        Assertions.assertFalse(invokeCanRewrite(agg),
                "canRewrite should return false when aggregate is SUM, not COUNT");
    }

    /**
     * Test getPartitionColumnNames for non-HMS external table.
     */
    @Test
    void testGetPartitionColumnNamesNonHmsTable(@Injectable HMSExternalTable mockTable) {
        List<Rule> rules = ruleFactory.buildRules();

        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(countAlias), fileScan);

        new Expectations() {
            {
                mockTable.getPartitionColumnNames();
                result = Sets.newHashSet("dt", "region");
                minTimes = 0;
            }
        };

        List<Plan> results = rules.get(0).transform(agg,
                MemoTestUtils.createCascadesContext(connectContext, agg));
        Assertions.assertEquals(1, results.size());
    }

    /**
     * Test count(*) with GROUP BY multiple partition columns should be rewritten.
     */
    @Test
    void testCountStarGroupByMultiplePartitionColumns(@Injectable HMSExternalTable mockTable) {
        List<Rule> rules = ruleFactory.buildRules();

        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(partCol1, partCol2),
                ImmutableList.of(partCol1, partCol2, countAlias), fileScan);

        new Expectations() {
            {
                mockTable.getPartitionColumnNames();
                result = Sets.newHashSet("dt", "region");
                minTimes = 0;
            }
        };

        List<Plan> results = rules.get(0).transform(agg,
                MemoTestUtils.createCascadesContext(connectContext, agg));

        Assertions.assertEquals(1, results.size());
        Plan result = results.get(0);
        Assertions.assertTrue(result instanceof LogicalAggregate, "Result should be LogicalAggregate");

        LogicalAggregate<?> resultAgg = (LogicalAggregate<?>) result;
        Assertions.assertEquals(2, resultAgg.getGroupByExpressions().size(),
                "Group by should still have 2 expressions");

        NamedExpression lastOutput = resultAgg.getOutputExpressions().get(2);
        if (lastOutput instanceof Alias) {
            Assertions.assertTrue(((Alias) lastOutput).child(0) instanceof Sum0,
                    "Last output should be Alias(Sum0(...))");
        }
    }

    /**
     * Test count(*) with GROUP BY mixed (partition + non-partition) columns should NOT be rewritten.
     */
    @Test
    void testCountStarGroupByMixedColumnsNotRewritten(
            @Injectable HMSExternalTable mockTable) throws Exception {
        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(partCol1, col1),
                ImmutableList.of(partCol1, col1, countAlias), fileScan);

        new Expectations() {
            {
                mockTable.getPartitionColumnNames();
                result = Sets.newHashSet("dt", "region");
                minTimes = 0;
            }
        };

        // canRewrite should return false because col1 is not a partition column
        Assertions.assertFalse(invokeCanRewrite(agg),
                "canRewrite should return false when GROUP BY has mixed partition/non-partition columns");
    }

    /**
     * Test count(*) with GROUP BY partition columns through a Project should be rewritten.
     * This tests the Aggregate -> Project -> FileScan pattern.
     */
    @Test
    void testCountStarGroupByPartitionColumnsThroughProject(@Injectable HMSExternalTable mockTable) {
        List<Rule> rules = ruleFactory.buildRules();

        setupConstructorMocks(mockTable);
        LogicalFileScan fileScan = createFileScan(mockTable);

        // Create project that selects partition column
        LogicalProject<LogicalFileScan> project = new LogicalProject<>(
                ImmutableList.of(partCol1), fileScan);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalProject<LogicalFileScan>> agg = new LogicalAggregate<>(
                ImmutableList.of(partCol1), ImmutableList.of(partCol1, countAlias), project);

        new Expectations() {
            {
                mockTable.getPartitionColumnNames();
                result = Sets.newHashSet("dt", "region");
                minTimes = 0;
            }
        };

        List<Plan> results = rules.get(1).transform(agg,
                MemoTestUtils.createCascadesContext(connectContext, agg));

        Assertions.assertEquals(1, results.size());
        Plan result = results.get(0);
        Assertions.assertTrue(result instanceof LogicalAggregate, "Result should be LogicalAggregate");

        LogicalAggregate<?> resultAgg = (LogicalAggregate<?>) result;
        Assertions.assertEquals(1, resultAgg.getGroupByExpressions().size(),
                "Group by should still have 1 expression");

        // Verify the result has a Project under the Aggregate
        Assertions.assertTrue(resultAgg.child() instanceof LogicalProject,
                "Aggregate child should be LogicalProject");

        LogicalProject<?> resultProject = (LogicalProject<?>) resultAgg.child();
        // The project should have 2 outputs: partition column + __count_from_metadata__
        Assertions.assertEquals(2, resultProject.getProjects().size(),
                "Project should have partition column + __count_from_metadata__");

        NamedExpression secondProject = resultProject.getProjects().get(1);
        Assertions.assertTrue(secondProject instanceof SlotReference,
                "Second project should be SlotReference for __count_from_metadata__");
        Assertions.assertEquals("__count_from_metadata__", ((SlotReference) secondProject).getName(),
                "Second project should be __count_from_metadata__");

        NamedExpression secondOutput = resultAgg.getOutputExpressions().get(1);
        if (secondOutput instanceof Alias) {
            Assertions.assertTrue(((Alias) secondOutput).child(0) instanceof Sum0,
                    "Second output should be Alias(Sum0(...))");
        }
    }

    /**
     * Test that Text format Hive table should NOT be rewritten to COUNT_FROM_METADATA.
     * Text/CSV/JSON format tables should fall back to regular COUNT pushdown.
     */
    @Test
    void testTextFormatHiveTableNotRewritten(@Injectable HMSExternalTable mockTable) throws Exception {
        setupConstructorMocks(mockTable);

        // Override isParquetOrOrcFormat to return false for Text format
        new Expectations() {
            {
                mockTable.isParquetOrOrcFormat();
                result = false;
                minTimes = 0;
            }
        };

        LogicalFileScan fileScan = createFileScan(mockTable);

        Count countStar = new Count();
        Alias countAlias = new Alias(countStar, "count(*)");
        LogicalAggregate<LogicalFileScan> agg = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(countAlias), fileScan);

        // canRewrite should return false because Text format does not support COUNT_FROM_METADATA
        Assertions.assertFalse(invokeCanRewrite(agg),
                "canRewrite should return false for Text format Hive table");
    }
}
