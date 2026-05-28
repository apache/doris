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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lower;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.InternalQueryExecutionException;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Test for PruneHMSScanPartition rule, focusing on executePartitionFilterQuery
 * method coverage.
 */
class PruneHMSScanPartitionTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    /**
     * Test executePartitionFilterQuery method with successful execution
     */
    @Test
    void testExecutePartitionFilterQuerySuccess(@Injectable HMSExternalTable mockTable) throws Exception {
        List<ResultRow> mockResults = Lists.newArrayList();
        mockResults.add(new ResultRow(Lists.newArrayList("2023-01-01", "us")));
        mockResults.add(new ResultRow(Lists.newArrayList("2023-01-02", "eu")));

        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                return mockResults;
            }
        };

        List<ResultRow> result;
        try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
            result = PruneHMSScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
        }

        Assertions.assertNotNull(result, "Result should not be null");
        Assertions.assertEquals(2, result.size(), "Should return 2 partition rows");
        Assertions.assertEquals("2023-01-01", result.get(0).getValues().get(0),
                "First partition should have correct date");
        Assertions.assertEquals("us", result.get(0).getValues().get(1),
                "First partition should have correct region");
    }

    /**
     * Test executePartitionFilterQuery method exception handling
     */
    @Test
    void testExecutePartitionFilterQueryExceptionHandling(@Injectable HMSExternalTable mockTable) throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                throw new RuntimeException("Query execution failed");
            }
        };

        new Expectations(mockTable) {
            {
                mockTable.getDbName();
                result = "test_db";

                mockTable.getName();
                result = "test_table";
            }
        };
        InternalQueryExecutionException exception = Assertions.assertThrows(
                InternalQueryExecutionException.class,
                () -> {
                try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
                    PruneHMSScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
                }
            });

        String expectedMessage = "prune hive partitions failed for test_db.test_table";
        Assertions.assertEquals(expectedMessage, exception.getMessage(),
                "Exception message should match expected format");
    }

    /**
     * Test executePartitionFilterQuery with different exception types
     */
    @Test
    void testExecutePartitionFilterQueryDifferentExceptions(@Injectable HMSExternalTable mockTable) throws Exception {
        new Expectations(mockTable) {
            {
                mockTable.getDbName();
                result = "test_db";
                minTimes = 1;

                mockTable.getName();
                result = "test_table";
                minTimes = 1;
            }
        };

        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                throw new java.sql.SQLException("Database connection failed");
            }
        };

        InternalQueryExecutionException sqlException = Assertions.assertThrows(
                InternalQueryExecutionException.class,
                () -> {
                try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
                    PruneHMSScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
                }
            });

        Assertions.assertTrue(sqlException.getMessage().contains("prune hive partitions failed for"),
                "SQLException should be wrapped with standard error message");

        // Test with TimeoutException
        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                throw new java.util.concurrent.TimeoutException("Query timeout");
            }
        };

        InternalQueryExecutionException timeoutException = Assertions.assertThrows(
                InternalQueryExecutionException.class,
                () -> {
                try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
                    PruneHMSScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
                }
            });

        Assertions.assertTrue(timeoutException.getMessage().contains("prune hive partitions failed for"),
                "TimeoutException should be wrapped with standard error message");
    }

    /**
     * Test executePartitionFilterQuery with empty result
     */
    @Test
    void testExecutePartitionFilterQueryEmptyResult(@Injectable HMSExternalTable mockTable) throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() throws Exception {
                return Lists.newArrayList(); // Empty result
            }
        };

        List<ResultRow> result;
        try (AutoCloseConnectContext context = new AutoCloseConnectContext(connectContext)) {
            result = PruneHMSScanPartition.executePartitionFilterQuery(mockTable, context, "SELECT * FROM test");
        }

        Assertions.assertNotNull(result, "Result should not be null even when empty");
        Assertions.assertEquals(0, result.size(), "Should return empty list when no partitions match");
    }

    @Test
    void testGetConjunctsWithoutPartitionPredicate(@Injectable LogicalFileScan logicalFileScan,
                                                   @Injectable HMSExternalTable table,
                                                   @Injectable LogicalFilter<LogicalFileScan> logicalFilter) {
        Slot slot1 = new SlotReference("col1", IntegerType.INSTANCE);
        Slot slot2 = new SlotReference("col2", IntegerType.INSTANCE);
        Slot slot3 = new SlotReference("col3", StringType.INSTANCE);
        Slot slot4 = new SlotReference("col4", StringType.INSTANCE);

        Column col1 = new Column("col1", PrimitiveType.INT);
        Column col2 = new Column("col2", PrimitiveType.INT);

        Expression expression1 = new EqualTo(slot1, new IntegerLiteral(1));
        Expression expression2 = new LessThan(slot2, new IntegerLiteral(3));
        Expression expression3 = new EqualTo(slot3, new StringLiteral("abc"));

        new Expectations() {
            {
                logicalFileScan.getTable();
                result = table;
                minTimes = 1;

                logicalFileScan.getOutput();
                result = Lists.newArrayList(slot1, slot2, slot3, slot4);
                minTimes = 1;

                table.getPartitionColumns();
                result = Lists.newArrayList(col1, col2);
                minTimes = 1;

                logicalFilter.getConjuncts();
                result = Sets.newHashSet(expression1, expression2, expression3);
                minTimes = 1;
            }
        };
        PruneHMSScanPartition pruneHMSScanPartition = new PruneHMSScanPartition();
        Set<Expression> conjuncts = pruneHMSScanPartition.getConjunctsWithoutPartitionPredicate(logicalFileScan,
                logicalFilter);
        Assertions.assertEquals(1, conjuncts.size());
        Assertions.assertEquals(expression3, conjuncts.toArray()[0]);
    }

    @Test
    void testPruneFilePartitionWithHudi(@Injectable HMSExternalTable table) {
        Slot slot1 = new SlotReference("col1", IntegerType.INSTANCE);
        Slot slot2 = new SlotReference("col2", IntegerType.INSTANCE);
        Slot slot3 = new SlotReference("col3", StringType.INSTANCE);

        Expression expression1 = new EqualTo(slot1, new IntegerLiteral(1));
        Expression expression2 = new LessThan(slot2, new IntegerLiteral(3));
        Expression expression3 = new EqualTo(slot3, new StringLiteral("abc"));

        final SelectedPartitions selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
        Set<Expression> conjuncts = Sets.newHashSet(expression1, expression2, expression3);
        LogicalFileScan logicalFileScan = new LogicalFileScan(new RelationId(1), table,
                Lists.newArrayList("test", "test"), Collections.emptyList(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
        new Expectations() {
            {
                table.getName();
                result = "test";
                minTimes = 1;

                table.supportInternalPartitionPruned();
                result = true;
                minTimes = 1;
            }
        };
        new MockUp<PruneHMSScanPartition>() {
            @Mock
            SelectedPartitions pruneHMSPartitions(HMSExternalTable hiveTbl,
                                                           LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan, CascadesContext ctx) {
                return selectedPartitions;
            }

            @Mock
            Set<Expression> getConjunctsWithoutPartitionPredicate(LogicalFileScan fileScan,
                                                                  LogicalFilter<LogicalFileScan> filter) {
                return Sets.newHashSet();
            }
        };
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(conjuncts, logicalFileScan);
        List<Plan> planList = new PruneHMSScanPartition().build().transform(filter,
                MemoTestUtils.createCascadesContext(filter));
        Assertions.assertEquals(1, planList.size());
        Assertions.assertTrue(planList.get(0) instanceof LogicalFileScan);
        Assertions.assertEquals(selectedPartitions, ((LogicalFileScan) planList.get(0)).getSelectedPartitions());
    }

    @Test
    void testPruneFilePartitionWithHive(@Injectable HMSExternalTable table) {
        Slot slot1 = new SlotReference("col1", IntegerType.INSTANCE);
        Slot slot2 = new SlotReference("col2", IntegerType.INSTANCE);
        Slot slot3 = new SlotReference("col3", StringType.INSTANCE);

        Expression expression1 = new EqualTo(slot1, new IntegerLiteral(1));
        Expression expression2 = new LessThan(slot2, new IntegerLiteral(3));
        Expression expression3 = new EqualTo(slot3, new StringLiteral("abc"));

        final SelectedPartitions selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
        Set<Expression> conjuncts = Sets.newHashSet(expression1, expression2, expression3);
        LogicalFileScan logicalFileScan = new LogicalFileScan(new RelationId(1), table,
                Lists.newArrayList("test", "test"), Collections.emptyList(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
        new Expectations() {
            {
                table.getName();
                result = "test";
                minTimes = 1;

                table.supportInternalPartitionPruned();
                result = true;
                minTimes = 1;
            }
        };
        new MockUp<PruneHMSScanPartition>() {
            @Mock
            SelectedPartitions pruneHMSPartitions(HMSExternalTable hiveTbl,
                                                          LogicalFilter<LogicalFileScan> filter,
                                                          LogicalFileScan scan,
                                                          CascadesContext ctx) {
                return selectedPartitions;
            }

            @Mock
            Set<Expression> getConjunctsWithoutPartitionPredicate(LogicalFileScan fileScan,
                                                                  LogicalFilter<LogicalFileScan> filter) {
                return Sets.newHashSet();
            }
        };
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(conjuncts, logicalFileScan);
        List<Plan> planList = new PruneHMSScanPartition().build().transform(filter,
                MemoTestUtils.createCascadesContext(filter));
        Assertions.assertEquals(1, planList.size());
        Assertions.assertInstanceOf(LogicalFileScan.class, planList.get(0));
    }

    @Test
    void testGetSelectedPartitionItems(@Injectable HMSExternalTable table, @Injectable HiveMetaStoreCache cache,
                                       @Injectable HiveMetaStoreCache.HivePartitionValues hivePartitionValues1,
                                       @Injectable HiveMetaStoreCache.HivePartitionValues hivePartitionValues2) {
        Map<Long, PartitionItem> mockPartitionItemMap = new HashMap<>();
        mockPartitionItemMap.put(1L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap.put(2L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap.put(3L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));

        Map<Long, PartitionItem> mockPartitionItemMap2 = new HashMap<>();
        mockPartitionItemMap2.put(1L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));

        Map<Long, PartitionItem> mockPartitionItemMap3 = new HashMap<>();
        mockPartitionItemMap3.put(1L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap3.put(2L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap3.put(3L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap3.put(4L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));

        new Expectations() {
            {
                table.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.VARCHAR);
                minTimes = 1;

                cache.getPartitionValuesByFilter(table, anyString,
                        Lists.newArrayList("dt"), Lists.newArrayList(Type.VARCHAR));
                result = mockPartitionItemMap;
                minTimes = 1;

                // getPartitionItems is called twice: partitionNum=1 and partitionNum=4,
                // both go through cache.getPartitionValues since partitionNum < 3000.
                // Second call returns hivePartitionValues1 (1 entry),
                // third call returns hivePartitionValues2 (4 entries).
                cache.getPartitionValues(table, Lists.newArrayList(Type.VARCHAR));
                result = hivePartitionValues1;
                result = hivePartitionValues2;

                hivePartitionValues1.getIdToPartitionItem();
                result = mockPartitionItemMap2;
                minTimes = 1;

                BiMap<String, Long> nameToIdMap1 = HashBiMap.create();
                nameToIdMap1.put("p1", 1L);
                hivePartitionValues1.getPartitionNameToIdMap();
                result = nameToIdMap1;
                minTimes = 1;

                hivePartitionValues2.getIdToPartitionItem();
                result = mockPartitionItemMap3;
                minTimes = 1;

                BiMap<String, Long> nameToIdMap2 = HashBiMap.create();
                nameToIdMap2.put("p1", 1L);
                nameToIdMap2.put("p2", 2L);
                nameToIdMap2.put("p3", 3L);
                nameToIdMap2.put("p4", 4L);
                hivePartitionValues2.getPartitionNameToIdMap();
                result = nameToIdMap2;
                minTimes = 1;

            }
        };
        PruneHMSScanPartition pruneHMSScanPartition = new PruneHMSScanPartition();
        Map<String, PartitionItem> partitionItemMap = pruneHMSScanPartition.getSelectedPartitionItems(table,
                Lists.newArrayList("dt"), BooleanLiteral.TRUE, 2000, cache);
        Assertions.assertEquals(3, partitionItemMap.size());

        partitionItemMap = pruneHMSScanPartition.getSelectedPartitionItems(table,
            Lists.newArrayList("dt"),
            new EqualTo(new Lower(new SlotReference("dt", VarcharType.SYSTEM_DEFAULT)),
                    new StringLiteral("2025-10-10")), 1, cache);
        Assertions.assertEquals(1, partitionItemMap.size());
        partitionItemMap = pruneHMSScanPartition.getSelectedPartitionItems(table, Lists.newArrayList("dt"),
                new EqualTo(new Lower(new SlotReference("dt", VarcharType.SYSTEM_DEFAULT)),
                    new StringLiteral("2025-10-10")), 4, cache);
        Assertions.assertEquals(4, partitionItemMap.size());
    }

    @Test
    void testGetSelectedPartitionItemsFromView(@Injectable HMSExternalTable table, @Injectable HiveMetaStoreCache cache,
                                               @Injectable HiveMetaStoreCache.HivePartitionValues hivePartitionValues1,
                                               @Injectable HiveMetaStoreCache.HivePartitionValues hivePartitionValues2) {
        Map<Long, PartitionItem> mockPartitionItemMap = new HashMap<>();
        mockPartitionItemMap.put(1L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap.put(2L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap.put(3L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));

        Map<Long, PartitionItem> mockPartitionItemMap2 = new HashMap<>();
        mockPartitionItemMap2.put(1L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));

        Map<Long, PartitionItem> mockPartitionItemMap3 = new HashMap<>();
        mockPartitionItemMap3.put(1L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap3.put(2L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap3.put(3L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));
        mockPartitionItemMap3.put(4L, new ListPartitionItem(Lists.newArrayList(new PartitionKey())));

        new Expectations() {
            {
                table.getPartitionColumnTypes();
                result = Lists.newArrayList(Type.VARCHAR);
                minTimes = 1;

                cache.getPartitionValuesByFilter(table, anyString, Lists.newArrayList("dt"),
                        Lists.newArrayList(Type.VARCHAR));
                result = mockPartitionItemMap;
                minTimes = 1;

                // getPartitionItems is called twice: partitionNum=1 and partitionNum=4,
                // both go through cache.getPartitionValues since partitionNum < 3000.
                // Second call returns hivePartitionValues1 (1 entry),
                // third call returns hivePartitionValues2 (4 entries).
                cache.getPartitionValues(table, Lists.newArrayList(Type.VARCHAR));
                result = hivePartitionValues1;
                result = hivePartitionValues2;

                hivePartitionValues1.getIdToPartitionItem();
                result = mockPartitionItemMap2;
                minTimes = 1;

                BiMap<String, Long> viewNameToIdMap1 = HashBiMap.create();
                viewNameToIdMap1.put("p1", 1L);
                hivePartitionValues1.getPartitionNameToIdMap();
                result = viewNameToIdMap1;
                minTimes = 1;

                hivePartitionValues2.getIdToPartitionItem();
                result = mockPartitionItemMap3;
                minTimes = 1;

                BiMap<String, Long> viewNameToIdMap2 = HashBiMap.create();
                viewNameToIdMap2.put("p1", 1L);
                viewNameToIdMap2.put("p2", 2L);
                viewNameToIdMap2.put("p3", 3L);
                viewNameToIdMap2.put("p4", 4L);
                hivePartitionValues2.getPartitionNameToIdMap();
                result = viewNameToIdMap2;
                minTimes = 1;

            }
        };
        PruneHMSScanPartition pruneHMSScanPartition = new PruneHMSScanPartition();
        Map<String, PartitionItem> partitionItemMap = pruneHMSScanPartition.getSelectedPartitionItems(table,
                Lists.newArrayList("dt"), BooleanLiteral.TRUE, 2000, cache);
        Assertions.assertEquals(3, partitionItemMap.size());

        partitionItemMap = pruneHMSScanPartition.getSelectedPartitionItems(table, Lists.newArrayList("dt"),
            new EqualTo(new Lower(new SlotReference("dt", VarcharType.SYSTEM_DEFAULT)),
                    new StringLiteral("2025-10-10")), 1, cache);
        Assertions.assertEquals(1, partitionItemMap.size());
        partitionItemMap = pruneHMSScanPartition.getSelectedPartitionItems(table, Lists.newArrayList("dt"),
            new EqualTo(new Lower(new SlotReference("dt", VarcharType.SYSTEM_DEFAULT)),
                    new StringLiteral("2025-10-10")), 4, cache);
        Assertions.assertEquals(4, partitionItemMap.size());
    }

    /**
     * Test applyExactPartitionPruneOnCandidates returns the subset of partitions that
     * actually satisfy the exact partition predicate. The HMS prefilter intentionally
     * over-approximates with range predicates, so this helper is required to drop the
     * false positives before scanning.
     */
    @Test
    void testApplyExactPartitionPruneOnCandidates() throws Exception {
        Slot dt = new SlotReference("dt", VarcharType.SYSTEM_DEFAULT);
        Expression exactPartitionPredicate = new InPredicate(dt, Lists.newArrayList(
                new StringLiteral("2023-01-01"), new StringLiteral("2023-01-03")));

        Map<String, PartitionItem> candidatePartitionItems = new HashMap<>();
        candidatePartitionItems.put("p1", createListPartitionItem("2023-01-01"));
        candidatePartitionItems.put("p2", createListPartitionItem("2023-01-02"));
        candidatePartitionItems.put("p3", createListPartitionItem("2023-01-03"));

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), Lists.newArrayList("test_table")));
        Map<String, PartitionItem> selectedPartitionItems = new PruneHMSScanPartition()
                .applyExactPartitionPruneOnCandidates(Lists.newArrayList(dt), exactPartitionPredicate,
                        candidatePartitionItems, cascadesContext);

        Assertions.assertNotNull(selectedPartitionItems);
        Assertions.assertEquals(2, selectedPartitionItems.size());
        Assertions.assertTrue(selectedPartitionItems.containsKey("p1"));
        Assertions.assertTrue(selectedPartitionItems.containsKey("p3"));
        Assertions.assertFalse(selectedPartitionItems.containsKey("p2"));
    }

    /**
     * Test applyExactPartitionPruneOnCandidates returns null when PartitionPruner.tryPrune
     * cannot determine the result (e.g. predicate involves a non-partition expression).
     * Callers must treat null as "could not prune exactly" and keep the HMS-prefiltered set.
     */
    @Test
    void testApplyExactPartitionPruneOnCandidatesFailure() throws Exception {
        Slot dt = new SlotReference("dt", VarcharType.SYSTEM_DEFAULT);
        Expression exactPartitionPredicate = new EqualTo(new Lower(dt), new StringLiteral("2023-01-01"));
        Map<String, PartitionItem> candidatePartitionItems = new HashMap<>();
        candidatePartitionItems.put("p1", createListPartitionItem("2023-01-01"));

        new MockUp<PartitionPruner>() {
            @Mock
            public boolean tryPrune(List<Slot> partitionSlots, Expression predicate,
                    Map<String, PartitionItem> candidateItems, List<String> prunedPartitions,
                    CascadesContext ctx) {
                return false;
            }
        };

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), Lists.newArrayList("test_table")));
        Map<String, PartitionItem> selectedPartitionItems = new PruneHMSScanPartition()
                .applyExactPartitionPruneOnCandidates(Lists.newArrayList(dt), exactPartitionPredicate,
                        candidatePartitionItems, cascadesContext);

        Assertions.assertNull(selectedPartitionItems);
    }

    /**
     * Test partitionPredicateToSql outputs both predicates when IN rewrite happened,
     * and only the exact predicate otherwise.
     */
    @Test
    void testPartitionPredicateToSql() throws Exception {
        Method method = PruneHMSScanPartition.class.getDeclaredMethod("partitionPredicateToSql",
                Expression.class, Expression.class, boolean.class);
        method.setAccessible(true);
        PruneHMSScanPartition pruneHMSScanPartition = new PruneHMSScanPartition();
        Expression exactPredicate = new EqualTo(new SlotReference("dt", VarcharType.SYSTEM_DEFAULT),
                new StringLiteral("2023-01-01"));
        Expression hmsPredicate = new EqualTo(new SlotReference("dt", VarcharType.SYSTEM_DEFAULT),
                new StringLiteral("2023-01-01"));

        // without IN rewrite, only output exactPredicate
        Assertions.assertEquals(exactPredicate.toSql(),
                method.invoke(pruneHMSScanPartition, hmsPredicate, exactPredicate, false));

        // with IN rewrite, output both hmsPredicate and exactPredicate
        Assertions.assertEquals("hmsPredicate: " + hmsPredicate.toSql()
                        + ", exactPredicate: " + exactPredicate.toSql(),
                method.invoke(pruneHMSScanPartition, hmsPredicate, exactPredicate, true));

        // hmsPredicate is null, only output exactPredicate
        Assertions.assertEquals(exactPredicate.toSql(),
                method.invoke(pruneHMSScanPartition, null, exactPredicate, false));
    }

    private ListPartitionItem createListPartitionItem(String value) throws AnalysisException {
        PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(
                Lists.newArrayList(new PartitionValue(value)),
                Lists.newArrayList(Type.VARCHAR), true);
        return new ListPartitionItem(Lists.newArrayList(partitionKey));
    }
}
