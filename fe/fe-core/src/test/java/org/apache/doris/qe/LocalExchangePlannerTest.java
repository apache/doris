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

package org.apache.doris.qe;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.AddLocalExchange;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class LocalExchangePlannerTest extends TestWithFeService {
    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE test.t1 (k1 INT, k2 INT, v1 INT) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 6 "
                + "PROPERTIES ('replication_num'='1')");
        createTable("CREATE TABLE test.t2 (k1 INT, k2 INT, v2 INT) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 6 "
                + "PROPERTIES ('replication_num'='1')");
    }

    @Test
    public void testAggFromScanUsesLocalExecutionHashShuffle() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(true);
        connectContext.getSessionVariable().setPipelineTaskNum("4");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        // With the Bug 7 fix, OlapScanNode returns NOOP (no self-wrapping),
        // so the parent AggregationNode's requireHash() resolves to
        // LOCAL_EXECUTION_HASH_SHUFFLE (scan child → local hash).
        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select k1, k2, count(*) from test.t1 group by k1, k2");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());
        String explain = collectFragmentExplain(planner.getFragments());

        Assertions.assertTrue(types.contains(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE),
                "expected LOCAL_EXECUTION_HASH_SHUFFLE in plan, actual: " + types);
        Assertions.assertTrue(explain.contains("LOCAL_EXECUTION_HASH_SHUFFLE"),
                "expected LOCAL_EXECUTION_HASH_SHUFFLE in explain output, actual explain: " + explain);
    }

    @Test
    public void testNonSerialScanKeepsBucketHashDistribution() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(false);
        connectContext.getSessionVariable().setPipelineTaskNum("1");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select k1, count(*) from test.t1 group by k1 order by k1");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        String explain = collectFragmentExplain(planner.getFragments());

        Assertions.assertFalse(explain.contains("LOCAL_EXECUTION_HASH_SHUFFLE"),
                "non-serial scan should keep BUCKET_HASH_SHUFFLE output and avoid local hash exchange, explain: "
                        + explain);
    }

    @Test
    public void testJoinPlanContainsGlobalExecutionHash() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(false);
        connectContext.getSessionVariable().setPipelineTaskNum("4");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select a.k1, count(*) "
                        + "from test.t1 a join test.t2 b on a.k1 = b.k1 group by a.k1");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());
        String explain = collectFragmentExplain(planner.getFragments());

        Assertions.assertTrue(types.contains(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE),
                "expected GLOBAL_EXECUTION_HASH_SHUFFLE in plan, actual: " + types);
        Assertions.assertTrue(explain.contains("GLOBAL_EXECUTION_HASH_SHUFFLE"),
                "expected GLOBAL_EXECUTION_HASH_SHUFFLE in explain output, actual explain: " + explain);
    }

    @Test
    public void testNoopLocalExchangeNotInjected() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql("explain distributed plan select * from test.t1 limit 1");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());

        Assertions.assertFalse(types.contains(LocalExchangeType.NOOP),
                "NOOP local exchange should not be materialized as a node");
    }

    @Test
    public void testHashShuffleHasDistributeExprs() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(false);
        connectContext.getSessionVariable().setPipelineTaskNum("4");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select a.k1, count(*) "
                        + "from test.t1 a join test.t2 b on a.k1 = b.k1 group by a.k1");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        List<LocalExchangeNode> localExchanges = collectLocalExchangeNodes(planner.getFragments());

        boolean hasHashShuffleWithExpr = localExchanges.stream().anyMatch(node -> node.getExchangeType().isHashShuffle()
                && node.getDistributeExprLists() != null
                && !node.getDistributeExprLists().isEmpty());
        Assertions.assertTrue(hasHashShuffleWithExpr,
                "expected at least one hash local exchange with distribute exprs");
    }

    @Test
    public void testRequireHashSatisfyAllHashShuffleTypes() {
        LocalExchangeNode.LocalExchangeTypeRequire requireHash = LocalExchangeNode.LocalExchangeTypeRequire.requireHash();
        Assertions.assertTrue(requireHash.satisfy(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE));
        Assertions.assertTrue(requireHash.satisfy(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE));
        Assertions.assertTrue(requireHash.satisfy(LocalExchangeType.BUCKET_HASH_SHUFFLE));
        Assertions.assertFalse(requireHash.satisfy(LocalExchangeType.PASSTHROUGH));
    }

    @Test
    public void testSetOperationUnderAggHasHashShuffle() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(false);
        connectContext.getSessionVariable().setPipelineTaskNum("4");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select k1, count(*) from ("
                        + "select k1 from test.t1 union all select k1 from test.t2"
                        + ") u group by k1");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());

        Assertions.assertTrue(types.contains(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE),
                "expected GLOBAL_EXECUTION_HASH_SHUFFLE in set-operation plan, actual: " + types);
    }

    @Test
    public void testAnalyticPlanContainsPassthroughAndLocalHashShuffle() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(true);
        connectContext.getSessionVariable().setPipelineTaskNum("4");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select k1, k2, row_number() over(partition by k1 order by k2) "
                        + "from test.t1 order by k1, k2");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());

        Assertions.assertTrue(types.contains(LocalExchangeType.PASSTHROUGH),
                "expected PASSTHROUGH in analytic plan, actual: " + types);
        Assertions.assertTrue(types.contains(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE),
                "expected LOCAL_EXECUTION_HASH_SHUFFLE in analytic plan, actual: " + types);
    }

    @Test
    public void testGroupingSetsPlanContainsHashShuffle() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(false);
        connectContext.getSessionVariable().setPipelineTaskNum("4");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select k1, k2, sum(v1) from test.t1 "
                        + "group by grouping sets((k1), (k1, k2))");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());

        Assertions.assertFalse(types.contains(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE),
                "grouping-sets plan should not force local execution hash shuffle when scan keeps bucket"
                        + " distribution, actual: " + types);
    }

    @Test
    public void testLocalAndGlobalExecutionHashShufflePreferType() {
        PlanTranslatorContext translatorContext = new PlanTranslatorContext();
        LocalExchangeNode.LocalExchangeTypeRequire requireHash = LocalExchangeNode.LocalExchangeTypeRequire.requireHash();
        LocalExchangeNode.LocalExchangeTypeRequire requireBucketHash
                = LocalExchangeNode.LocalExchangeTypeRequire.requireBucketHash();
        LocalExchangeNode.LocalExchangeTypeRequire requireGlobalHash
                = LocalExchangeNode.LocalExchangeTypeRequire.requireGlobalExecutionHash();

        LocalExchangeType localType = AddLocalExchange.resolveExchangeType(
                requireHash, translatorContext, null, new MockScanNode(new PlanNodeId(1001)));
        LocalExchangeType globalType = AddLocalExchange.resolveExchangeType(
                requireHash, translatorContext, null, new MockPlanNode(new PlanNodeId(1002)));
        // Explicit GLOBAL_EXECUTION_HASH_SHUFFLE must NOT be degraded, even on a scan path.
        // If it appears on a scan path, the plan is wrong — not something resolveExchangeType should fix.
        LocalExchangeType explicitGlobalOnScanType = AddLocalExchange.resolveExchangeType(
                requireGlobalHash, translatorContext, null, new MockScanNode(new PlanNodeId(1003)));

        Assertions.assertEquals(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE, localType);
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, globalType);
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, explicitGlobalOnScanType);
        Assertions.assertEquals(LocalExchangeType.BUCKET_HASH_SHUFFLE, requireBucketHash.preferType());
    }

    @Test
    public void testPlanContainsBothLocalAndGlobalExecutionHashShuffle() throws Exception {
        connectContext.getSessionVariable().setEnableLocalShufflePlanner(true);
        connectContext.getSessionVariable().setEnableLocalShuffle(true);
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setIgnoreStorageDataDistribution(false);
        connectContext.getSessionVariable().setPipelineTaskNum("4");
        connectContext.getSessionVariable().setForceToLocalShuffle(false);

        StmtExecutor executor = executeNereidsSql(
                "explain distributed plan select u.k1, count(*) from ("
                        + "select k1, k2 from test.t1 group by grouping sets((k1), (k1, k2))"
                        + ") u join test.t2 b on u.k1 = b.k1 group by u.k1");
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        EnumSet<LocalExchangeType> types = collectLocalExchangeTypes(planner.getFragments());
        String explain = collectFragmentExplain(planner.getFragments());

        Assertions.assertTrue(types.contains(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE),
                "expected GLOBAL_EXECUTION_HASH_SHUFFLE in mixed plan, actual: " + types);
        Assertions.assertTrue(explain.contains("GLOBAL_EXECUTION_HASH_SHUFFLE"),
                "expected GLOBAL_EXECUTION_HASH_SHUFFLE in explain output, actual explain: " + explain);
    }

    private EnumSet<LocalExchangeType> collectLocalExchangeTypes(List<PlanFragment> fragments) {
        EnumSet<LocalExchangeType> types = EnumSet.noneOf(LocalExchangeType.class);
        for (PlanFragment fragment : fragments) {
            collect(fragment.getPlanRoot(), types);
        }
        return types;
    }

    private List<LocalExchangeNode> collectLocalExchangeNodes(List<PlanFragment> fragments) {
        List<LocalExchangeNode> nodes = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            collectLocalExchangeNode(fragment.getPlanRoot(), nodes);
        }
        return nodes;
    }

    private String collectFragmentExplain(List<PlanFragment> fragments) {
        StringBuilder explain = new StringBuilder();
        for (PlanFragment fragment : fragments) {
            explain.append(fragment.getExplainString(TExplainLevel.NORMAL));
        }
        return explain.toString();
    }

    private void collect(PlanNode node, EnumSet<LocalExchangeType> types) {
        if (node instanceof LocalExchangeNode) {
            types.add(((LocalExchangeNode) node).getExchangeType());
        }
        for (PlanNode child : node.getChildren()) {
            collect(child, types);
        }
    }

    private void collectLocalExchangeNode(PlanNode node, List<LocalExchangeNode> nodes) {
        if (node instanceof LocalExchangeNode) {
            nodes.add((LocalExchangeNode) node);
        }
        for (PlanNode child : node.getChildren()) {
            collectLocalExchangeNode(child, nodes);
        }
    }

    private static class MockPlanNode extends PlanNode {
        MockPlanNode(PlanNodeId id) {
            super(id, "MOCK-PLAN");
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }

        @Override
        public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
            return "";
        }
    }

    private static class MockScanNode extends ScanNode {
        MockScanNode(PlanNodeId id) {
            super(id, new TupleDescriptor(new TupleId(id.asInt())), "MOCK-SCAN");
        }

        @Override
        protected void createScanRangeLocations() throws UserException {
        }

        @Override
        public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
            return java.util.Collections.emptyList();
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }

        @Override
        public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
            return "";
        }
    }
}
