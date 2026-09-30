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

package org.apache.doris.planner;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.planner.normalize.QueryCacheNormalizer;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class ConditionCacheEligibilityTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("cache_eligibility");
        useDatabase("cache_eligibility");
        createTable("create table t(k int, a array<int>, dt1 datetime, dt2 datetime) "
                + "duplicate key(k) distributed by hash(k) buckets 3 properties('replication_num'='1')");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setEnableQueryCache(true);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 2;
        connectContext.getSessionVariable().enableConditionCache = true;
    }

    @Test
    void testBothCachesRejectUnstableFilters() throws Exception {
        for (String predicate : ImmutableList.of(
                "k < 0 or rand(1) < 0.0001",
                "k < 0 or rand() < 0.0001",
                "concat(cast(k as string), uuid()) = 'x'",
                "array_shuffle(a) = [1,2,3]",
                "array_shuffle(a, 1) = [1,2,3]",
                "shuffle(a, 1) = [1,2,3]",
                "cast(timediff(dt1, dt2) as date) = date '2026-10-01'",
                "cast(timediff(dt1, dt2) as datetime) = cast(dt1 as datetime)")) {
            Planner planner = getSqlStmtExecutor("select k, sum(k) from t where " + predicate + " group by k")
                    .planner();
            List<TPlanNode> scans = scans(planner);
            Assertions.assertEquals(1, scans.size(), predicate);
            Assertions.assertFalse(scans.get(0).isEnableConditionCache(), predicate);
            for (PlanFragment fragment : planner.getFragments()) {
                Assertions.assertFalse(new QueryCacheNormalizer(fragment, planner.getDescTable())
                        .normalize(connectContext).isPresent(), predicate);
            }
        }
    }

    @Test
    void testDeterministicFiltersAndRandomOutput() throws Exception {
        Planner deterministic = getSqlStmtExecutor("select k, sum(k) from t where k > 100 group by k").planner();
        Assertions.assertTrue(scans(deterministic).get(0).isEnableConditionCache());
        Assertions.assertTrue(deterministic.getFragments().stream().anyMatch(fragment ->
                new QueryCacheNormalizer(fragment, deterministic.getDescTable()).normalize(connectContext).isPresent()));
        Planner randomOutput = getSqlStmtExecutor("select rand(), k from t where k > 100").planner();
        Assertions.assertTrue(scans(randomOutput).get(0).isEnableConditionCache());
    }

    @Test
    void testScanEligibilityIsIndependentAcrossUnionBranches() throws Exception {
        Planner planner = getSqlStmtExecutor("select k from t where k < 0 or rand(1) < 0.0001 "
                + "union all select k from t where k > 100").planner();
        List<TPlanNode> scans = scans(planner);
        Assertions.assertEquals(2, scans.size());
        Assertions.assertEquals(1, scans.stream().filter(TPlanNode::isEnableConditionCache).count());
    }

    @Test
    void testSqlCacheRejectsClockDependentCastsAndShuffle() throws Exception {
        boolean enabled = connectContext.getSessionVariable().isEnableSqlCache();
        connectContext.getSessionVariable().setEnableSqlCache(true);
        try {
            for (String expression : ImmutableList.of("k + 1", "year(now())")) {
                NereidsPlanner planner = (NereidsPlanner) getSqlStmtExecutor("select " + expression + " from t")
                        .planner();
                Assertions.assertTrue(planner.getStatementContext().getSqlCacheContext().orElseThrow()
                        .supportSqlCache(), expression);
            }
            for (String expression : ImmutableList.of("cast(timediff(dt1, dt2) as date)",
                    "cast(timediff(dt1, dt2) as datetime)", "array_shuffle(a)", "shuffle(a, 1)")) {
                NereidsPlanner planner = (NereidsPlanner) getSqlStmtExecutor("select " + expression + " from t")
                        .planner();
                SqlCacheContext cache = planner.getStatementContext().getSqlCacheContext().orElseThrow();
                Assertions.assertFalse(cache.supportSqlCache(), expression);
            }
        } finally {
            connectContext.getSessionVariable().setEnableSqlCache(enabled);
        }
    }

    private List<TPlanNode> scans(Planner planner) {
        List<TPlanNode> scans = new ArrayList<>();
        for (PlanFragment fragment : planner.getFragments()) {
            for (TPlanNode node : fragment.getPlanRoot().treeToThrift().getNodes()) {
                if (node.getNodeType() == TPlanNodeType.OLAP_SCAN_NODE) {
                    Assertions.assertTrue(node.isSetEnableConditionCache());
                    scans.add(node);
                }
            }
        }
        return scans;
    }
}
