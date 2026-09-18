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

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Audit: for every multi-input operator that depends on hash placement, all of its input
 * branches must end up on the SAME placement function.
 *
 * <p>Motivation: {@code RequireHash} means "I need hash-partitioned input"; it is satisfied by
 * GLOBAL_EXECUTION_HASH_SHUFFLE, LOCAL_EXECUTION_HASH_SHUFFLE and BUCKET_HASH_SHUFFLE alike.
 * It does NOT mean "all my branches must use the same hash function". A multi-input operator
 * that hands the generic require to every branch can therefore end up with one branch keeping
 * a storage-bucket placement while another is re-partitioned by execution hash — the same key
 * then sits in two different pipeline tasks. This audit walks the finished plan and flags that
 * mix instead of relying on a reviewer noticing it.
 */
public class LocalExchangePlacementAuditTest extends TestWithFeService {
    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        // b1/b2 share bucket count so the optimizer may bucket-shuffle one onto the other;
        // b3 has a different bucket count so it must be re-shuffled.
        createTable("CREATE TABLE test.b1 (k INT, k2 INT, v INT) DISTRIBUTED BY HASH(k) BUCKETS 6 "
                + "PROPERTIES ('replication_num'='1')");
        createTable("CREATE TABLE test.b2 (k INT, k2 INT, v INT) DISTRIBUTED BY HASH(k) BUCKETS 6 "
                + "PROPERTIES ('replication_num'='1')");
        createTable("CREATE TABLE test.b3 (k INT, k2 INT, v INT) DISTRIBUTED BY HASH(k) BUCKETS 7 "
                + "PROPERTIES ('replication_num'='1')");
    }

    /** The placement a branch actually lands on, as observed from the finished plan tree. */
    private static LocalExchangeType effectivePlacement(PlanNode node, ConnectContext ctx) {
        if (node instanceof LocalExchangeNode) {
            LocalExchangeType type = ((LocalExchangeNode) node).getExchangeType();
            // A PASSTHROUGH/BROADCAST/... wrapper does not decide hash placement; look through it.
            return type.isHashShuffle() ? type : effectivePlacement(node.getChild(0), ctx);
        }
        if (node instanceof ExchangeNode) {
            TPartitionType partitionType = ((ExchangeNode) node).getPartitionType();
            if (partitionType == TPartitionType.HASH_PARTITIONED) {
                return LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE;
            }
            if (partitionType == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED) {
                return LocalExchangeType.BUCKET_HASH_SHUFFLE;
            }
            return LocalExchangeType.NOOP;
        }
        if (node instanceof OlapScanNode) {
            // Mirrors OlapScanNode.enforceAndDeriveLocalExchange: a pooling (serial) scan claims
            // nothing, a non-pooling bucket scan claims its storage bucket distribution.
            boolean pooling = node.getFragment() != null && node.getFragment().useSerialSource(ctx);
            return pooling ? LocalExchangeType.NOOP : LocalExchangeType.BUCKET_HASH_SHUFFLE;
        }
        // Pass-through operators report their child's placement upward.
        if (node.getChildren().size() == 1) {
            return effectivePlacement(node.getChild(0), ctx);
        }
        return LocalExchangeType.NOOP;
    }

    /** Returns "" when the plan is consistent, otherwise a description of the mixed placement. */
    private static String findMixedPlacement(List<PlanFragment> fragments, ConnectContext ctx) {
        StringBuilder problems = new StringBuilder();
        for (PlanFragment fragment : fragments) {
            walk(fragment.getPlanRoot(), problems, ctx);
        }
        return problems.toString();
    }

    private static void walk(PlanNode node, StringBuilder problems, ConnectContext ctx) {
        boolean placementSensitive = node instanceof SetOperationNode || node instanceof HashJoinNode;
        if (placementSensitive && node.getChildren().size() > 1) {
            Map<LocalExchangeType, List<Integer>> byPlacement = new LinkedHashMap<>();
            for (int i = 0; i < node.getChildren().size(); i++) {
                LocalExchangeType placement = effectivePlacement(node.getChild(i), ctx);
                if (placement.isHashShuffle()) {
                    byPlacement.computeIfAbsent(placement, k -> new ArrayList<>()).add(i);
                }
            }
            if (byPlacement.size() > 1) {
                problems.append(node.getClass().getSimpleName())
                        .append('(').append(node.getId().asInt()).append(") mixes ")
                        .append(byPlacement).append('\n');
            }
        }
        for (PlanNode child : node.getChildren()) {
            walk(child, problems, ctx);
        }
    }

    private String audit(String label, String sql, boolean pooling, boolean bucketUpgrade)
            throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableLocalShufflePlanner(true);
        sv.setEnableLocalShuffle(true);
        sv.setEnableNereidsDistributePlanner(true);
        // ignore_storage_data_distribution is the real pooling switch: useSerialSource() gates on
        // it. force_to_local_shuffle only forces ScanNode.isSerialNode(), which a small table
        // already satisfies via `scanRangeNum < parallelExecInstanceNum * numScanBackends`, so
        // flipping it alone would not give a non-pooling arm at all.
        sv.setIgnoreStorageDataDistribution(pooling);
        sv.setForceToLocalShuffle(pooling);
        sv.setPipelineTaskNum(bucketUpgrade ? "16" : "8");
        sv.setBucketShuffleDowngradeRatio(0);
        // ratio <= 1 disables the upgrade entirely; 1.01 makes it fire whenever instances
        // slightly exceed buckets-with-data, which is what a bucket join above a mis-claiming
        // child needs in order to be fooled by that claim.
        sv.setLocalShuffleBucketUpgradeRatio(bucketUpgrade ? 1.01 : 1.5);
        sv.disableColocatePlan = true;

        StmtExecutor executor = executeNereidsSql("explain distributed plan " + sql);
        NereidsPlanner planner = (NereidsPlanner) executor.planner();
        String problems = findMixedPlacement(planner.getFragments(), connectContext);
        return problems.isEmpty() ? "" : "MIXED | pooling=" + pooling + " upgrade=" + bucketUpgrade
                + " | " + label + " | " + problems.trim().replace('\n', ';');
    }

    @Test
    public void auditPlacementConsistency() {
        List<String> failures = new ArrayList<>();
        // set operation kind x consumer kind x which side needs re-shuffling
        String[][] cases = {
            {"union+window(partition by bucket key)",
                "select k, row_number() over (partition by k order by v) from "
                    + "(select k, v from test.b1 union all select k, v from test.b2) u"},
            {"union+window(partition by non-bucket key)",
                "select k2, row_number() over (partition by k2 order by v) from "
                    + "(select k2, v from test.b1 union all select k2, v from test.b2) u"},
            {"union+window(no order by)",
                "select k, row_number() over (partition by k) from "
                    + "(select k from test.b1 union all select k from test.b2) u"},
            {"union+agg",
                "select k, count(*) from (select k from test.b1 union all select k from test.b2) u group by k"},
            {"union+agg(distinct)",
                "select k, count(distinct v) from "
                    + "(select k, v from test.b1 union all select k, v from test.b2) u group by k"},
            {"union+shuffle join",
                "select u.k from (select k from test.b1 union all select k from test.b2) u "
                    + "join[shuffle] test.b3 t on u.k = t.k"},
            {"union+bucket join",
                "select u.k from (select k from test.b1 union all select k from test.b2) u "
                    + "join test.b1 t on u.k = t.k"},
            {"union of different bucket counts + window",
                "select k, row_number() over (partition by k order by v) from "
                    + "(select k, v from test.b1 union all select k, v from test.b3) u"},
            {"3-way union + window",
                "select k, row_number() over (partition by k order by v) from "
                    + "(select k, v from test.b1 union all select k, v from test.b2 "
                    + "union all select k, v from test.b3) u"},
            {"union(scan, values) + window",
                "select k, row_number() over (partition by k order by v) from "
                    + "(select k, v from test.b1 union all select 1, 2) u"},
            {"intersect+window",
                "select k, row_number() over (partition by k order by k) from "
                    + "(select k from test.b1 intersect select k from test.b2) u"},
            {"except+window",
                "select k, row_number() over (partition by k order by k) from "
                    + "(select k from test.b1 except select k from test.b2) u"},
            {"intersect(join as basic child)+window",
                "select k, row_number() over (partition by k order by k) from "
                    + "(select a.k from test.b1 a join test.b2 b on a.k=b.k intersect "
                    + "select k from test.b3) u"},
            {"nested union under intersect + window",
                "select k, row_number() over (partition by k order by k) from "
                    + "((select k from test.b1 union all select k from test.b2) "
                    + "intersect select k from test.b3) u"},
            {"union under union + window",
                "select k, row_number() over (partition by k order by v) from "
                    + "(select k, v from test.b1 union all "
                    + "(select k, v from test.b2 union all select k, v from test.b3)) u"},
            {"window(partition by bucket key) under bucket join",
                "select w.k from (select k, row_number() over (partition by k) rn from test.b1) w "
                    + "join test.b1 t on w.k = t.k"},
            {"window(partition by bucket key, no order by) under shuffle join",
                "select w.k from (select k, row_number() over (partition by k) rn from test.b1) w "
                    + "join[shuffle] test.b3 t on w.k = t.k"},
            {"window(partition by bucket key, order by) under bucket join",
                "select w.k from (select k, row_number() over (partition by k order by v) rn "
                    + "from test.b1) w join test.b1 t on w.k = t.k"},
            {"window(no order by) under bucket join on 2 tables",
                "select w.k from (select k, row_number() over (partition by k) rn from test.b1) w "
                    + "join test.b2 t on w.k = t.k"},
            {"intersect under bucket join",
                "select u.k from (select k from test.b1 intersect select k from test.b3) u "
                    + "join test.b1 t on u.k = t.k"},
            {"union under bucket join under window",
                "select k, row_number() over (partition by k order by c) from "
                    + "(select u.k k, count(*) c from "
                    + "(select k from test.b1 union all select k from test.b2) u "
                    + "join test.b1 t on u.k = t.k group by u.k) x"},
            {"agg over union under bucket join",
                "select u.k from (select k, count(*) c from "
                    + "(select k from test.b1 union all select k from test.b2) x group by k) u "
                    + "join test.b1 t on u.k = t.k"},
        };
        for (boolean pooling : new boolean[] {true, false}) {
            for (boolean bucketUpgrade : new boolean[] {false, true}) {
                for (String[] c : cases) {
                    try {
                        String failure = audit(c[0], c[1], pooling, bucketUpgrade);
                        if (!failure.isEmpty()) {
                            failures.add(failure);
                        }
                    } catch (Exception e) {
                        failures.add("PLANFAIL | pooling=" + pooling + " upgrade=" + bucketUpgrade
                                + " | " + c[0] + " | " + e);
                    }
                }
            }
        }
        Assertions.assertTrue(failures.isEmpty(), String.join("\n", failures));
    }
}
