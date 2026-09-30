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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo.HashType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.planner.DistributionMode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.NestedLoopJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.thrift.TDistributionHashType;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** SQL-to-thrift coverage of storage layouts in set operations and broadcast joins. */
public class IdentitySetOperationTest extends TestWithFeService {
    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("identity_set_operation");
        useDatabase("identity_set_operation");
        createTable("CREATE TABLE identity8(id BIGINT NOT NULL) DISTRIBUTED BY HASH(id) BUCKETS 8 "
                + "PROPERTIES('replication_num'='1', 'distribution_hash_type'='identity')");
        createTable("CREATE TABLE identity5(id BIGINT NOT NULL) DISTRIBUTED BY HASH(id) BUCKETS 5 "
                + "PROPERTIES('replication_num'='1', 'distribution_hash_type'='identity')");
        createTable("CREATE TABLE crc7(id BIGINT NOT NULL) DISTRIBUTED BY HASH(id) BUCKETS 7 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE random8(id BIGINT NOT NULL) DISTRIBUTED BY RANDOM BUCKETS 8 "
                + "PROPERTIES('replication_num'='1')");
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setEnableLocalShufflePlanner(true);
        sv.setEnableLocalShuffle(true);
        sv.setEnableNereidsDistributePlanner(true);
        sv.setPipelineTaskNum("4");
        sv.setBucketShuffleDowngradeRatio(0);
        // Force a serial basic scan so a BUCKET_HASH_SHUFFLE local exchange must align it.
        sv.setForceToLocalShuffle(true);
        // Keep the SQL child order so the matrix really tests both left and right basics.
        sv.setDisableNereidsRules("REORDER_INTERSECT");
    }

    @Test
    public void testIdentityLeftBasic() throws Exception {
        checkSetOperations("identity8", "identity5", 0, HashType.IDENTITY);
    }

    @Test
    public void testIdentityRightBasic() throws Exception {
        checkSetOperations("identity5", "identity8", 1, HashType.IDENTITY);
    }

    @Test
    public void testMixedIdentityTarget() throws Exception {
        checkSetOperations("identity8", "crc7", 0, HashType.IDENTITY);
    }

    @Test
    public void testMixedCrc32Target() throws Exception {
        checkSetOperations("identity8", "crc7", 1, HashType.CRC32);
    }

    @Test
    public void testRandomProbeIdentityBroadcastToThrift() throws Exception {
        // Specify broadcast in LEADING so join reconstruction preserves it regardless of other tests' statistics.
        checkBroadcastToThrift("SELECT /*+ LEADING(r broadcast i) */ r.id FROM random8 r "
                + "JOIN [broadcast] identity8 i ON r.id = i.id", "random8", "identity8", 1);
    }

    @Test
    public void testIdentityProbeRandomBroadcastToThrift() throws Exception {
        checkBroadcastToThrift("SELECT /*+ LEADING(i broadcast r) */ i.id FROM identity8 i "
                + "JOIN [broadcast] random8 r ON i.id = r.id", "identity8", "random8", 1);
    }

    @Test
    public void testRandomProbeNestedIdentityBroadcastToThrift() throws Exception {
        checkBroadcastToThrift("SELECT /*+ LEADING(r broadcast i broadcast j) */ r.id FROM random8 r "
                + "JOIN [broadcast] identity8 i ON r.id = i.id "
                + "JOIN [broadcast] identity8 j ON r.id = j.id", "random8", "identity8", 2);
    }

    @Test
    public void testRandomProbeIdentityNestedLoopToThrift() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean oldLocalShufflePlanner = sv.isEnableLocalShufflePlanner();
        try {
            for (boolean localShufflePlanner : new boolean[] {false, true}) {
                sv.setEnableLocalShufflePlanner(localShufflePlanner);
                String sql = "SELECT /*+ LEADING(r i) */ r.id FROM random8 r JOIN identity8 i ON r.id < i.id";
                NereidsPlanner planner = (NereidsPlanner) executeNereidsSql("explain distributed plan " + sql)
                        .planner();
                int joinCount = 0;
                for (PlanFragment fragment : planner.getFragments()) {
                    List<NestedLoopJoinNode> joins = fragment.getPlanRoot()
                            .collectInCurrentFragment(node -> node instanceof NestedLoopJoinNode);
                    for (NestedLoopJoinNode join : joins) {
                        joinCount++;
                        List<OlapScanNode> probes = join.getChild(0)
                                .collectInCurrentFragment(node -> node instanceof OlapScanNode);
                        Assertions.assertEquals(1, probes.size());
                        Assertions.assertEquals("random8", probes.get(0).getOlapTable().getName());
                        if (!localShufflePlanner) {
                            Assertions.assertNull(join.getStorageDistributionHashType());
                        }
                    }
                    Assertions.assertDoesNotThrow(() -> fragment.toThrift(),
                            "localShufflePlanner=" + localShufflePlanner + ": " + sql);
                }
                Assertions.assertEquals(1, joinCount);
            }
        } finally {
            sv.setEnableLocalShufflePlanner(oldLocalShufflePlanner);
        }
    }

    private void checkBroadcastToThrift(String sql, String probeTable, String buildTable, int expectedJoins)
            throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean oldLocalShufflePlanner = sv.isEnableLocalShufflePlanner();
        boolean oldForceToLocalShuffle = sv.isForceToLocalShuffle();
        try {
            sv.setForceToLocalShuffle(false);
            for (boolean localShufflePlanner : new boolean[] {false, true}) {
                sv.setEnableLocalShufflePlanner(localShufflePlanner);
                String context = "localShufflePlanner=" + localShufflePlanner + ": " + sql;
                NereidsPlanner planner = (NereidsPlanner) executeNereidsSql("explain distributed plan " + sql)
                        .planner();
                Assertions.assertTrue(SessionVariable.canUseNereidsDistributePlanner(connectContext), context);
                List<PlanFragment> fragments = planner.getFragments();
                int joinCount = 0;
                for (PlanFragment fragment : fragments) {
                    List<HashJoinNode> joins = fragment.getPlanRoot()
                            .collectInCurrentFragment(node -> node instanceof HashJoinNode);
                    for (HashJoinNode join : joins) {
                        joinCount++;
                        Assertions.assertEquals(DistributionMode.BROADCAST, join.getDistributionMode(), context);
                        // Stay in this fragment: a build scan behind a broadcast exchange cannot define the probe layout.
                        List<OlapScanNode> probes = join.getChild(0)
                                .collectInCurrentFragment(node -> node instanceof OlapScanNode);
                        Assertions.assertEquals(1, probes.size(), context);
                        OlapTable probe = probes.get(0).getOlapTable();
                        Assertions.assertEquals(probeTable, probe.getName(), context);
                        if (probeTable.equals("random8")) {
                            Assertions.assertInstanceOf(RandomDistributionInfo.class,
                                    probe.getDefaultDistributionInfo(), context);
                            Assertions.assertEquals(TPartitionType.RANDOM,
                                    fragment.getDataPartition().getType(), context);
                            if (!localShufflePlanner) {
                                Assertions.assertNull(join.getStorageDistributionHashType(), context);
                            }
                        } else {
                            Assertions.assertEquals(HashType.IDENTITY,
                                    ((HashDistributionInfo) probe.getDefaultDistributionInfo()).getHashType(), context);
                        }
                        List<ExchangeNode> builds = join.getChild(1)
                                .collectInCurrentFragment(node -> node instanceof ExchangeNode);
                        Assertions.assertEquals(1, builds.size(), context);
                        Assertions.assertEquals(TPartitionType.UNPARTITIONED,
                                builds.get(0).getPartitionType(), context);
                        PlanFragment sender = fragments.stream().filter(f -> f.getDestNode() == builds.get(0))
                                .findFirst().orElseThrow();
                        List<OlapScanNode> buildScans = sender.getPlanRoot()
                                .collectInCurrentFragment(node -> node instanceof OlapScanNode);
                        Assertions.assertEquals(1, buildScans.size(), context);
                        Assertions.assertEquals(buildTable, buildScans.get(0).getOlapTable().getName(), context);
                    }
                }
                Assertions.assertEquals(expectedJoins, joinCount, context);
                // EXPLAIN does not serialize every fragment; exercise the fallback that incorrectly included the build.
                for (PlanFragment fragment : fragments) {
                    Assertions.assertDoesNotThrow(() -> fragment.toThrift(), context);
                }
            }
        } finally {
            sv.setEnableLocalShufflePlanner(oldLocalShufflePlanner);
            sv.setForceToLocalShuffle(oldForceToLocalShuffle);
        }
    }

    private void checkSetOperations(String left, String right, int basicIndex, HashType hashType)
            throws Exception {
        String[] tables = {left, right};
        for (int i = 0; i < tables.length; i++) {
            // Mock backend reports rather than ALTER STATS, which requires the internal
            // statistics repository (not started by TestWithFeService).
            OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrMetaException("identity_set_operation").getTableOrMetaException(tables[i]);
            for (Partition partition : table.getPartitions()) {
                partition.getBaseIndex().setRowCount(i == basicIndex ? 10000 : 100);
                partition.getBaseIndex().setRowCountReported(true);
            }
        }
        for (String op : new String[] {"UNION ALL", "INTERSECT", "EXCEPT"}) {
            String sql = "SELECT id, row_number() OVER (PARTITION BY id ORDER BY id) rn FROM "
                    + "(SELECT id FROM " + left + " " + op + " SELECT id FROM " + right + ") u";
            NereidsPlanner planner = (NereidsPlanner) executeNereidsSql("explain distributed plan " + sql)
                    .planner();
            Assertions.assertTrue(SessionVariable.canUseNereidsDistributePlanner(connectContext));
            List<PhysicalSetOperation> sets = new ArrayList<>();
            collectPhysicalSets(planner.getOptimizedPlan(), sets);
            Assertions.assertEquals(1, sets.size(), sql);
            PhysicalSetOperation set = sets.get(0);
            DistributionSpecHash output = hashSpec(set);
            Assertions.assertEquals(ShuffleType.NATURAL, output.getShuffleType(), sql);
            Assertions.assertEquals(hashType, output.getHashType(), sql);
            Assertions.assertInstanceOf(PhysicalOlapScan.class, set.child(basicIndex), sql);
            Assertions.assertEquals(tables[basicIndex],
                    ((PhysicalOlapScan) set.child(basicIndex)).getTable().getName(), sql);
            DistributionSpecHash basic = hashSpec((PhysicalPlan) set.child(basicIndex));
            Assertions.assertEquals(basic.getTableId(), output.getTableId(), sql);
            Assertions.assertEquals(basic.getPartitionIds(), output.getPartitionIds(), sql);
            Assertions.assertEquals(set.getOutput().get(0).getExprId(), output.getOrderedShuffledColumns().get(0));
            PhysicalDistribute<?> shuffled = Assertions.assertInstanceOf(PhysicalDistribute.class,
                    set.child(1 - basicIndex), sql);
            DistributionSpecHash shuffledSpec = hashSpec(shuffled);
            Assertions.assertEquals(ShuffleType.STORAGE_BUCKETED, shuffledSpec.getShuffleType(), sql);
            Assertions.assertEquals(hashType, shuffledSpec.getHashType(), sql);
            checkTranslatedSet(planner.getFragments(), hashType, sql);
        }
    }

    private static DistributionSpecHash hashSpec(PhysicalPlan plan) {
        return Assertions.assertInstanceOf(DistributionSpecHash.class,
                plan.getPhysicalProperties().getDistributionSpec());
    }

    private static void collectPhysicalSets(Plan plan, List<PhysicalSetOperation> sets) {
        if (plan instanceof PhysicalSetOperation) {
            sets.add((PhysicalSetOperation) plan);
        }
        for (Plan child : plan.children()) {
            collectPhysicalSets(child, sets);
        }
    }

    private static void checkTranslatedSet(List<PlanFragment> fragments, HashType hashType, String sql) {
        List<SetOperationNode> sets = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            collectSetNodes(fragment.getPlanRoot(), sets);
        }
        Assertions.assertEquals(1, sets.size(), sql);
        SetOperationNode set = sets.get(0);
        Assertions.assertTrue(set.isBucketShuffle(), sql);
        Assertions.assertEquals(hashType, set.getStorageDistributionHashType(), sql);
        List<ExchangeNode> remotes = new ArrayList<>();
        List<LocalExchangeNode> locals = new ArrayList<>();
        for (PlanNode child : set.getChildren()) {
            collectExchanges(child, remotes, locals);
        }
        Assertions.assertEquals(1, remotes.size(), sql);
        ExchangeNode remote = remotes.get(0);
        Assertions.assertEquals(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, remote.getPartitionType(), sql);
        Assertions.assertEquals(hashType, remote.getDistributionHashType(), sql);
        TDistributionHashType thriftHash = hashType == HashType.IDENTITY
                ? TDistributionHashType.IDENTITY : TDistributionHashType.CRC32;
        PlanFragment sender = fragments.stream().filter(f -> f.getDestNode() == remote).findFirst().orElseThrow();
        Assertions.assertEquals(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED,
                sender.getOutputPartition().toThrift().getType(), sql);
        Assertions.assertEquals(thriftHash, sender.getOutputPartition().toThrift().getDistributionHashType(), sql);
        Assertions.assertFalse(locals.isEmpty(), "must exercise FE local bucket exchange: " + sql);
        for (LocalExchangeNode local : locals) {
            Assertions.assertEquals(LocalExchangeType.BUCKET_HASH_SHUFFLE, local.getExchangeType(), sql);
            Assertions.assertEquals(thriftHash, local.treeToThrift().getNodes().get(0)
                    .getLocalExchangeNode().getDistributionHashType(), sql);
        }
    }

    private static void collectSetNodes(PlanNode node, List<SetOperationNode> sets) {
        if (node instanceof SetOperationNode) {
            sets.add((SetOperationNode) node);
        }
        if (!(node instanceof ExchangeNode)) {
            for (PlanNode child : node.getChildren()) {
                collectSetNodes(child, sets);
            }
        }
    }

    private static void collectExchanges(PlanNode node, List<ExchangeNode> remotes,
            List<LocalExchangeNode> locals) {
        if (node instanceof ExchangeNode) {
            remotes.add((ExchangeNode) node);
            return;
        }
        // PASSTHROUGH wrappers below the bucket exchange do not determine hash placement.
        // Keep every hash exchange so an accidental execution-hash re-alignment still fails.
        if (node instanceof LocalExchangeNode
                && ((LocalExchangeNode) node).getExchangeType().isHashShuffle()) {
            locals.add((LocalExchangeNode) node);
        }
        for (PlanNode child : node.getChildren()) {
            collectExchanges(child, remotes, locals);
        }
    }

    /**
     * ADD PARTITION on an identity-distributed table must inherit the table's hash type:
     * DDL cannot carry distribution_hash_type, so InternalCatalog.addPartition overwrites the
     * new partition's hash type with the table's. If the inheritance were dropped, BE would
     * bucket rows in the new partition with one hash function while FE pruned with another,
     * making the new partition's rows unreadable through equality pruning. This drives the
     * real addPartition path (not the hash-type setter) and asserts the stored metadata.
     */
    @Test
    public void testAddPartitionInheritsIdentityHashType() throws Exception {
        useDatabase("identity_set_operation");
        createTable("CREATE TABLE identity_add_partition (id BIGINT NOT NULL, dt INT NOT NULL) "
                + "PARTITION BY RANGE(dt) ( PARTITION p1 values less than (10) ) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 5 "
                + "PROPERTIES('replication_num'='1', 'distribution_hash_type'='identity')");

        String addPartitionSql = "ALTER TABLE identity_add_partition ADD PARTITION p2 values less than (20) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 5";
        Assertions.assertNotNull(getSqlStmtExecutor(addPartitionSql));

        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrAnalysisException("identity_set_operation")
                .getTableOrAnalysisException("identity_add_partition");
        Partition added = table.getPartition("p2");
        Assertions.assertNotNull(added, "ADD PARTITION must create p2");
        Assertions.assertTrue(added.getDistributionInfo() instanceof HashDistributionInfo,
                "new partition must keep a hash distribution");
        Assertions.assertEquals(HashType.IDENTITY,
                ((HashDistributionInfo) added.getDistributionInfo()).getHashType(),
                "ADD PARTITION must inherit the table's identity hash type");
        // the initial partition keeps its type too
        Assertions.assertEquals(HashType.IDENTITY,
                ((HashDistributionInfo) table.getPartition("p1").getDistributionInfo()).getHashType());
    }
}
