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
import org.apache.doris.catalog.HashDistributionInfo.HashType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
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

/** SQL-to-thrift coverage of the storage layout INSIDE a set operation, not its parent join. */
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
}
