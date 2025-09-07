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

package org.apache.doris.nereids.mv;

import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Test get available mvs after rewrite by rules
 */
public class OptimizeGetAvailableMvsTest extends SqlTestBase {

    @Test
    void testWhenNotPartitionPrune() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Partition getPartition(long partitionId) {
                return new Partition() {
                    @Override
                    public long getId() {
                        return 1L;
                    }

                    @Override
                    public String getName() {
                        return "mock_partition";
                    }

                    @Override
                    public PartitionState getState() {
                        return PartitionState.NORMAL;
                    }

                    @Override
                    public MaterializedIndex getIndex(long indexId) {
                        return new MaterializedIndex(1L, IndexState.NORMAL);
                    }

                    @Override
                    public DistributionInfo getDistributionInfo() {
                        return new DistributionInfo() {
                            @Override
                            public DistributionInfoType getType() {
                                return DistributionInfoType.RANDOM;
                            }
                        };
                    }
                };
            }
        };

        new MockUp<LogicalOlapScan>() {
            @Mock
            public List<Long> getSelectedPartitionIds() {
                return Lists.newArrayList(1L);
            }
        };

        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv1 "
                + "        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        PARTITION BY (id)\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as "
                + "        select T4.id from T4 inner join T2 "
                + "        on T4.id = T2.id;");
        CascadesContext c1 = createCascadesContext(
                "select T4.id "
                        + "from T4 "
                        + "inner join T2 on T4.id = T2.id "
                        + "inner join T3 on T4.id = T3.id",
                connectContext
        );
        PlanChecker.from(c1)
                .setIsQuery()
                .analyze()
                .rewrite()
                .preMvRewrite()
                .optimize()
                .printlnBestPlanTree();
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = c1.getStatementContext()
                .getTableUsedPartitionNameMap();
        Assertions.assertFalse(tableUsedPartitionNameMap.isEmpty());

        for (Map.Entry<List<String>, Pair<RelationId, Set<String>>> tableInfoEntry
                : tableUsedPartitionNameMap.entries()) {
            if (tableInfoEntry.getKey().contains("T2")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            } else if (tableInfoEntry.getKey().contains("T3")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            } else if (tableInfoEntry.getKey().contains("T4")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            }
        }

        Map<BaseTableInfo, Collection<Partition>> mvCanRewritePartitionsMap = c1.getStatementContext()
                .getMvCanRewritePartitionsMap();
        Assertions.assertEquals(1, mvCanRewritePartitionsMap.size());
        Assertions.assertTrue(mvCanRewritePartitionsMap.keySet().iterator().next().getTableName()
                .equalsIgnoreCase("mv1"));

        dropMvByNereids("drop materialized view mv1");
    }

    @Test
    void testWhenPartitionPrune() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };

        new MockUp<PartitionPruner>() {
            @Mock
            public <K extends Comparable<K>> List<Long> prune(List<Slot> partitionSlots, Expression partitionPredicate,
                    Map<K, PartitionItem> idToPartitions, CascadesContext cascadesContext,
                    PartitionTableType partitionTableType, Optional<SortedPartitionRanges<K>> sortedPartitionRanges) {
                return Lists.newArrayList(1L);
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Partition getPartition(long partitionId) {
                return new Partition() {
                    @Override
                    public long getId() {
                        return 1L;
                    }

                    @Override
                    public String getName() {
                        return "mock_partition";
                    }

                    @Override
                    public PartitionState getState() {
                        return PartitionState.NORMAL;
                    }

                    @Override
                    public MaterializedIndex getIndex(long indexId) {
                        return new MaterializedIndex(1L, IndexState.NORMAL);
                    }

                    @Override
                    public DistributionInfo getDistributionInfo() {
                        return new DistributionInfo() {
                            @Override
                            public DistributionInfoType getType() {
                                return DistributionInfoType.RANDOM;
                            }
                        };
                    }
                };
            }
        };

        new MockUp<LogicalOlapScan>() {
            @Mock
            public List<Long> getSelectedPartitionIds() {
                return Lists.newArrayList(1L);
            }
        };

        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv2 "
                + "        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        PARTITION BY (id)\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as "
                + "        select T4.id from T4 inner join T2 "
                + "        on T4.id = T2.id;");
        CascadesContext c1 = createCascadesContext(
                "select T4.id "
                        + "from T4 "
                        + "inner join T2 on T4.id = T2.id "
                        + "inner join T3 on T4.id = T3.id "
                        + "where T4.id > 0",
                connectContext
        );
        PlanChecker.from(c1)
                .setIsQuery()
                .analyze()
                .rewrite()
                .preMvRewrite()
                .optimize()
                .printlnBestPlanTree();
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = c1.getStatementContext()
                .getTableUsedPartitionNameMap();
        Map<BaseTableInfo, Collection<Partition>> mvCanRewritePartitionsMap = c1.getStatementContext()
                .getMvCanRewritePartitionsMap();
        Assertions.assertFalse(tableUsedPartitionNameMap.isEmpty());

        for (Map.Entry<List<String>, Pair<RelationId, Set<String>>> tableInfoEntry
                : tableUsedPartitionNameMap.entries()) {
            if (tableInfoEntry.getKey().contains("T2")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            } else if (tableInfoEntry.getKey().contains("T3")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            } else if (tableInfoEntry.getKey().contains("T4")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            }
        }

        Assertions.assertEquals(1, mvCanRewritePartitionsMap.size());
        Assertions.assertTrue(mvCanRewritePartitionsMap.keySet().iterator().next().getTableName()
                .equalsIgnoreCase("mv2"));

        dropMvByNereids("drop materialized view mv2");
    }
}
