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

package org.apache.doris.nereids.memo;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.Set;
import java.util.stream.Collectors;

class StructInfoMapTest extends SqlTestBase {
    @Test
    void testTableMap() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };
        CascadesContext c1 = createCascadesContext(
                "select T1.id from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .optimize();
        Group root = c1.getMemo().getRoot();
        Set<BitSet> tableMaps = root.getstructInfoMap().getTableMaps();
        Assertions.assertTrue(tableMaps.isEmpty());
        root.getstructInfoMap().refresh(root, c1);
        Assertions.assertEquals(1, tableMaps.size());
        new MockUp<MTMVRelationManager>() {
            @Mock
            public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx) {
                return true;
            }
        };
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;

        createMvByNereids("create materialized view mv1 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select T1.id from T1 inner join T2 "
                + "on T1.id = T2.id;");
        c1 = createCascadesContext(
                "select T1.id from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .optimize()
                .printlnBestPlanTree();
        root = c1.getMemo().getRoot();
        root.getstructInfoMap().refresh(root, c1);
        tableMaps = root.getstructInfoMap().getTableMaps();
        Assertions.assertEquals(2, tableMaps.size());
        dropMvByNereids("drop materialized view mv1");
    }

    @Test
    void testLazyRefresh() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };
        CascadesContext c1 = createCascadesContext(
                "select T1.id from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .optimize();
        Group root = c1.getMemo().getRoot();
        Set<BitSet> tableMaps = root.getstructInfoMap().getTableMaps();
        Assertions.assertTrue(tableMaps.isEmpty());
        root.getstructInfoMap().refresh(root, c1);
        root.getstructInfoMap().refresh(root, c1);
        Assertions.assertEquals(1, tableMaps.size());
        new MockUp<MTMVRelationManager>() {
            @Mock
            public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx) {
                return true;
            }
        };
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv1 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select T1.id from T1 inner join T2 "
                + "on T1.id = T2.id;");
        c1 = createCascadesContext(
                "select T1.id from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .optimize()
                .printlnBestPlanTree();
        root = c1.getMemo().getRoot();
        root.getstructInfoMap().refresh(root, c1);
        root.getstructInfoMap().refresh(root, c1);
        tableMaps = root.getstructInfoMap().getTableMaps();
        Assertions.assertEquals(2, tableMaps.size());
        dropMvByNereids("drop materialized view mv1");
    }

    @Test
    void testTableChild() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };
        CascadesContext c1 = createCascadesContext(
                "select T1.id from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        new MockUp<MTMVRelationManager>() {
            @Mock
            public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx) {
                return true;
            }
        };
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv1 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select T1.id from T1 inner join T2 "
                + "on T1.id = T2.id;");
        c1 = createCascadesContext(
                "select T1.id from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .optimize();
        Group root = c1.getMemo().getRoot();
        root.getstructInfoMap().refresh(root, c1);
        StructInfoMap structInfoMap = root.getstructInfoMap();
        Assertions.assertEquals(2, structInfoMap.getTableMaps().size());
        BitSet mvMap = structInfoMap.getTableMaps().stream()
                .filter(b -> b.cardinality() == 2)
                .collect(Collectors.toList()).get(0);
        StructInfo structInfo = structInfoMap.getStructInfo(c1, mvMap, root, null);
        System.out.println(structInfo.getOriginalPlan().treeString());
        BitSet bitSet = new BitSet();
        for (CatalogRelation relation : structInfo.getRelations()) {
            bitSet.set(c1.getStatementContext().getTableId(relation.getTable()).asInt());
        }
        Assertions.assertEquals(bitSet, mvMap);
        dropMvByNereids("drop materialized view mv1");
    }
}
