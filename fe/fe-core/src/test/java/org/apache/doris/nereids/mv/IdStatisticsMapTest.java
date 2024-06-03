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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.Map;
import java.util.Set;

/**
 * Test idStatisticsMap in StatementContext is valid
 */
public class IdStatisticsMapTest extends SqlTestBase {

    @Test
    void testIdStatisticsIsExistWhenRewriteByMv() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };
        new MockUp<MTMVRelationManager>() {
            @Mock
            public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx) {
                return true;
            }
        };
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv100 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select T1.id from T1 inner join T2 "
                + "on T1.id = T2.id;");
        CascadesContext c1 = createCascadesContext(
                "select T1.id from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        PlanChecker tmpPlanChecker = PlanChecker.from(c1)
                .analyze()
                .rewrite();
        // scan plan output will be refreshed after mv rewrite successfully, so need tmp store
        Set<Slot> materializationScanOutput = c1.getMaterializationContexts().get(0).getScanPlan().getOutputSet();
        tmpPlanChecker
                .optimize()
                .printlnBestPlanTree();
        Map<RelationId, Statistics> idStatisticsMap = c1.getStatementContext().getRelationIdToStatisticsMap();
        Assertions.assertFalse(idStatisticsMap.isEmpty());
        Statistics statistics = idStatisticsMap.values().iterator().next();
        // statistics key set should be equals to materialization scan plan output
        Assertions.assertEquals(materializationScanOutput, statistics.columnStatistics().keySet());
        dropMvByNereids("drop materialized view mv100");
    }
}
