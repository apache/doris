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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class BuildStructInfoTest extends SqlTestBase {
    @Test
    void testSimpleSQL() {
        String sql = "select * from T1, T2, T3, T4 "
                + "where "
                + "T1.id = T2.id and "
                + "T2.score = T3.score and "
                + "T3.id = T4.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .matches(logicalJoin()
                        .when(j -> {
                            HyperGraph.toStructInfo(j);
                            return true;
                        }));

    }

    @Test
    void testStructInfoNode() {
        String sql = "select * from T1 inner join "
                + "(select sum(id) as id from T2 where id = 1) T2 "
                + "on T1.id = T2.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .matches(logicalJoin()
                        .when(j -> {
                            List<HyperGraph> hyperGraph = HyperGraph.toStructInfo(j);
                            Assertions.assertTrue(hyperGraph.get(0).getNodes().stream()
                                    .allMatch(n -> n.getPlan()
                                            .collectToList(GroupPlan.class::isInstance).isEmpty()));
                            return true;
                        }));

    }

    @Test
    void testFilter() {
        String sql = "select * from T1 left outer join "
                + " (select id from T2 where id = 1) T2 "
                + "on T1.id = T2.id ";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin()
                        .when(j -> {
                            HyperGraph structInfo = HyperGraph.toStructInfo(j).get(0);
                            Assertions.assertTrue(structInfo.getJoinEdge(0).getJoinType().isLeftOuterJoin());
                            Assertions.assertEquals(0, (int) structInfo.getFilterEdge(0).getRejectEdges().get(0));
                            return true;
                        }));

        sql = "select * from (select id from T1 where id = 0) T1 left outer join T2 "
                + "on T1.id = T2.id ";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin()
                        .when(j -> {
                            HyperGraph structInfo = HyperGraph.toStructInfo(j).get(0);
                            Assertions.assertTrue(structInfo.getJoinEdge(0).getJoinType().isLeftOuterJoin());
                            Assertions.assertTrue(structInfo.getFilterEdge(0).getRejectEdges().isEmpty());
                            return true;
                        }));
    }
}
