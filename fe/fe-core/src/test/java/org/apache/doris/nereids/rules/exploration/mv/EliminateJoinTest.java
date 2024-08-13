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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class EliminateJoinTest extends SqlTestBase {
    @Test
    void testLOJWithGroupBy() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join (select id from T2 group by id) T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        CascadesContext c3 = createCascadesContext(
                "select * from T1 left outer join (select id as id2 from T2 group by id) T2 "
                        + "on T1.id = T2.id2 ",
                connectContext
        );
        Plan p3 = PlanChecker.from(c3)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        HyperGraph h3 = HyperGraph.builderForMv(p3).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(!HyperGraphComparator.isLogicCompatible(h1, h3,
                constructContext(p1, p2, c1)).isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
    }

    @Test
    void testLOJWithUK() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint uk unique (id)");
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        dropConstraint("alter table T2 drop constraint uk");
    }

    @Test
    void testLOJWithPKFK() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk primary key (id)");
        addConstraint("alter table T1 add constraint fk foreign key (id) references T2(id)");
        CascadesContext c2 = createCascadesContext(
                "select * from T1 inner join T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        CascadesContext c3 = createCascadesContext(
                "select * from T1 inner join (select id as id2 from T2) T2 "
                        + "on T1.id = T2.id2 ",
                connectContext
        );
        Plan p3 = PlanChecker.from(c3)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        HyperGraph h3 = HyperGraph.builderForMv(p3).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        Assertions.assertTrue(!HyperGraphComparator.isLogicCompatible(h1, h3, constructContext(p1, p2, c1)).isInvalid());
        dropConstraint("alter table T2 drop constraint pk");
    }

    @Disabled
    @Test
    void testLOJWithPKFKAndUK1() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk primary key (id)");
        addConstraint("alter table T1 add constraint fk foreign key (id) references T2(id)");
        addConstraint("alter table T3 add constraint uk unique (id)");
        CascadesContext c2 = createCascadesContext(
                "select * from (select T1.*, T3.id as id3 from T1 left outer join T3 on T1.id = T3.id) T1 inner join T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        dropConstraint("alter table T2 drop constraint pk");
        dropConstraint("alter table T3 drop constraint uk");
    }

    @Test
    void testLOJWithPKFKAndUK2() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk primary key (id)");
        addConstraint("alter table T1 add constraint fk foreign key (id) references T2(id)");
        addConstraint("alter table T3 add constraint uk unique (id)");
        CascadesContext c2 = createCascadesContext(
                "select * from (select T1.*, T2.id as id2 from T1 inner join T2 on T1.id = T2.id) T1 left outer join T3 "
                        + "on T1.id = T3.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        dropConstraint("alter table T2 drop constraint pk");
        dropConstraint("alter table T3 drop constraint uk");
    }
}
