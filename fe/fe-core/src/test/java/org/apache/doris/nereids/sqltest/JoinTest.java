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

package org.apache.doris.nereids.sqltest;

import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JoinTest extends SqlTestBase {
    @Test
    void testJoinUsing() {
        String sql = "SELECT * FROM T1 JOIN T2 using (id)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new ReorderJoin())
                .matches(
                        innerLogicalJoin().when(j -> j.getHashJoinConjuncts().size() == 1)
                );
    }

    @Test
    void testColocatedJoin() {
        String sql = "select * from T2 join T2 b on T2.id = b.id and T2.id = b.id;";
        String plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .optimize()
                .getBestPlanTree()
                .treeString();
        Assertions.assertEquals(plan,
                "PhysicalHashJoin ( type=INNER_JOIN, hashJoinCondition=[(id#0 = id#2), (id#0 = id#2)], otherJoinCondition=[], stats=(rows=1, width=2, penalty=0.0) )\n"
                        + "|--PhysicalOlapScan ( qualified=default_cluster:test.T2, output=[id#0, score#1], stats=(rows=1, width=1, penalty=0.0) )\n"
                        + "+--PhysicalOlapScan ( qualified=default_cluster:test.T2, output=[id#2, score#3], stats=(rows=1, width=1, penalty=0.0) )");
        sql = "select * from T1 join T0 on T1.score = T0.score and T1.id = T0.id;";
        plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .optimize()
                .getBestPlanTree()
                .treeString();
        Assertions.assertEquals(plan,
                "PhysicalHashJoin ( type=INNER_JOIN, hashJoinCondition=[(score#1 = score#3), (id#0 = id#2)], otherJoinCondition=[], stats=(rows=1, width=2, penalty=0.0) )\n"
                        + "|--PhysicalOlapScan ( qualified=default_cluster:test.T1, output=[id#0, score#1], stats=(rows=1, width=1, penalty=0.0) )\n"
                        + "+--PhysicalOlapScan ( qualified=default_cluster:test.T0, output=[id#2, score#3], stats=(rows=1, width=1, penalty=0.0) )");
    }
}
