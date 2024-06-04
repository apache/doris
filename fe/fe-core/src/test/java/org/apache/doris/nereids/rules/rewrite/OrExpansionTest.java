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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OrExpansionTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id1 int not null,\n"
                        + "    id2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t2 (\n"
                        + "    id1 int not null,\n"
                        + "    id2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
    }

    @Test
    void testOrExpand() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select t1.id1 + 1 as id from t1 join t2 on t1.id1 = t2.id1 or t1.id2 = t2.id2";
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .getPlan();
        Assertions.assertTrue(plan instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1) instanceof LogicalCTEAnchor);
    }

    @Test
    void testOrExpandCTE() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().inlineCTEReferencedThreshold = 0;
        String sql = "with t3 as (select t1.id1 + 1 as id1, t1.id2 + 2 as id2 from t1), "
                + "t4 as (select t2.id1 + 1 as id1, t2.id2 + 2 as id2  from t2) "
                + "select t3.id1 from "
                + "(select id1, id2 from t3 group by id1, id2) t3 "
                + " join "
                + "(select id1, id2 from t4 group by id1, id2) t4  "
                + "on t3.id1 = t4.id1 or t3.id2 = t4.id2";
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .getPlan();
        Assertions.assertTrue(plan instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1) instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1).child(1) instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1).child(1).anyMatch(x -> x instanceof LogicalCTEConsumer));
        Assertions.assertTrue(plan.child(1).child(1).child(1) instanceof LogicalCTEAnchor);
        Assertions.assertTrue(plan.child(1).child(1).child(1)
                .anyMatch(x -> x instanceof LogicalCTEConsumer));
    }
}
