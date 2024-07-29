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

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class PullUpJoinFromUnionTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id int not null,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t2 (\n"
                        + "    id int not null,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t3 (\n"
                        + "    id int,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testSimple() {
        String sql = "select * from t1 join t2 on t1.id = t2.id "
                + "union all "
                + "select * from t1 join t3 on t1.id = t3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));
    }

    @Test
    void testProject() {
        String sql = "select t2.id from t1 join t2 on t1.id = t2.id "
                + "union all "
                + "select t3.id from t1 join t3 on t1.id = t3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));

        sql = "select t2.id, t1.name from t1 join t2 on t1.id = t2.id "
                + "union all "
                + "select t3.id, t1.name from t1 join t3 on t1.id = t3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));
    }

    @Test
    void testConstant() {
        String sql = "select t2.id, t1.name, 1 as id1 from t1 join t2 on t1.id = t2.id "
                + "union all "
                + "select t3.id, t1.name, 2 as id2 from t1 join t3 on t1.id = t3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));
    }

    @Test
    void testComplexProject() {
        String sql = "select t2.id + 1, t1.name + 1, 1 as id1 from t1 join t2 on t1.id = t2.id "
                + "union all "
                + "select t3.id + 1, t1.name + 1, 2 as id2 from t1 join t3 on t1.id = t3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalUnion(), any()));
    }

    @Test
    void testMissJoinSlot() {
        String sql = "select t1.name + 1, 1 as id1 from t1 join t2 on t1.id = t2.id "
                + "union all "
                + "select t1.name + 1, 2 as id2 from t1 join t3 on t1.id = t3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalUnion(), any()));
    }

    @Test
    void testFilter() {
        String sql = "select * from t1 join t2 on t1.id = t2.id where t1.name = '' "
                + "union all "
                + "select * from t1 join t3 on t1.id = t3.id where t1.name = '' ;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));

        sql = "select t2.id from t1 join t2 on t1.id = t2.id where t1.name = '' "
                + "union all "
                + "select t3.id from t1 join t3 on t1.id = t3.id where t1.name = '' ;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));
    }

    @Test
    void testMultipleJoinConditions() {
        String sql = "select * from t1 join t2 on t1.id = t2.id and t1.name = t2.name "
                + "union all "
                + "select * from t1 join t3 on t1.id = t3.id and t1.name = t3.name;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));
    }

    @Test
    void testNonEqualityJoinConditions() {
        String sql = "select * from t1 join t2 on t1.id < t2.id "
                + "union all "
                + "select * from t1 join t3 on t1.id < t3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin(logicalProject(logicalUnion()), any()));
    }

    @Test
    void testSubqueries() {
        String sql = "select * from t1 join (select * from t2 where t2.id > 10) s2 on t1.id = s2.id "
                + "union all "
                + "select * from t1 join (select * from t3 where t3.id > 10) s3 on t1.id = s3.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalUnion()), any()));
    }
}
