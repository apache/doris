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

class PushDownAggThroughJoinOnPkFkTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS pri (\n"
                        + "    id1 int not null,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_not_null (\n"
                        + "    id2 int not null,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id2)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_null (\n"
                        + "    id3 int,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id3)\n"
                        + "DISTRIBUTED BY HASH(id3) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
        addConstraint("Alter table pri add constraint pk primary key (id1)");
        addConstraint("Alter table foreign_not_null add constraint f_not_null foreign key (id2)\n"
                + "references pri(id1)");
        addConstraint("Alter table foreign_null add constraint f_not_null foreign key (id3)\n"
                + "references pri(id1)");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testGroupByFk() {
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkAndOther() {
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, foreign_not_null.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalAggregate()), any()))
                .printlnTree();
        sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
        sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name, foreign_not_null.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalAggregate()), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkWithCount() {
        String sql = "select count(pri.id1) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
        sql = "select count(foreign_not_null.id2) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkWithForeigAgg() {
        String sql = "select sum(foreign_not_null.id2) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkWithPrimaryAgg() {
        String sql = "select sum(pri.id1) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalAggregate(logicalProject(logicalJoin())))
                .printlnTree();
    }

    @Test
    void testMultiJoin() {
        String sql = "select count(pri.id1), pri.name from foreign_not_null inner join foreign_null on foreign_null.name = foreign_not_null.name\n"
                + " inner join pri on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();

        sql = "select count(pri.id1), pri.name from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "inner join foreign_null on foreign_null.name = foreign_not_null.name\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testMissSlot() {
        String sql = "select count(pri.name) from pri inner join foreign_not_null on pri.name = foreign_not_null.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalAggregate(logicalProject(logicalJoin())))
                .printlnTree();
    }
}
