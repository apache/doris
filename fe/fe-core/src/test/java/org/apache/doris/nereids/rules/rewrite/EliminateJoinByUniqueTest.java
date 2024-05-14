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

class EliminateJoinByUniqueTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id1 int not null,\n"
                        + "    id_null int\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t2 (\n"
                        + "    id2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id2)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n");
        addConstraint("Alter table t1 add constraint uk_t1 unique (id1)");
        addConstraint("Alter table t1 add constraint id_null unique (id_null)");
        addConstraint("Alter table t2 add constraint uk_t2 unique (id2)");
    }

    @Test
    void testNotNull() throws Exception {
        String sql = "select t1.id1 from t1 left outer join t2 on t1.id1 = t2.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();

        sql = "select t2.id2 from t1 left outer join t2 on t1.id1 = t2.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin())
                .printlnTree();
    }

    @Test
    void testNull() throws Exception {
        String sql = "select t1.id1 from t1 left outer join t2 on t1.id_null = t2.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();

        sql = "select t2.id2 from t1 left outer join t2 on t1.id_null = t2.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin())
                .printlnTree();
    }
}
