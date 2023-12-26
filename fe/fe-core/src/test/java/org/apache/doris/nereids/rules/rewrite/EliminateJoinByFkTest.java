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

class EliminateJoinByFkTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS pri (\n"
                        + "    id1 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_not_null (\n"
                        + "    id2 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id2)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_null (\n"
                        + "    id3 int\n"
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
    }

    @Test
    void testNotNull() throws Exception {
        addConstraint("Alter table foreign_not_null add constraint uk1 unique (id2)\n");
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();
        dropConstraint("Alter table foreign_not_null drop constraint uk1\n");
    }

    @Test
    void testNotNullWithPredicate() throws Exception {
        addConstraint("Alter table foreign_not_null add constraint uk2 unique (id2)\n");
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "where pri.id1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();
        dropConstraint("Alter table foreign_not_null drop constraint uk2\n");
    }

    @Test
    void testNull() throws Exception {
        addConstraint("Alter table foreign_null add constraint uk unique (id3)\n");
        String sql = "select pri.id1 from pri inner join foreign_null on pri.id1 = foreign_null.id3";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();
        dropConstraint("Alter table foreign_null drop constraint uk\n");
    }

    @Test
    void testNullWithPredicate() throws Exception {
        addConstraint("Alter table foreign_null add constraint uk unique (id3)\n");
        String sql = "select pri.id1 from pri inner join foreign_null on pri.id1 = foreign_null.id3\n"
                + "where pri.id1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();
        dropConstraint("Alter table foreign_null drop constraint uk\n");
    }

    @Test
    void testMultiJoin() throws Exception {
        addConstraint("Alter table foreign_null add constraint uk_id3 unique (id3)\n");
        addConstraint("Alter table foreign_not_null add constraint uk_id2 unique (id2)\n");
        String sql = "select id1 from "
                + "foreign_null inner join foreign_not_null on id2 = id3\n"
                + "inner join pri on id1 = id3";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalOlapScan().when(scan -> scan.getTable().getName().equals("pri")))
                .printlnTree();
        dropConstraint("Alter table foreign_null drop constraint uk_id3\n");
        dropConstraint("Alter table foreign_not_null drop constraint uk_id2");
    }
}
