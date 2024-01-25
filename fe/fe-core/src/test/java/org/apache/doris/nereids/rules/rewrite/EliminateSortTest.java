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

/**
 * column prune ut.
 */
class EliminateSortTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int,sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
    }

    @Test
    void test() {
        PlanChecker.from(connectContext)
                .analyze("select * from student order by id")
                .rewrite()
                .matches(logicalSort());
        PlanChecker.from(connectContext)
                .analyze("select count(*) from (select * from student order by id) t")
                .rewrite()
                .nonMatch(logicalSort());
    }

    @Test
    void testSortLimit() {
        PlanChecker.from(connectContext)
                .analyze("select count(*) from (select * from student order by id) t limit 1")
                .rewrite()
                .nonMatch(logicalTopN());
        PlanChecker.from(connectContext)
                .analyze("select count(*) from (select * from student order by id limit 1) t")
                .rewrite()
                .matches(logicalTopN());

        PlanChecker.from(connectContext)
                .analyze("select count(*) from "
                        + "(select * from student order by id limit 1) t1 left join student t2 on t1.id = t2.id")
                .rewrite()
                .matches(logicalTopN());
        PlanChecker.from(connectContext)
                .analyze("select count(*) from "
                        + "(select * from student order by id) t1 left join student t2 on t1.id = t2.id limit 1")
                .rewrite()
                .nonMatch(logicalTopN());
    }
}
