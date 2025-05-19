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

public class OperativeColumnDeriveTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int,sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.score (\n" + "sid int not null, \n" + "cid int not null, \n" + "grade double)\n"
                + "distributed by hash(sid,cid) buckets 10\n" + "properties('replication_num' = '1');");

        createTable("create table test.course (\n" + "cid int not null, \n" + "cname varchar(128), \n"
                + "teacher varchar(128))\n" + "distributed by hash(cid) buckets 10\n"
                + "properties('replication_num' = '1');");

        connectContext.setDatabase("test");
    }

    @Test
    public void testProject() {
        PlanChecker.from(connectContext)
                .analyze("select sid+1, grade as x from score")
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalProject(
                        logicalOlapScan().when(scan ->
                                scan.getOperativeSlots().size() == 1
                                        && scan.getOperativeSlots().get(0).getName().equals("sid"))
                ));
    }

    @Test
    public void testUnionAll() {
        PlanChecker.from(connectContext)
                .analyze("select a, b from (select sid as a, cid as b from score union all select id as a, age as b from student) T")
                .applyTopDown(new MergeProjectable())
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalUnion(
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().isEmpty())
                        ),
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().isEmpty())
                        )
                ));
    }

    @Test
    public void testUnionAllBackPropagate() {
        PlanChecker.from(connectContext)
                .analyze("select a, b from (select sid as a, cid as b from score where cid > 0 union all select id as a, age as b from student) T")
                .applyTopDown(new MergeProjectable())
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalUnion(
                        logicalProject(
                                logicalFilter(
                                        logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 1
                                                    && scan.getOperativeSlots().get(0).getName().equals("cid")
                                        ))
                        ),
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 1
                                            && scan.getOperativeSlots().get(0).getName().equals("age"))
                        )
                ));
    }
}
