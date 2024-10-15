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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class MarkJoinTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("CREATE TABLE `test_sq_dj1` (\n"
                + " `c1` varchar(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` int(20) not NULL,\n"
                + " `k4` bitmap BITMAP_UNION,\n"
                + " `k5` bitmap BITMAP_UNION\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `test_sq_dj2` (\n"
                + " `c1` bigint(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` bigint(20) not NULL,\n"
                + " `k4` bitmap BITMAP_UNION ,\n"
                + " `k5` bitmap BITMAP_UNION \n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `test_sq_dj3` (\n"
                + " `c1` bigint(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` bigint(20) not NULL,\n"
                + " `k4` bitmap BITMAP_UNION ,\n"
                + " `k5` bitmap BITMAP_UNION \n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    // grouping sets
    // grouping
    @Test
    public void test1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2) OR c1 < 10;");
    }

    @Test
    public void test2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 > (SELECT AVG(c1) FROM test_sq_dj2) OR c1 < 10;");
    }

    @Test
    public void test2_1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 > (SELECT AVG(c1) FROM test_sq_dj2 where test_sq_dj1.c1 = test_sq_dj2.c1);");
    }

    @Test
    public void test2_2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 > (SELECT AVG(c1) FROM test_sq_dj2 where test_sq_dj1.c1 = test_sq_dj2.c1) and c1 = 10;");
    }

    @Test
    public void test2_3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 = (SELECT AVG(c1) FROM test_sq_dj2 where test_sq_dj1.c1 = test_sq_dj2.c1) and c1 = 10;");
    }

    @Test
    public void test3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE EXISTS (SELECT c1 FROM test_sq_dj2 WHERE c1 = 10) OR c1 < 10");
    }

    @Test
    public void test4() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 left semi join test_sq_dj2 on test_sq_dj1.c1 = test_sq_dj2.c1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2) OR c1 < 10");
    }

    @Test
    public void test5() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1) OR c1 < 10");
    }

    /*
    // Not support binaryOperator children at least one is in or exists subquery
    @Test
    public void test6() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE (c1 IN (SELECT c1 FROM test_sq_dj2)) != true");
    }*/

    /*
    // Not support binaryOperator children at least one is in or exists subquery
    @Test
    public void test7() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE (c1 IN (SELECT c1 FROM test_sq_dj2) OR c1 < 10) != true");
    }*/

    @Test
    public void test8() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR exists (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)");
    }

    @Test
    public void test9() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1) "
                    + " AND c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1) "
                    + " AND EXISTS (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)");
    }

    @Test
    public void test10() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE (c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)) "
                    + " AND EXISTS (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)");
    }

    @Test
    public void test11() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 != (SELECT sum(c1) FROM test_sq_dj2) OR c1 < 10;");
    }

    @Test
    public void test12() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 != (SELECT sum(c1) FROM test_sq_dj2) and c1 = 1 OR c1 < 10;");
    }

    @Test
    public void test13() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE (c1 != (SELECT sum(c1) FROM test_sq_dj2) and c1 = 1 OR c1 < 10) and c1 = 10 and c1 = 15;");
    }

    @Test
    public void test14() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT CASE\n"
                    + "            WHEN (\n"
                    + "                SELECT COUNT(*) / 2\n"
                    + "                FROM test_sq_dj2\n"
                    + "            ) > c1 THEN (\n"
                    + "                SELECT AVG(c1)\n"
                    + "                FROM test_sq_dj2\n"
                    + "            )\n"
                    + "            ELSE (\n"
                    + "                SELECT SUM(c2)\n"
                    + "                FROM test_sq_dj2\n"
                    + "            )\n"
                    + "            END AS kk4\n"
                    + "        FROM test_sq_dj2 ;");
    }

    @Test
    public void test14_1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT CASE\n"
                    + "            WHEN  exists (\n"
                    + "                SELECT COUNT(*) / 2\n"
                    + "                FROM test_sq_dj2\n"
                    + "            ) THEN (\n"
                    + "                SELECT AVG(c1)\n"
                    + "                FROM test_sq_dj2\n"
                    + "            )\n"
                    + "            ELSE (\n"
                    + "                SELECT SUM(c2)\n"
                    + "                FROM test_sq_dj2\n"
                    + "            )\n"
                    + "            END AS kk4\n"
                    + "        FROM test_sq_dj2 ;");
    }

    @Test
    public void test15() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 != (SELECT sum(c1) FROM test_sq_dj2 where test_sq_dj1.c1 = test_sq_dj2.c1) and c1 = 10 and c1 = 15;");
    }

    @Test
    public void test16() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR exists (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)");
    }

    @Test
    public void test17() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE ((c1 != (SELECT sum(c1) FROM test_sq_dj2) and c1 = 1 OR c1 < 10) and c1 = 10 and c1 = 15)"
                    + " and (c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1))"
                    + " and exists (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1);");
    }

    @Test
    public void test17_1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE ((c1 != (SELECT sum(c1) FROM test_sq_dj2) and c1 = 1 OR c1 < 10) and c1 = 10)"
                    + " and (c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1))"
                    + " and exists (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1);");
    }

    @Test
    public void test18() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select * from test_sq_dj1 where test_sq_dj1.c1 != (select sum(c1) from test_sq_dj2 where test_sq_dj2.c3 = test_sq_dj1.c3) or c1 > 10");
    }

    @Test
    public void test19() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 IN (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " OR c1 < (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)"
                    + " AND exists (SELECT c1 FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)");
    }

    @Test
    public void test20() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 < (cast('1.2' as decimal(2,1)) * (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1))");
    }

    @Test
    public void test21() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 < (cast('1.2' as decimal(2,1)) * (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)) or c1 > 10");
    }

    @Test
    public void test22() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c1 != (cast('1.2' as decimal(2,1)) * (SELECT sum(c1) FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1)) or c1 > 10");
    }

    @Test
    public void test23() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c2 in (select k4 from test_sq_dj2)");
    }

    @Test
    public void test24() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE c3 in (select k4 from test_sq_dj2)");
    }

    @Test
    public void test25() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select * from test_sq_dj1 where c1 in (select c1 from test_sq_dj1 where c2 in (select c2 from test_sq_dj2) and c2 > (select sum(c1) from test_sq_dj2))");
    }

    @Test
    public void test26() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE exists (SELECT * FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1 and test_sq_dj2.c1 = 1)"
                    + " and (exists (SELECT * FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1 and test_sq_dj2.c1 = 2)"
                    + " or exists (SELECT * FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1 and test_sq_dj2.c1 = 3))");
    }

    @Test
    public void test27() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE exists (SELECT * FROM test_sq_dj2 WHERE test_sq_dj1.c1 = test_sq_dj2.c1 and test_sq_dj2.c1 = 1)"
                    + " and (exists (SELECT * FROM test_sq_dj2, test_sq_dj3 WHERE test_sq_dj1.c1 = test_sq_dj2.c1 and test_sq_dj2.c1 = test_sq_dj3.c1 and test_sq_dj2.c1 = 2)"
                    + " or exists (SELECT * FROM test_sq_dj2, test_sq_dj3 WHERE test_sq_dj1.c1 = test_sq_dj2.c1 and test_sq_dj2.c1 = test_sq_dj3.c1 and test_sq_dj2.c1 = 3))");
    }

    @Test
    public void test28() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT * FROM test_sq_dj1 WHERE (c1 between (SELECT distinct c2+1 FROM test_sq_dj2 WHERE test_sq_dj2.c1 = 1) and (select distinct (c2 + 3) from test_sq_dj2 where test_sq_dj2.c1 = 1))");
    }
}
