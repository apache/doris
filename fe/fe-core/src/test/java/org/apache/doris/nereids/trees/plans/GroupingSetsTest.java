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

public class GroupingSetsTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("CREATE TABLE `t1` (\n"
                + " `k1` bigint(20) NULL,\n"
                + " `k2` bigint(20) NULL,\n"
                + " `k3` bigint(20) not NULL,\n"
                + " `k4` bigint(20) not NULL,\n"
                + " `k5` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 1\n"
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
    public void testGrouping1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1+1, grouping(k1+1) from t1 group by grouping sets((k1+1));");
    }

    @Test
    public void testGrouping2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1+1, grouping(k1) from t1 group by grouping sets((k1));");
    }

    @Test
    public void testGrouping3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2), grouping(k1) from t1 group by grouping sets((k1));");
    }

    @Test
    public void testGrouping4() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2+1), grouping(k1+1) from t1 group by grouping sets((k1+1));");
    }

    @Test
    public void testGrouping5() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2+1), grouping(k1+1) from t1 group by grouping sets((k1+1), (k1));");
    }

    @Test
    public void testGroupingWithHaving() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2+1), grouping(k1+1) from t1 group by grouping sets((k1+1)) having (k1+1) > 1;");
    }

    //grouping_id
    @Test
    public void testGroupingId1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1+1, grouping_id(k1+1) from t1 group by grouping sets((k1+1));");
    }

    @Test
    public void testGroupingId2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1+1, grouping_id(k1, k2) from t1 group by grouping sets((k1), (k2));");
    }

    @Test
    public void testGroupingId3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2), grouping_id(k1) from t1 group by grouping sets((k1));");
    }

    @Test
    public void testGroupingId4() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2+1), grouping_id(k1+1) from t1 group by grouping sets((k1+1));");
    }

    @Test
    public void testGroupingId5() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2+1), grouping_id(k1+1) from t1 group by grouping sets((k1+1), (k1));");
    }

    @Test
    public void testGroupingIdWithHaving() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2+1), grouping_id(k1+1) from t1 group by grouping sets((k1+1), (k1)) having (k1+1) > 1;");
    }

    @Test
    public void testGrouping6() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1+1, sum(k2+1), grouping(k1) from t1 group by grouping sets((k1))");
    }

    @Test
    public void testGrouping7() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select sum(k2) from t1 group by grouping sets((k1, k3, k4), (k3), (k4, k5));");
    }

    @Test
    public void testGroupingNullable() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k3, k4 from t1 group by grouping sets((k1, k3, k4), (k2))");
    }

    @Test
    public void testAliasGrouping() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select (k1 + 1) k1_, k2, sum(k3) from t1 group by grouping sets((k1_, k2)) order by k1_, k2;");
    }

    @Test
    public void testLiteralGrouping() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k2, sum(k3) from t1 group by cube(1, k2) order by k2;");
    }

    @Test
    public void testLiteralAliasGrouping() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select 1 as k1_, k2, sum(k3) from t1 group by cube(k1_, k2) order by k1_, k2;");
    }

    @Test
    public void testSubqueryAliasGrouping() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1_, k2_, sum(k3_) from (select (k1 + 1) k1_, k2 k2_, k3 k3_ from t1) as test"
                        + " group by grouping sets((k1_, k2_), (k2_)) order by k1_, k2_;");
    }

    @Test
    public void testIfGrouping() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select if(k1 = 1, 2, k1) k_if, k1, sum(k2) k2_sum from t1 where k3 is null or k2 = 1\n"
                        + " group by grouping sets((k_if, k1),()) order by k_if, k1, k2_sum");
    }

    @Test
    public void testNotNullGrouping() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k3, sum(k2) from "
                        + "(select k3, k2, grouping(k1), grouping(k2) from t1 group by grouping sets((k1), (k2), (k3)))a group by k3");
    }

    @Test
    public void test() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select if(k1 = 1, 2, k1) k_if from t1");
    }

    @Test
    public void test1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select coalesce(col1, 'all') as col1, count(*) as cnt from"
                        + " (select null as col1 union all select 'a' as col1 ) t group by grouping sets ((col1),());");
    }

    @Test
    public void test1_1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select coalesce(col1, 'all') as col2, count(*) as cnt from"
                        + " (select null as col1 union all select 'a' as col1 ) t group by grouping sets ((col1),());");
    }

    @Test
    public void test1_2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select coalesce(col1, 'all') as col2, count(*) as cnt from"
                        + " (select null as col1 union all select 'a' as col1 ) t group by grouping sets ((col2),());");
    }

    @Test
    public void test2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select if(1 = null, 'all', 2) as col1, count(*) as cnt from"
                        + " (select null as col1 union all select 'a' as col1 ) t group by grouping sets ((col1),());");
    }

    @Test
    public void test2_1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select if(col1 = null, 'all', 2) as col1, count(*) as cnt from"
                        + " (select null as col1 union all select 'a' as col1 ) t group by grouping sets ((col1),());");
    }

    @Test
    public void test2_2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select if(col1 = null, 'all', 2) as col2, count(*) as cnt from"
                        + " (select null as col1 union all select 'a' as col1 ) t group by grouping sets ((col1),());");
    }

    @Test
    public void test2_3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select if(col1 = null, 'all', 2) as col2, count(*) as cnt from"
                        + " (select null as col1 union all select 'a' as col1 ) t group by grouping sets ((col2),());");
    }
}
