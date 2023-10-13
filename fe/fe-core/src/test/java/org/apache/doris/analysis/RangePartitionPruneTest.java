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

package org.apache.doris.analysis;

import org.apache.doris.common.FeConstants;

import org.junit.jupiter.api.Test;

public class RangePartitionPruneTest extends PartitionPruneTestBase {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");

        String singleColumnPartitionTable =
                "CREATE TABLE `test`.`t1` (\n"
                        + "  `dt` int(11) NULL COMMENT \"\",\n"
                        + "  `k1` int(11) NULL COMMENT \"\",\n"
                        + "  `k2` int(11) NULL COMMENT \"\",\n"
                        + "  `k3` int(11) NULL COMMENT \"\",\n"
                        + "  `k4` int(11) NULL COMMENT \"\"\n"
                        + ") "
                        + "DUPLICATE KEY(`dt`, `k1`, `k2`, `k3`, `k4`)\n"
                        + "PARTITION BY RANGE(`dt`)\n"
                        + "(PARTITION p20211121 VALUES LESS THAN (\"20211121\"),\n"
                        + "PARTITION p20211122 VALUES [(\"20211121\"), (\"20211122\")),\n"
                        + "PARTITION p20211123 VALUES [(\"20211122\"), (\"20211123\")),\n"
                        + "PARTITION p20211124 VALUES [(\"20211123\"), (\"20211124\")),\n"
                        + "PARTITION p20211125 VALUES [(\"20211124\"), (\"20211125\")),\n"
                        + "PARTITION p20211126 VALUES [(\"20211125\"), (\"20211126\")),\n"
                        + "PARTITION p20211127 VALUES [(\"20211126\"), (\"20211127\")),\n"
                        + "PARTITION p20211128 VALUES [(\"20211127\"), (\"20211128\")))\n"
                        + "DISTRIBUTED BY HASH(`k1`) BUCKETS 60\n"
                        + "PROPERTIES('replication_num' = '1');";

        String notNullSingleColumnPartitionTable =
                "CREATE TABLE `test`.`single_not_null` (\n"
                        + "  `dt` int(11) NULL COMMENT \"\",\n"
                        + "  `k1` int(11) NULL COMMENT \"\",\n"
                        + "  `k2` int(11) NULL COMMENT \"\",\n"
                        + "  `k3` int(11) NULL COMMENT \"\",\n"
                        + "  `k4` int(11) NULL COMMENT \"\"\n"
                        + ") "
                        + "DUPLICATE KEY(`dt`, `k1`, `k2`, `k3`, `k4`)\n"
                        + "PARTITION BY RANGE(`dt`)\n"
                        + "(PARTITION p20211122 VALUES [(\"20211121\"), (\"20211122\")),\n"
                        + "PARTITION p20211123 VALUES [(\"20211122\"), (\"20211123\")),\n"
                        + "PARTITION p20211124 VALUES [(\"20211123\"), (\"20211124\")),\n"
                        + "PARTITION p20211125 VALUES [(\"20211124\"), (\"20211125\")),\n"
                        + "PARTITION p20211126 VALUES [(\"20211125\"), (\"20211126\")),\n"
                        + "PARTITION p20211127 VALUES [(\"20211126\"), (\"20211127\")),\n"
                        + "PARTITION p20211128 VALUES [(\"20211127\"), (\"20211128\")))\n"
                        + "DISTRIBUTED BY HASH(`k1`) BUCKETS 60\n"
                        + "PROPERTIES('replication_num' = '1');";

        String multipleColumnsPartitionTable =
                "CREATE TABLE `test`.`t2` (\n"
                        + "  `k1` int(11) NULL COMMENT \"\",\n"
                        + "  `k2` int(11) NULL COMMENT \"\",\n"
                        + "  `k3` int(11) NULL COMMENT \"\",\n"
                        + "  `k4` int(11) NULL COMMENT \"\",\n"
                        + "  `k5` int(11) NULL COMMENT \"\"\n"
                        + ") \n"
                        + "PARTITION BY RANGE(`k1`, `k2`)\n"
                        + "(PARTITION p1 VALUES LESS THAN (\"3\", \"1\"),\n"
                        + "PARTITION p2 VALUES [(\"3\", \"1\"), (\"7\", \"10\")),\n"
                        + "PARTITION p3 VALUES [(\"7\", \"10\"), (\"8\", \"5\")),\n"
                        + "PARTITION p4 VALUES [(\"10\", \"10\"), (\"12\", \"5\")),\n"
                        + "PARTITION p5 VALUES [(\"15\", \"6\"), (\"20\", \"11\")),\n"
                        + "PARTITION p6 VALUES [(\"20\", \"11\"), (\"22\", \"3\")),\n"
                        + "PARTITION p7 VALUES [(\"23\", \"3\"), (\"23\", \"4\")),\n"
                        + "PARTITION p8 VALUES [(\"23\", \"4\"), (\"23\", \"20\")),\n"
                        + "PARTITION p9 VALUES [(\"24\", \"1\"), (\"25\", \"9\")))\n"
                        + "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n"
                        + "PROPERTIES ('replication_num' = '1');";

        String notNullMultipleColumnsPartitionTable =
                "CREATE TABLE `test`.`multi_not_null` (\n"
                        + "  `k1` int(11) NULL COMMENT \"\",\n"
                        + "  `k2` int(11) NULL COMMENT \"\",\n"
                        + "  `k3` int(11) NULL COMMENT \"\",\n"
                        + "  `k4` int(11) NULL COMMENT \"\",\n"
                        + "  `k5` int(11) NULL COMMENT \"\"\n"
                        + ") \n"
                        + "PARTITION BY RANGE(`k1`, `k2`)\n"
                        + "(PARTITION p1 VALUES [(\"3\", \"1\"), (\"3\", \"3\")),\n"
                        + "PARTITION p2 VALUES [(\"4\", \"2\"), (\"4\", \"6\")))\n"
                        + "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n"
                        + "PROPERTIES ('replication_num' = '1');";

        String autoCreatePartitionTable = new String("CREATE TABLE test.test_to_date_trunc(\n"
                + "    event_day DATETIME\n"
                + ")\n"
                + "DUPLICATE KEY(event_day)\n"
                + "AUTO PARTITION BY range date_trunc(event_day, \"day\") (\n"
                + "\tPARTITION `p20230807` values [(20230807 ), (20230808 )),\n"
                + "\tPARTITION `p20020106` values [(20020106 ), (20020107 ))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day) BUCKETS 4\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");
        createTables(singleColumnPartitionTable,
                notNullSingleColumnPartitionTable,
                multipleColumnsPartitionTable,
                notNullMultipleColumnsPartitionTable,
                autoCreatePartitionTable);
    }

    private void initTestCases() {
        // 1. Single partition column
        // no filters
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1", "partitions=8/8");
        // equal to
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt=20211122", "partitions=1/8");
        // less than
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt<20211122", "partitions=2/8");
        // less than or equal
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt<=20211122", "partitions=3/8");
        // greater than
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt>20211122", "partitions=6/8");
        // greater than or equal
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt>=20211122", "partitions=6/8");
        // in
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt in (20211124, 20211126, 20211122)", "partitions=3/8");
        // is null
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt is null", "partitions=1/8");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.`single_not_null` where dt is null", "partitions=0/7");
        // not equal to
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt!=20211122", "partitions=8/8");

        // 2. Multiple partition columns
        // no filters
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2", "partitions=9/9");
        // equal to
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1=7", "partitions=2/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k2=7", "partitions=9/9");
        // less than
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1<7", "partitions=2/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k2<7", "partitions=9/9");
        // less than or equal
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1<=7", "partitions=3/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k2>7", "partitions=9/9");
        // greater than or equal
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1>=7", "partitions=8/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k2>=7", "partitions=9/9");
        // in
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 in (7,9,16)", "partitions=3/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k2 in (7,9,16)", "partitions=9/9");
        // is null
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 is null", "partitions=1/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k2 is null", "partitions=9/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.multi_not_null where k1 is null", "partitions=0/2");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.multi_not_null where k2 is null", "partitions=2/2");
        // not equal to
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1!=23", "partitions=9/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k2!=23", "partitions=9/9");

        // 3. Conjunctive predicates
        // equal to and other predicates
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1=23 and k2=5", "partitions=1/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1=23 and k2>5", "partitions=1/9");
        // in and other equal predicates
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 in (3, 10, 13) and k2>10", "partitions=2/9");
        // is null and other predicates
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 > 10 and k1 is null", "partitions=0/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 is null and k1 > 10", "partitions=0/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.multi_not_null where k1 > 10 and k1 is null", "partitions=0/2");
        // others predicates combination
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 > 10 and k2 < 4", "partitions=6/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 >10 and k1 < 10 and (k1=11 or k1=12)", "partitions=0/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 > 20 and k1 < 7 and k1 = 10", "partitions=0/9");

        // 4. Disjunctive predicates
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1=10 or k1=23", "partitions=3/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where (k1=10 or k1=23) and (k2=4 or k2=5)", "partitions=1/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where (k1=10 or k1=23) and (k2=4 or k2=11)", "partitions=2/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where (k1=10 or k1=23) and (k2=3 or k2=4 or k2=11)", "partitions=3/9");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where dt=20211123 or dt=20211124", "partitions=2/8");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where ((dt=20211123 and k1=1) or (dt=20211125 and k1=3))", "partitions=2/8");
        // TODO: predicates are "PREDICATES: ((`dt` = 20211123 AND `k1` = 1) OR (`dt` = 20211125 AND `k1` = 3)), `k2` > ",
        // maybe something goes wrong with ExtractCommonFactorsRule.
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where ((dt=20211123 and k1=1) or (dt=20211125 and k1=3)) and k2>0",
                "partitions=2/8");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t2 where k1 > 10 or k2 < 1", "partitions=9/9");
        // add some cases for CompoundPredicate
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where (dt >= 20211121 and dt <= 20211122) or (dt >= 20211123 and dt <= 20211125)",
                "partitions=5/8");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where (dt between 20211121 and 20211122) or (dt between 20211123 and 20211125)",
                "partitions=5/8");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.t1 where (dt between 20211121 and 20211122) or dt is null or (dt between 20211123 and 20211125)",
                "partitions=6/8");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test_to_date_trunc where event_day= \"2023-08-07 11:00:00\" ",
                "partitions=1/2");
        addCase("select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test_to_date_trunc where date_trunc(event_day, \"day\")= \"2023-08-07 11:00:00\" ",
                "partitions=1/2");

    }


    @Test
    public void testPartitionPrune() throws Exception {
        initTestCases();
        doTest();
    }
}
