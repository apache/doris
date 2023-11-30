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

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PruneOlapScanPartitionTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("create table test_list_parts(id int, part int not null) "
                + "partition by list(part) ("
                + "  partition p1 (('1'), ('4'), ('7')),"
                + "  partition p2 (('8'), ('9'), ('5')),"
                + "  partition p3 (('11'), ('0'), ('6'))"
                + ") "
                + "distributed by hash(id) "
                + "properties ('replication_num'='1')");

        createTable("create table test_range_parts(id int, part int) "
                + "partition by range(part) ("
                + "  partition p1 values[('1'), ('2')),"
                + "  partition p2 values[('2'), ('3')),"
                + "  partition p3 values[('3'), ('4')),"
                + "  partition p4 values[('4'), ('5'))"
                + ") "
                + "distributed by hash(id) "
                + "properties ('replication_num'='1')");

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

        createTables(singleColumnPartitionTable,
                notNullSingleColumnPartitionTable,
                multipleColumnsPartitionTable,
                notNullMultipleColumnsPartitionTable);
        FeConstants.runningUnitTest = true;
    }

    @Test
    void testOlapScanPartitionWithSingleColumnCase() throws Exception {
        createTable("create table testOlapScanPartitionWithSingleColumnCase("
                + "  id int not null,"
                + "  col1 int not null"
                + "  ) "
                + "partition by range(col1) ("
                + "  partition p1 values[('0'), ('5')),"
                + "  partition p2 values[('5'), ('10'))"
                + ") "
                + "distributed by hash(id) "
                + "properties ('replication_num'='1')");

        test("testOlapScanPartitionWithSingleColumnCase", "col1 < 4", 1);
        test("testOlapScanPartitionWithSingleColumnCase", "col1 < 0 or col1 > 6", 1);
        test("testOlapScanPartitionWithSingleColumnCase", "col1 >= 0 and col1 <= 5", 2);
    }

    @Test
    void testOlapScanPartitionPruneWithMultiColumnCase() throws Exception {
        createTable("create table testOlapScanPartitionPruneWithMultiColumnCase("
                + "  id int not null,"
                + "  col1 int not null,"
                + "  col2 int not null"
                + "  ) "
                + "partition by range(col1, col2) ("
                + "  partition p1 values[('1', '10'), ('4', '5'))"
                + ") "
                + "distributed by hash(id) "
                + "properties ('replication_num'='1')");

        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 = 4", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 = 1", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 = 2", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 1", 0);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 >= 4", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 > 4", 0);

        test("testOlapScanPartitionPruneWithMultiColumnCase", "col2 = 10", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col2 = 5", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col2 = 100", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col2 = -1", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col2 < 10", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col2 >= 5", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col2 > 5", 1);

        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 4 and col2 > 11", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 2 and col2 > 11", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 2 and col2 <= 10", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 2 and col2 < 10", 0);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 2 and col2 <= 10", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 >= 4 and col2 > 10", 0);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 4 or col2 > 5", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 4 or col2 > 3", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 1 or col2 > 5", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 <= 1 or col2 >= 10", 1);

        test("testOlapScanPartitionPruneWithMultiColumnCase", "cast(col1 as bigint) = 1", 1);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "cast(col1 as bigint) = 5", 0);
        test("testOlapScanPartitionPruneWithMultiColumnCase", "cast(col1 as bigint) + 1 = 5", 1);
    }

    @Test
    public void prunePartitionWithDefaultPartition() throws Exception {
        createTable("CREATE TABLE IF NOT EXISTS test_default_in_parts (\n"
                + "            k1 tinyint NOT NULL, \n"
                + "            k2 smallint NOT NULL, \n"
                + "            k3 int NOT NULL, \n"
                + "            k4 bigint NOT NULL, \n"
                + "            k5 decimal(9, 3) NOT NULL,\n"
                + "            k8 double max NOT NULL, \n"
                + "            k9 float sum NOT NULL ) \n"
                + "        AGGREGATE KEY(k1,k2,k3,k4,k5)\n"
                + "        PARTITION BY LIST(k1) ( \n"
                + "            PARTITION p1 VALUES IN (\"1\",\"2\",\"3\",\"4\"), \n"
                + "            PARTITION p2 VALUES IN (\"5\",\"6\",\"7\",\"8\"), \n"
                + "            PARTITION p3 ) \n"
                + "        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties(\"replication_num\" = \"1\")");
        test("test_default_in_parts", "k1 = 10", 1);
        test("test_default_in_parts", "k1 > 5", 2);
        test("test_default_in_parts", "k1 > 2", 3);
        test("test_default_in_parts", "(k1 > 1 and k1 < 8)", 3);
    }

    @Test
    public void prunePartitionWithOrPredicate() {
        test("test_list_parts", "(part = 9 and id <= 500) or (part = 3)", 1);
        test("test_range_parts", "(part = 1 and id <= 500) or (part = 3)", 2);
    }

    @Test
    public void canNotPruneComplexPredicate() {
        test("test_range_parts", "(part = 10) or (part + id = 1)", 4);
        test("test_range_parts", "(part + id = 1) and (part = 4)", 1);
        test("test_range_parts", "(part = 2) and (part <> id)", 1);
        test("test_range_parts", "(part = 2) or (part <> id)", 4);
    }

    @Test
    public void pruneMultiColumnListPartition() throws Exception {
        createTable("create table test_multi_list_parts(id int, part1 int not null, part2 varchar(32) not null) "
                + "partition by list(part1, part2) ("
                + "  partition p1 (('1', 'd'), ('3', 'a')),"
                + "  partition p2 (('4', 'c'), ('6', 'f'))"
                + ") "
                + "distributed by hash(id) "
                + "properties ('replication_num'='1')");

        test("test_multi_list_parts", "part1 = 1 and part2 < 'd'", 0);
    }

    @Test
    void testOlapScanPartitionPruneWithNonEnumerableRange() throws Exception {
        String tableName = "testOlapScanPartitionPruneWithNonEnumerableRange";
        createTable("create table " + tableName + "("
                + "  id int not null,"
                + "  col1 datetime not null"
                + "  ) "
                + "partition by range(col1) ("
                + "  partition p1 values[('2023-03-21 00:00:00'), ('2023-03-21 23:59:59'))"
                + ") "
                + "distributed by hash(id) "
                + "properties ('replication_num'='1')");

        test(tableName, "date(col1) = '2023-03-21'", 1);
        test(tableName, "date(col1) = '2023-03-22'", 0);
    }

    @Test
    void testMaxValue() throws Exception {
        createTable("CREATE TABLE IF NOT EXISTS `test_basic_agg` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\",\n"
                + "  `k3` int(11) NULL COMMENT \"\",\n"
                + "  `k4` bigint(20) NULL COMMENT \"\",\n"
                + "  `k5` decimal(9, 3) NULL COMMENT \"\",\n"
                + "  `k6` char(5) NULL COMMENT \"\",\n"
                + "  `k10` date NULL COMMENT \"\",\n"
                + "  `k11` datetime NULL COMMENT \"\",\n"
                + "  `k7` varchar(20) NULL COMMENT \"\",\n"
                + "  `k8` double MAX NULL COMMENT \"\",\n"
                + "  `k9` float SUM NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "(PARTITION p1 VALUES [(\"-128\"), (\"-64\")),\n"
                + "PARTITION p2 VALUES [(\"-64\"), (\"0\")),\n"
                + "PARTITION p3 VALUES [(\"0\"), (\"64\")),\n"
                + "PARTITION p4 VALUES [(\"64\"), (MAXVALUE)))\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ");");

        // TODO: support like function to prune partition
        test("test_basic_agg", "  1998  like '1%'", 4);
        test("test_basic_agg", " '1998' like '1%'", 4);
        test("test_basic_agg", "  2998  like '1%'", 4);
        test("test_basic_agg", " '2998' like '1%'", 4);
        test("test_basic_agg", " 199.8  like '1%'", 4);
        test("test_basic_agg", "'199.8' like '1%'", 4);
        test("test_basic_agg", " 299.8  like '1%'", 4);
        test("test_basic_agg", "'299.8' like '1%'", 4);
    }

    @Test
    void legacyTests() {
        // 1. Single partition column
        // no filters
        test("t1", "", 8);
        // equal to
        test("t1", "dt=20211122", 1);
        // less than
        test("t1", "dt<20211122", 2);
        // less than or equal
        test("t1", "dt<=20211122", 3);
        // greater than
        test("t1", "dt>20211122", 5); // legacy return 6
        // greater than or equal
        test("t1", "dt>=20211122", 6);
        // in
        test("t1", "dt in (20211124, 20211126, 20211122)", 3);
        // is null
        test("t1", "dt is null", 1);
        test("`single_not_null`", "dt is null", 0);
        // not equal to
        test("t1", "dt!=20211122", 7); //legacy return 8

        // 2. Multiple partition columns
        // no filters
        test("t2", "", 9);
        // equal to
        test("t2", "k1=7", 2);
        test("t2", "k2=7", 7); // legacy return 9
        // less than
        test("t2", "k1<7", 2);
        test("t2", "k2<7", 9);
        // less than or equal
        test("t2", "k1<=7", 3);
        test("t2", "k2>7", 8); // legacy return 9
        // greater than or equal
        test("t2", "k1>=7", 8);
        test("t2", "k2>=7", 8); // legacy return 9
        // in
        test("t2", "k1 in (7,9,16)", 3);
        test("t2", "k2 in (7,9,16)", 8); // legacy return 9
        // is null
        test("t2", "k1 is null", 1);
        test("t2", "k2 is null", 7); // legacy return 9
        test("multi_not_null", "k1 is null", 0);
        test("multi_not_null", "k2 is null", 0); // legacy return 2
        // not equal to
        test("t2", "k1!=23", 7); // legacy return 9
        test("t2", "k2!=23", 9);

        // 3. Conjunctive predicates
        // equal to and other predicates
        test("t2", "k1=23 and k2=5", 1);
        test("t2", "k1=23 and k2>5", 1);
        // in and other equal predicates
        test("t2", "k1 in (3, 10, 13) and k2>10", 2);
        // is null and other predicates
        test("t2", "k1 > 10 and k1 is null", 0);
        test("t2", "k1 is null and k1 > 10", 0);
        test("multi_not_null", "k1 > 10 and k1 is null", 0);
        // others predicates combination
        test("t2", "k1 > 10 and k2 < 4", 5); // legacy return 6
        test("t2", "k1 >10 and k1 < 10 and (k1=11 or k1=12)", 0);
        test("t2", "k1 > 20 and k1 < 7 and k1 = 10", 0);

        // 4. Disjunctive predicates
        test("t2", "k1=10 or k1=23", 3);
        test("t2", "(k1=10 or k1=23) and (k2=4 or k2=5)", 1);
        test("t2", "(k1=10 or k1=23) and (k2=4 or k2=11)", 2);
        test("t2", "(k1=10 or k1=23) and (k2=3 or k2=4 or k2=11)", 3);
        test("t1", "dt=20211123 or dt=20211124", 2);
        test("t1", "((dt=20211123 and k1=1) or (dt=20211125 and k1=3))", 2);
        // maybe something goes wrong with ExtractCommonFactorsRule.
        test("t1", "((dt=20211123 and k1=1) or (dt=20211125 and k1=3)) and k2>0",
                2);
        test("t2", "k1 > 10 or k2 < 1", 9);
        // add some cases for CompoundPredicate
        test("t1", "(dt >= 20211121 and dt <= 20211122) or (dt >= 20211123 and dt <= 20211125)",
                5);
        test("t1", "(dt between 20211121 and 20211122) or (dt between 20211123 and 20211125)",
                5);
        test("t1", "(dt between 20211121 and 20211122) or dt is null or (dt between 20211123 and 20211125)",
                6);

    }

    private void test(String table, String filter, int expectScanPartitionNum) {
        PlanChecker planChecker = PlanChecker.from(connectContext)
                .analyze("select * from " + table + (filter.isEmpty() ? "" : " where " + filter))
                .rewrite()
                .printlnTree();

        if (expectScanPartitionNum == 0) {
            try {
                planChecker.matches(logicalEmptyRelation());
                return;
            } catch (Throwable t) {
                // do nothing
            }
        }
        planChecker.matches(
                logicalOlapScan().when(scan -> {
                    Assertions.assertEquals(expectScanPartitionNum, scan.getSelectedPartitionIds().size());
                    return true;
                })
        );
    }
}
