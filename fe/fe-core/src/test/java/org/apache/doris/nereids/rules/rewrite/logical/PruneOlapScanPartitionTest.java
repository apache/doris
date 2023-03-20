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

package org.apache.doris.nereids.rules.rewrite.logical;

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

        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 4 and col2 > 11", 0);
//        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 4 or col2 > 5", 1);
//        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 4 or col2 > 3", 2);
//        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 < 1 or col2 > 5", 1);
//        test("testOlapScanPartitionPruneWithMultiColumnCase", "col1 <= 1 or col2 >= 10", 1);
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

    private void test(String table, String filter, int expectScanPartitionNum) {
        PlanChecker.from(connectContext)
                .analyze("select * from " + table + " where " + filter)
                .rewrite()
                .printlnTree()
                .matchesFromRoot(
                    logicalFilter(
                        logicalOlapScan().when(scan -> {
                            Assertions.assertEquals(expectScanPartitionNum, scan.getSelectedPartitionIds().size());
                            return true;
                        })
                    )
                );
    }
}
