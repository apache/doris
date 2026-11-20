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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AdminCompactTableCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_admin_compact");
        createTable("CREATE TABLE test_admin_compact.part_tbl (k INT, v INT) DUPLICATE KEY(k)\n"
                + "PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (100),"
                + " PARTITION p2 VALUES LESS THAN (200))\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1')");
        createTable("CREATE TABLE test_admin_compact.non_part_tbl (k INT, v INT) DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1')");
    }

    private void assertAnalysisError(String sql, String expectedMsg) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminCompactTableCommand);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ((AdminCompactTableCommand) plan).run(connectContext, null));
        Assertions.assertTrue(exception.getMessage().contains(expectedMsg), exception.getMessage());
    }

    @Test
    public void testNoPartitionSpecified() {
        assertAnalysisError("ADMIN COMPACT TABLE test_admin_compact.part_tbl WHERE type = 'BASE'",
                "No partition selected for compaction");
        assertAnalysisError("ADMIN COMPACT TABLE test_admin_compact.non_part_tbl WHERE type = 'BASE'",
                "No partition selected for compaction");
    }

    @Test
    public void testMultiplePartitions() {
        assertAnalysisError("ADMIN COMPACT TABLE test_admin_compact.part_tbl PARTITION (p1, p2) WHERE type = 'BASE'",
                "Only support single partition for compaction");
    }

    @Test
    public void testInvalidWhereClause() {
        assertAnalysisError("ADMIN COMPACT TABLE test_admin_compact.part_tbl PARTITION p1",
                "Compaction type must be specified in Where clause");
        assertAnalysisError("ADMIN COMPACT TABLE test_admin_compact.part_tbl PARTITION p1 WHERE type = 'xxx'",
                "Where clause should looks like");
    }
}
