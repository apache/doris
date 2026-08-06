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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test:
 * For short-circuit point query, we should not rewrite LogicalOlapScan to LogicalEmptyRelation
 * even if the table partitions are empty. Otherwise PreparedStatement could not cache
 * ShortCircuitQueryContext and may downgrade to normal planning repeatedly.
 */
class ShortCircuitPointQueryTest extends TestWithFeService
        implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE `tbl_point_query` (\n"
                + "  `key` int(11) NULL,\n"
                + "  `v1` varchar(30) NULL\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`key`)\n"
                + "DISTRIBUTED BY HASH(`key`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"enable_unique_key_merge_on_write\" = \"true\",\n"
                + "  \"light_schema_change\" = \"true\",\n"
                + "  \"store_row_column\" = \"true\"\n"
                + ");");
    }

    @Test
    void testShortCircuitPointQueryKeepOlapScanWhenTableEmpty() {
        boolean originRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            String sql = "select * from tbl_point_query where `key` = 1";
            Plan plan = PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .getPlan();

            // short-circuit flag should be set
            Assertions.assertTrue(connectContext.getStatementContext().isShortCircuitQuery());
            // should still keep scan node for point query path
            Assertions.assertTrue(plan.anyMatch(p -> p instanceof LogicalOlapScan));
            Assertions.assertFalse(plan.anyMatch(p -> p instanceof LogicalEmptyRelation));
        } finally {
            FeConstants.runningUnitTest = originRunningUnitTest;
        }
    }
}
