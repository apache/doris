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

import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for plan visitors to make sure the result meets expectation.
 */
public class PlanVisitorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("visitor_test");
        useDatabase("visitor_test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        createTable("CREATE TABLE `table1` (\n"
                + " `c1` varchar(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` int(20) not NULL,\n"
                + " `c4` DATE not NULL,\n"
                + " `k4` bitmap BITMAP_UNION NULL,\n"
                + " `k5` bitmap BITMAP_UNION NULL \n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`, `c4`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\" \n"
                + ");");

        createTable("CREATE TABLE `table2` (\n"
                + " `c1` bigint(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` bigint(20) not NULL,\n"
                + " `c4` DATE not NULL,\n"
                + " `k4` bitmap BITMAP_UNION NULL ,\n"
                + " `k5` bitmap BITMAP_UNION NULL \n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`, `c4`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE `table3` (\n"
                + " `c1` bigint(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` bigint(20) not NULL,\n"
                + " `c4` DATE not NULL,\n"
                + " `k4` bitmap BITMAP_UNION NULL ,\n"
                + " `k5` bitmap BITMAP_UNION NULL \n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`, `c4`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        createView("CREATE VIEW `view1` AS SELECT t1.*, random() FROM\n"
                + "`table1` t1 LEFT JOIN\n"
                + "`table2` t2 ON t1.c1 = t2.c1;");

        createMvByNereids("create materialized view mv1 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1') \n"
                + "as "
                + "select t1.c1, t3.c2 "
                + "from table1 t1 "
                + "inner join table3 t3 on t1.c1= t3.c2;");
    }

    @Test
    public void testTimeFunction() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT *, now() FROM table1 "
                                + "LEFT SEMI JOIN table2 ON table1.c1 = table2.c1 "
                                + "WHERE table1.c1 IN (SELECT c1 FROM table2) OR current_date() < '2023-01-01' and current_time() = '2023-01-10'",
                        nereidsPlanner -> {
                            // Check nondeterministic collect
                            List<Expression> nondeterministicFunctionSet =
                                    MaterializedViewUtils.extractNondeterministicFunction(
                                            nereidsPlanner.getAnalyzedPlan());
                            Assertions.assertEquals(3, nondeterministicFunctionSet.size());
                            Assertions.assertTrue(nondeterministicFunctionSet.get(0) instanceof Now);
                            Assertions.assertTrue(nondeterministicFunctionSet.get(1) instanceof CurrentDate);
                            Assertions.assertTrue(nondeterministicFunctionSet.get(2) instanceof CurrentTime);
                        });
    }

    @Test
    public void testCurrentDateFunction() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT * FROM table1 "
                                + "LEFT SEMI JOIN table2 ON table1.c1 = table2.c1 "
                                + "WHERE table1.c1 IN (SELECT c1 FROM table2) OR current_date() < '2023-01-01'",
                        nereidsPlanner -> {
                            // Check nondeterministic collect
                            List<Expression> nondeterministicFunctionSet =
                                    MaterializedViewUtils.extractNondeterministicFunction(
                                            nereidsPlanner.getAnalyzedPlan());
                            Assertions.assertEquals(1, nondeterministicFunctionSet.size());
                            Assertions.assertTrue(nondeterministicFunctionSet.get(0) instanceof CurrentDate);
                        });
    }

    @Test
    public void testContainsNondeterministic() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT * FROM table1 "
                                + "LEFT SEMI JOIN table2 ON table1.c1 = table2.c1 "
                                + "WHERE table1.c1 IN (SELECT c1 FROM table2) OR date_add(current_date(), INTERVAL 2 DAY) < '2023-01-01'",
                        nereidsPlanner -> {
                            // Check nondeterministic collect
                            List<Expression> nondeterministicFunctionSet =
                                    MaterializedViewUtils.extractNondeterministicFunction(
                                            nereidsPlanner.getAnalyzedPlan());
                            Assertions.assertEquals(1, nondeterministicFunctionSet.size());
                            Assertions.assertTrue(nondeterministicFunctionSet.get(0) instanceof CurrentDate);
                        });
    }

    @Test
    public void testUnixTimestampWithArgsFunction() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT * FROM table1 "
                                + "LEFT SEMI JOIN table2 ON table1.c1 = table2.c1 "
                                + "WHERE table1.c1 IN (SELECT c1 FROM table2) OR unix_timestamp(table1.c4, '%Y-%m-%d %H:%i-%s') < '2023-01-01' and unix_timestamp(table1.c4) = '2023-01-10'",
                        nereidsPlanner -> {
                            // Check nondeterministic collect
                            List<Expression> nondeterministicFunctionSet =
                                    MaterializedViewUtils.extractNondeterministicFunction(
                                            nereidsPlanner.getAnalyzedPlan());
                            Assertions.assertEquals(0, nondeterministicFunctionSet.size());
                        });
    }

    @Test
    public void testUnixTimestampWithoutArgsFunction() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT unix_timestamp(), * FROM table1 "
                                + "LEFT SEMI JOIN table2 ON table1.c1 = table2.c1 ",
                        nereidsPlanner -> {
                            // Check nondeterministic collect
                            List<Expression> nondeterministicFunctionSet =
                                    MaterializedViewUtils.extractNondeterministicFunction(
                                            nereidsPlanner.getAnalyzedPlan());
                            Assertions.assertEquals(1, nondeterministicFunctionSet.size());
                            Assertions.assertTrue(nondeterministicFunctionSet.get(0) instanceof UnixTimestamp);
                        });
    }
}
