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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class ReadMorAsDupTest extends TestWithFeService implements GeneratedPlanPatterns {
    private static final String DB = "test_read_mor_as_dup_db";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB);

        // MOR UNIQUE table (enable_unique_key_merge_on_write = false)
        createTable("CREATE TABLE " + DB + ".mor_tbl (\n"
                + "  k INT NOT NULL,\n"
                + "  v1 INT,\n"
                + "  v2 VARCHAR(100)\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'false');");

        // MOW UNIQUE table (should not be affected)
        createTable("CREATE TABLE " + DB + ".mow_tbl (\n"
                + "  k INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true');");

        // DUP table (should not be affected)
        createTable("CREATE TABLE " + DB + ".dup_tbl (\n"
                + "  k INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");

        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testDeleteSignFilterPresentByDefault() {
        connectContext.getSessionVariable().readMorAsDupTables = "";

        PlanChecker.from(connectContext)
                .analyze("select * from mor_tbl")
                .rewrite()
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalOlapScan()
                                ).when(filter -> hasDeleteSignPredicate(filter.getConjuncts()))
                        )
                );
    }

    @Test
    void testDeleteSignFilterSkippedWithWildcard() {
        connectContext.getSessionVariable().readMorAsDupTables = "*";

        try {
            PlanChecker.from(connectContext)
                    .analyze("select * from mor_tbl")
                    .rewrite()
                    .nonMatch(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> hasDeleteSignPredicate(filter.getConjuncts()))
                    );
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = "";
        }
    }

    @Test
    void testDeleteSignFilterSkippedWithTableName() {
        connectContext.getSessionVariable().readMorAsDupTables = "mor_tbl";

        try {
            PlanChecker.from(connectContext)
                    .analyze("select * from mor_tbl")
                    .rewrite()
                    .nonMatch(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> hasDeleteSignPredicate(filter.getConjuncts()))
                    );
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = "";
        }
    }

    @Test
    void testDeleteSignFilterSkippedWithDbTableName() {
        connectContext.getSessionVariable().readMorAsDupTables =
                DEFAULT_CLUSTER_PREFIX + DB + ".mor_tbl";

        try {
            PlanChecker.from(connectContext)
                    .analyze("select * from mor_tbl")
                    .rewrite()
                    .nonMatch(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> hasDeleteSignPredicate(filter.getConjuncts()))
                    );
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = "";
        }
    }

    @Test
    void testMowTableNotAffected() {
        // MOW table should still have delete sign filter even with read_mor_as_dup = *
        connectContext.getSessionVariable().readMorAsDupTables = "*";

        try {
            // MOW tables also have delete sign filter; read_mor_as_dup should NOT remove it
            PlanChecker.from(connectContext)
                    .analyze("select * from mow_tbl")
                    .rewrite()
                    .matches(
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> hasDeleteSignPredicate(filter.getConjuncts()))
                            )
                    );
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = "";
        }
    }

    @Test
    void testPreAggOnForMorWithReadAsDup() {
        connectContext.getSessionVariable().readMorAsDupTables = "*";

        try {
            Plan plan = PlanChecker.from(connectContext)
                    .analyze("select * from mor_tbl")
                    .rewrite()
                    .getPlan();

            // Find the LogicalOlapScan and verify preAggStatus is ON
            LogicalOlapScan scan = findOlapScan(plan);
            Assertions.assertNotNull(scan, "Should find LogicalOlapScan in plan");
            Assertions.assertTrue(scan.getPreAggStatus().isOn(),
                    "PreAggStatus should be ON when read_mor_as_dup is enabled");
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = "";
        }
    }

    @Test
    void testPerTableControlOnlyAffectsSpecifiedTable() {
        // Only enable for mor_tbl, not for other tables
        connectContext.getSessionVariable().readMorAsDupTables = "mor_tbl";

        try {
            // mor_tbl should NOT have delete sign filter
            PlanChecker.from(connectContext)
                    .analyze("select * from mor_tbl")
                    .rewrite()
                    .nonMatch(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> hasDeleteSignPredicate(filter.getConjuncts()))
                    );

            // dup_tbl should be unaffected (no delete sign filter for DUP)
            PlanChecker.from(connectContext)
                    .analyze("select * from dup_tbl")
                    .rewrite()
                    .matches(logicalOlapScan());
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = "";
        }
    }

    @Test
    void testSessionVariableHelperMethod() {
        // Test the isReadMorAsDupEnabled helper directly
        connectContext.getSessionVariable().readMorAsDupTables = "";
        Assertions.assertFalse(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("db", "tbl"));

        connectContext.getSessionVariable().readMorAsDupTables = "*";
        Assertions.assertTrue(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("db", "tbl"));
        Assertions.assertTrue(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("any_db", "any_tbl"));

        connectContext.getSessionVariable().readMorAsDupTables = "mydb.mytbl";
        Assertions.assertTrue(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("mydb", "mytbl"));
        Assertions.assertFalse(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("otherdb", "othertbl"));
        // "mydb.mytbl" entry requires both db and table components to match
        Assertions.assertFalse(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("anything", "mytbl"));

        // Table name only (no db prefix) matches any db
        connectContext.getSessionVariable().readMorAsDupTables = "mytbl";
        Assertions.assertTrue(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("anydb", "mytbl"));
        Assertions.assertFalse(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("anydb", "othertbl"));

        connectContext.getSessionVariable().readMorAsDupTables = "db1.tbl1,db2.tbl2";
        Assertions.assertTrue(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("db1", "tbl1"));
        Assertions.assertTrue(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("db2", "tbl2"));
        Assertions.assertFalse(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("db1", "tbl2"));

        // Case insensitive
        connectContext.getSessionVariable().readMorAsDupTables = "MyDB.MyTbl";
        Assertions.assertTrue(
                connectContext.getSessionVariable().isReadMorAsDupEnabled("mydb", "mytbl"));

        // Cleanup
        connectContext.getSessionVariable().readMorAsDupTables = "";
    }

    private boolean hasDeleteSignPredicate(Set<Expression> conjuncts) {
        return conjuncts.stream()
                .anyMatch(expr -> expr.toSql().contains(Column.DELETE_SIGN));
    }

    private LogicalOlapScan findOlapScan(Plan plan) {
        if (plan instanceof LogicalOlapScan) {
            return (LogicalOlapScan) plan;
        }
        for (Plan child : plan.children()) {
            LogicalOlapScan result = findOlapScan(child);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }
}
