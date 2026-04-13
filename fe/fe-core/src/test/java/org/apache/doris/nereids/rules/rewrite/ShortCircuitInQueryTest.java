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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that key-column IN queries on MOW tables trigger the short-circuit point query path.
 */
public class ShortCircuitInQueryTest extends TestWithFeService {

    private static final String MOW_TABLE = "mow_tbl";
    private static final String SINGLE_KEY_TABLE = "mow_single_key";
    private static final String DUP_TABLE = "dup_tbl";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        // MOW table with two key columns, light_schema_change and store_row_column enabled.
        // All three properties are required by scanMatchShortCircuitCondition.
        createTables(
                "CREATE TABLE IF NOT EXISTS " + MOW_TABLE + " (\n"
                        + "    k1 INT NOT NULL,\n"
                        + "    k2 INT NOT NULL,\n"
                        + "    v1 VARCHAR(100)\n"
                        + ") ENGINE=OLAP\n"
                        + "UNIQUE KEY(k1, k2)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 4\n"
                        + "PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\",\n"
                        + "    \"enable_unique_key_merge_on_write\" = \"true\",\n"
                        + "    \"light_schema_change\" = \"true\",\n"
                        + "    \"store_row_column\" = \"true\"\n"
                        + ")",

                // MOW table with a single key column, used to verify IN on the only key.
                "CREATE TABLE IF NOT EXISTS " + SINGLE_KEY_TABLE + " (\n"
                        + "    id INT NOT NULL,\n"
                        + "    name VARCHAR(100)\n"
                        + ") ENGINE=OLAP\n"
                        + "UNIQUE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 4\n"
                        + "PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\",\n"
                        + "    \"enable_unique_key_merge_on_write\" = \"true\",\n"
                        + "    \"light_schema_change\" = \"true\",\n"
                        + "    \"store_row_column\" = \"true\"\n"
                        + ")",

                // Duplicate-key table – must NOT trigger short-circuit even with IN on key columns.
                "CREATE TABLE IF NOT EXISTS " + DUP_TABLE + " (\n"
                        + "    k1 INT NOT NULL,\n"
                        + "    k2 INT NOT NULL,\n"
                        + "    v1 VARCHAR(100)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1, k2)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 4\n"
                        + "PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\"\n"
                        + ")"
        );
    }

    // -----------------------------------------------------------------------
    // Positive cases: should trigger short-circuit (isShortCircuitQuery = true)
    // -----------------------------------------------------------------------

    /**
     * SELECT * FROM mow_single_key WHERE id IN (1, 2, 3)
     * Only one key column, covered by IN → short-circuit.
     */
    @Test
    void testSingleKeyInQueryTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + SINGLE_KEY_TABLE + " WHERE id IN (1, 2, 3)")
                .rewrite()
                .getCascadesContext();
        Assertions.assertTrue(ctx.getStatementContext().isShortCircuitQuery(),
                "Single-key IN query on MOW table should trigger short-circuit");
    }

    /**
     * SELECT * FROM mow_single_key WHERE id IN (42)
     * IN with a single element is still a valid IN predicate → short-circuit.
     */
    @Test
    void testSingleKeyInWithOneValueTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + SINGLE_KEY_TABLE + " WHERE id IN (42)")
                .rewrite()
                .getCascadesContext();
        Assertions.assertTrue(ctx.getStatementContext().isShortCircuitQuery(),
                "IN with a single value on the only key column should trigger short-circuit");
    }

    /**
     * SELECT * FROM mow_tbl WHERE k1 IN (1, 2) AND k2 = 10
     * Two key columns: k1 covered by IN, k2 by equality → all keys covered → short-circuit.
     */
    @Test
    void testCompositeKeyInAndEqualTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + MOW_TABLE
                        + " WHERE k1 IN (1, 2) AND k2 = 10")
                .rewrite()
                .getCascadesContext();
        Assertions.assertTrue(ctx.getStatementContext().isShortCircuitQuery(),
                "IN on first key + equality on second key should trigger short-circuit");
    }

    /**
     * SELECT * FROM mow_tbl WHERE k1 = 5 AND k2 IN (10, 20, 30)
     * Same as above with the roles swapped.
     */
    @Test
    void testCompositeKeyEqualAndInTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + MOW_TABLE
                        + " WHERE k1 = 5 AND k2 IN (10, 20, 30)")
                .rewrite()
                .getCascadesContext();
        Assertions.assertTrue(ctx.getStatementContext().isShortCircuitQuery(),
                "Equality on first key + IN on second key should trigger short-circuit");
    }

    /**
     * SELECT * FROM mow_tbl WHERE k1 = 1 AND k2 = 2
     * Pure equality (original behaviour) must still work.
     */
    @Test
    void testPureEqualityStillTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + MOW_TABLE + " WHERE k1 = 1 AND k2 = 2")
                .rewrite()
                .getCascadesContext();
        Assertions.assertTrue(ctx.getStatementContext().isShortCircuitQuery(),
                "Pure equality on all key columns should still trigger short-circuit");
    }

    // -----------------------------------------------------------------------
    // Negative cases: should NOT trigger short-circuit
    // -----------------------------------------------------------------------

    /**
     * SELECT * FROM dup_tbl WHERE k1 IN (1, 2, 3) AND k2 = 1
     * Duplicate-key table: store_row_column / light_schema_change not set → no short-circuit.
     */
    @Test
    void testDuplicateKeyTableNotTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + DUP_TABLE + " WHERE k1 IN (1, 2, 3) AND k2 = 1")
                .rewrite()
                .getCascadesContext();
        Assertions.assertFalse(ctx.getStatementContext().isShortCircuitQuery(),
                "Duplicate-key table should NOT trigger short-circuit");
    }

    /**
     * SELECT * FROM mow_tbl WHERE k1 IN (1, 2, 3)
     * Only k1 is provided via IN; k2 is missing → key columns not fully covered → no short-circuit.
     */
    @Test
    void testPartialKeyInNotTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + MOW_TABLE + " WHERE k1 IN (1, 2, 3)")
                .rewrite()
                .getCascadesContext();
        Assertions.assertFalse(ctx.getStatementContext().isShortCircuitQuery(),
                "IN on only part of the key columns should NOT trigger short-circuit");
    }

    /**
     * SELECT * FROM mow_tbl WHERE v1 IN ('a', 'b') AND k1 = 1 AND k2 = 2
     * IN on a non-key (value) column v1 is NOT a valid short-circuit expression → no short-circuit.
     */
    @Test
    void testNonKeyColumnInNotTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + MOW_TABLE
                        + " WHERE v1 IN ('a', 'b') AND k1 = 1 AND k2 = 2")
                .rewrite()
                .getCascadesContext();
        Assertions.assertFalse(ctx.getStatementContext().isShortCircuitQuery(),
                "IN on a non-key column should NOT trigger short-circuit");
    }

    /**
     * SELECT * FROM mow_tbl WHERE k1 > 5 AND k2 = 2
     * Range predicate (>) on a key column is not a valid short-circuit expression → no short-circuit.
     */
    @Test
    void testRangePredicateNotTriggerShortCircuit() {
        CascadesContext ctx = PlanChecker.from(connectContext)
                .analyze("SELECT * FROM " + MOW_TABLE + " WHERE k1 > 5 AND k2 = 2")
                .rewrite()
                .getCascadesContext();
        Assertions.assertFalse(ctx.getStatementContext().isShortCircuitQuery(),
                "Range predicate on key column should NOT trigger short-circuit");
    }
}
