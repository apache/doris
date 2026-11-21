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

package org.apache.doris.nereids.spm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Round-75 review fixes on the capture side.
 *
 * <ul>
 *   <li>#1 the capture fingerprints must be hashed UNDER the planning locks: CollectRelation
 *       resolves (and caches) the relations BEFORE NereidsPlanner#collectAndLockTable takes
 *       them, so a concurrent `DROP COLUMN v` between that resolution and the lock left an
 *       entry describing the OLD schema while the locked bind froze the NEW one - re-adding
 *       the column then restored the old name/type string and the identical query replayed a
 *       plan that omitted it. A later hash now REPLACES an earlier one, and the capture
 *       context re-records every collected table once the locks are held;</li>
 *   <li>#2 the parser's TYPE-CONSTRUCTOR fallback (`DATE 'not-a-date'` becomes a
 *       NON-explicit `CAST('not-a-date' AS DATE)`) is a user-visible conversion, not an
 *       execution detail: stripping it froze the raw STRING and the replay returned
 *       'not-a-date' where ordinary execution returns a typed DATE NULL.</li>
 * </ul>
 */
public class SPMRound75PlannerTest {

    // ==================== #1: the fingerprint describes the LOCKED schema ====================

    /**
     * CollectRelation's pre-lock hash must not survive: a concurrent `DROP COLUMN v` between
     * that resolution and the planning lock left `[k, v]` recorded while the locked capture
     * froze `[k]`, and re-adding `v` made the stale entry match again.
     */
    @Test
    public void testLaterFingerprintReplacesThePreLockOne() {
        Map<String, String> fingerprints = new HashMap<>();
        java.util.concurrent.atomic.AtomicReference<List<Column>> schema =
                new java.util.concurrent.atomic.AtomicReference<>(
                        List.of(column("k"), column("v")));
        TableIf table = mockedTable(schema);
        SPMPlanTreeSupport.recordTableFingerprint(fingerprints, table, List.of("db", "t"));

        String preLock = fingerprints.get("t");
        Assertions.assertNotNull(preLock, "the pre-lock resolution recorded an entry");

        // the concurrent DROP COLUMN v lands before the locks are taken
        schema.set(List.of(column("k")));
        SPMPlanTreeSupport.recordTableFingerprints(fingerprints,
                Map.of(List.of("db", "t"), table));

        Assertions.assertNotEquals(preLock, fingerprints.get("t"),
                "the lock-time rehash must REPLACE the pre-lock entry, not keep it");
        Map<String, String> expected = new HashMap<>();
        SPMPlanTreeSupport.recordTableFingerprint(expected, table, List.of("db", "t"));
        Assertions.assertEquals(expected.get("t"), fingerprints.get("t"),
                "the stored entry describes the schema the LOCKED bind freezes");
        Assertions.assertEquals(expected.get("db.t"), fingerprints.get("db.t"),
                "the qualifier key is rehashed with the table");
    }

    /**
     * The capture context re-hashes every collected table when the planning locks are taken,
     * so the map it hands back to the CREATE describes the locked schema.
     */
    @Test
    public void testCaptureContextRehashesWhenTheLocksAreTaken() throws Exception {
        java.util.concurrent.atomic.AtomicReference<List<Column>> schema =
                new java.util.concurrent.atomic.AtomicReference<>(
                        List.of(column("k"), column("v")));
        TableIf table = mockedTable(schema);

        Class<?> captureClass = Class.forName(
                "org.apache.doris.nereids.spm.SPMOptimizer$CaptureStatementContext");
        java.lang.reflect.Constructor<?> ctor =
                captureClass.getDeclaredConstructor(ConnectContext.class, String.class);
        ctor.setAccessible(true);
        StatementContext capture = (StatementContext) ctor.newInstance(
                new ConnectContext(), "SELECT k FROM t");
        // no physical locks for a mocked table: the hook itself is what this test observes
        capture.setNeedLockTables(false);

        capture.setTables(Map.of(List.of("db", "t"), table));
        // the pre-lock CollectRelation hook: it resolves the relation BEFORE the locks
        capture.getAndCacheTable(List.of("db", "t"), StatementContext.TableFrom.QUERY,
                java.util.Optional.empty());
        Map<String, String> preLock = new HashMap<>(fingerprintsOf(captureClass, capture));
        Assertions.assertNotNull(preLock.get("t"),
                "the pre-lock hook already recorded a first value");

        schema.set(List.of(column("k")));
        capture.lock();

        Map<String, String> underLocks = new HashMap<>(fingerprintsOf(captureClass, capture));
        Assertions.assertEquals(1, capture.getTables().size(),
                "the statement caches its resolved table: " + capture.getTables());
        Map<String, String> direct = new HashMap<>();
        SPMPlanTreeSupport.recordTableFingerprints(direct, capture.getTables());
        Assertions.assertEquals(direct.get("t"), underLocks.get("t"),
                "the entry matches the CURRENT schema the statement holds");
        Assertions.assertNotEquals(preLock.get("t"), underLocks.get("t"),
                "taking the locks must rehash the collected tables");
        Map<String, String> expected = new HashMap<>();
        SPMPlanTreeSupport.recordTableFingerprint(expected, table, List.of("db", "t"));
        Assertions.assertEquals(expected.get("t"), underLocks.get("t"),
                "the entry the CREATE receives describes the LOCKED schema: " + underLocks);
    }

    // ==================== #2: the parser's type-constructor fallback ====================

    /**
     * `DATE 'not-a-date'` cannot fold to a literal, so the parser emits
     * `CAST('not-a-date' AS DATE)` (non-explicit). That cast IS the user's type: dropping it
     * froze `SELECT 'not-a-date'`, which returns the STRING instead of a typed DATE NULL.
     */
    @Test
    public void testInvalidDateConstructorCastIsPreserved() {
        Cast fallback = new Cast(new StringLiteral("not-a-date"), DateV2Type.INSTANCE);
        Assertions.assertFalse(fallback.isExplicitType(),
                "precondition: the parser fallback is NOT an explicit cast");

        String printed = print(fallback);
        Assertions.assertTrue(printed.contains("AS DATEV2"),
                "the constructor's conversion must survive the freeze: " + printed);
        Assertions.assertTrue(printed.startsWith("CAST("),
                "the frozen text keeps the conversion call: " + printed);
    }

    /** The TIMESTAMP constructor has the same fallback (its target type carries a scale). */
    @Test
    public void testInvalidTimestampConstructorCastIsPreserved() {
        Cast fallback = new Cast(new StringLiteral("not-a-timestamp"), DateTimeV2Type.MAX);
        String printed = print(fallback);
        Assertions.assertTrue(printed.contains("CAST(") && printed.contains("AS DATETIME"),
                "the TIMESTAMP constructor's conversion must survive: " + printed);
    }

    /**
     * The ordinary implicit coercion of a VALID literal is still stripped: it is an execution
     * detail the replay's own analysis re-derives, and keeping it would change the frozen text
     * of every date-filter shape.
     */
    @Test
    public void testValidDateCoercionIsStillStripped() {
        Cast coercion = new Cast(new StringLiteral("2020-01-01"), DateV2Type.INSTANCE);
        String printed = print(coercion);
        Assertions.assertFalse(printed.contains("CAST"),
                "a VALID literal's implicit coercion stays an execution detail: " + printed);
    }

    /** The decompiler sees the PARAMETERIZED tree: the payload carries the captured text. */
    @Test
    public void testParameterizedConstructorFallbackIsPreserved() {
        Cast fallback = new Cast(
                SpmConstVar.of(1L, new StringLiteral("not-a-date")), DateV2Type.INSTANCE);
        String printed = print(fallback);
        Assertions.assertTrue(printed.contains("AS DATEV2"),
                "the parameterized fallback keeps its conversion: " + printed);
        Assertions.assertTrue(printed.contains("_spm_const_var(1)"),
                "the placeholder still renders its id-only form: " + printed);
    }

    // ==================== helpers ====================

    private static String print(Expression expr) {
        return new SPMExprSqlBuilder().print(expr, new SQLRelation());
    }

    private static TableIf mockedTable(
            java.util.concurrent.atomic.AtomicReference<List<Column>> schema) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getId()).thenReturn(100L);
        Mockito.when(table.getBaseSchema()).thenAnswer(invocation -> schema.get());
        return table;
    }

    private static Column column(String name) {
        return new Column(name, Type.INT);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> fingerprintsOf(Class<?> captureClass,
            StatementContext capture) throws Exception {
        java.lang.reflect.Method getter =
                captureClass.getDeclaredMethod("getTableFingerprints");
        getter.setAccessible(true);
        return (Map<String, String>) getter.invoke(capture);
    }
}
