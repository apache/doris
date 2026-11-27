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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Round-75 review fixes on the capture side.
 *
 * <ul>
 *   <li>#1 the capture fingerprints must be hashed UNDER the planning locks: CollectRelation
 *       resolves (and caches) the relations BEFORE NereidsPlanner#collectAndLockTable takes
 *       them, so a concurrent `DROP COLUMN v` between that resolution and the lock left an
 *       entry describing the OLD schema while the locked bind froze the NEW one - re-adding
 *       the column then restored the old name/type string and the identical query replayed a
 *       plan that omitted it. Capture now records each distinct table once, after acquiring
 *       the planning locks;</li>
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
     * The capture context does not read metadata before locking, and aliases reuse the
     * one entry describing the locked schema.
     */
    @Test
    public void testCaptureContextReadsSchemaOnceAfterLocking() throws Exception {
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

        capture.setTables(Map.of(List.of("db", "t"), table, List.of("internal", "db", "t"), table));
        // the pre-lock CollectRelation hook: it resolves the relation BEFORE the locks
        capture.getAndCacheTable(List.of("db", "t"), StatementContext.TableFrom.QUERY,
                java.util.Optional.empty());
        Map<String, String> preLock = new HashMap<>(fingerprintsOf(captureClass, capture));
        Assertions.assertTrue(preLock.isEmpty());
        Mockito.verify(table, Mockito.never()).getBaseSchema();

        schema.set(List.of(column("k")));
        capture.lock();

        Map<String, String> underLocks = new HashMap<>(fingerprintsOf(captureClass, capture));
        capture.getAndCacheTable(List.of("db", "t"), StatementContext.TableFrom.QUERY,
                Optional.empty());
        capture.getAndCacheTable(List.of("internal", "db", "t"), StatementContext.TableFrom.QUERY,
                Optional.empty());
        Mockito.verify(table, Mockito.times(1)).getBaseSchema();
        Assertions.assertEquals(underLocks.get("db.t"), underLocks.get("internal.db.t"));
        Map<String, String> expected = new HashMap<>();
        SPMPlanTreeSupport.recordTableFingerprint(expected, table, List.of("db", "t"));
        Assertions.assertEquals(expected.get("t"), underLocks.get("t"));
        capture.close();
    }

    @Test
    public void testLateTableIsLockedBeforeCaptureAndReleased() throws Exception {
        Class<?> captureClass = Class.forName(
                "org.apache.doris.nereids.spm.SPMOptimizer$CaptureStatementContext");
        java.lang.reflect.Constructor<?> ctor =
                captureClass.getDeclaredConstructor(ConnectContext.class, String.class);
        ctor.setAccessible(true);
        StatementContext capture = (StatementContext) ctor.newInstance(
                new ConnectContext(), "SELECT k FROM t");
        TableIf late = mockedTable(new java.util.concurrent.atomic.AtomicReference<>(List.of(column("k"))));
        Mockito.when(late.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(late.tryReadLock(0, TimeUnit.MILLISECONDS)).thenReturn(true);
        capture.lock();
        capture.getTables().put(List.of("db", "t"), late);
        capture.getAndCacheTable(List.of("db", "t"), StatementContext.TableFrom.QUERY, Optional.empty());
        capture.getAndCacheTable(List.of("db", "t"), StatementContext.TableFrom.QUERY, Optional.empty());
        Assertions.assertNotNull(fingerprintsOf(captureClass, capture).get("db.t"));
        capture.releasePlannerResources();
        org.mockito.InOrder order = Mockito.inOrder(late);
        order.verify(late).tryReadLock(0, TimeUnit.MILLISECONDS);
        order.verify(late).getBaseSchema();
        order.verify(late).readUnlock();
        Mockito.verify(late, Mockito.times(1)).getBaseSchema();
        capture.close();
    }

    @Test
    public void testLateTableLockFailureDoesNotCaptureUnprotectedMetadata() throws Exception {
        Class<?> captureClass = Class.forName(
                "org.apache.doris.nereids.spm.SPMOptimizer$CaptureStatementContext");
        java.lang.reflect.Constructor<?> ctor =
                captureClass.getDeclaredConstructor(ConnectContext.class, String.class);
        ctor.setAccessible(true);
        StatementContext capture = (StatementContext) ctor.newInstance(
                new ConnectContext(), "SELECT k FROM t");
        TableIf late = mockedTable(new java.util.concurrent.atomic.AtomicReference<>(List.of(column("k"))));
        Mockito.when(late.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(late.tryReadLock(0, TimeUnit.MILLISECONDS)).thenReturn(false);
        capture.lock();
        capture.getTables().put(List.of("db", "t"), late);
        Assertions.assertThrows(RuntimeException.class,
                () -> capture.getAndCacheTable(List.of("db", "t"), StatementContext.TableFrom.QUERY,
                        Optional.empty()));
        Mockito.verify(late, Mockito.never()).getBaseSchema();
        Mockito.verify(late, Mockito.never()).readUnlock();
        Assertions.assertTrue(fingerprintsOf(captureClass, capture).isEmpty());
    }

    @Test
    public void testNativeOlapFingerprintUsesBaseIndexAndSchemaVersion() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getId()).thenReturn(100L);
        Mockito.when(table.getType()).thenReturn(TableIf.TableType.OLAP);
        Mockito.when(table.getBaseIndexId()).thenReturn(200L);
        Mockito.when(table.getBaseSchemaVersion()).thenReturn(3);
        Map<String, String> entries = new HashMap<>();
        SPMPlanTreeSupport.recordTableFingerprints(entries, Map.of(
                List.of("db", "t"), table, List.of("internal", "db", "t"), table));
        Assertions.assertEquals("t|100|olap-schema:200:3", entries.get("db.t"));
        Assertions.assertEquals(entries.get("db.t"), entries.get("internal.db.t"));
        Mockito.verify(table, Mockito.times(1)).getBaseSchemaVersion();
        Mockito.verify(table, Mockito.never()).getBaseSchema();
        Mockito.when(table.getBaseSchemaVersion()).thenReturn(4);
        SPMPlanTreeSupport.recordTableFingerprint(entries, table, List.of("db", "t"));
        Assertions.assertEquals("t|100|olap-schema:200:4", entries.get("db.t"));
        Mockito.when(table.getBaseIndexId()).thenReturn(201L);
        SPMPlanTreeSupport.recordTableFingerprint(entries, table, List.of("db", "t"));
        Assertions.assertEquals("t|100|olap-schema:201:4", entries.get("db.t"));
    }

    @Test
    public void testRemoteOlapStillUsesContentFingerprint() {
        RemoteOlapTable table = Mockito.mock(RemoteOlapTable.class);
        Mockito.when(table.getName()).thenReturn("remote");
        Mockito.when(table.getId()).thenReturn(100L);
        Mockito.when(table.getType()).thenReturn(TableIf.TableType.OLAP);
        Mockito.when(table.getBaseSchema()).thenReturn(List.of(column("k")));
        Map<String, String> entries = new HashMap<>();
        SPMPlanTreeSupport.recordTableFingerprint(entries, table);
        Assertions.assertTrue(entries.get("remote").contains("|nullable:"));
        Mockito.verify(table, Mockito.times(1)).getBaseSchema();
        Mockito.verify(table, Mockito.never()).getBaseSchemaVersion();
    }

    @Test
    public void testPartitionSnapshotOnlyReadsExplicitPins() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(100L);
        LogicalOlapScan scan = Mockito.mock(LogicalOlapScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of());
        Map<Long, Map<Long, SPMPlanTreeSupport.PartitionPin>> pins = new HashMap<>();
        SPMPlanTreeSupport.recordPartitionNames(pins, scan);
        Assertions.assertTrue(pins.isEmpty());
        Mockito.verify(table, Mockito.never()).getPartition(Mockito.anyLong());

        Partition p1 = Mockito.mock(Partition.class);
        Partition p2 = Mockito.mock(Partition.class);
        Mockito.when(p1.getName()).thenReturn("p1");
        Mockito.when(p2.getName()).thenReturn("tmp");
        Mockito.when(table.getPartition(1L)).thenReturn(p1);
        Mockito.when(table.getPartition(2L)).thenReturn(p2);
        Mockito.when(table.isTemporaryPartition(2L)).thenReturn(true);
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of(1L, 2L));
        SPMPlanTreeSupport.recordPartitionNames(pins, scan);
        SPMPlanTreeSupport.recordPartitionNames(pins, scan);
        Assertions.assertEquals(2, pins.get(100L).size());
        Assertions.assertEquals("p1", pins.get(100L).get(1L).getName());
        Assertions.assertFalse(pins.get(100L).get(1L).isTemporary());
        Assertions.assertEquals("tmp", pins.get(100L).get(2L).getName());
        Assertions.assertTrue(pins.get(100L).get(2L).isTemporary());
        Mockito.verify(table, Mockito.times(1)).getPartition(1L);
        Mockito.verify(table, Mockito.times(1)).getPartition(2L);
        Mockito.verify(table, Mockito.never()).getPartitions();
        Mockito.verify(table, Mockito.never()).getAllTempPartitions();
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
