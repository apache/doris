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
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Review fixes that live in SPMPlanTreeSupport / the replay contract:
 *
 * - #3: a BARE bind column (CREATE BASELINE PLAN 'SELECT k FROM t' WITH 'SELECT
 *   k AS other FROM t') exposes no label at all through outputLabelOf, so the frozen
 *   sink's captured alias other leaked out as the caller's JDBC column label
 *   instead of being realigned to k.
 * - #4: the replay transfer of the caller's top-level LIMIT / OFFSET is a POSITIONAL
 *   merge; a manual plan may keep its own limit below a node the merge cannot align
 *   (DISTINCT over an inner limit). The helper below is the shape probe the guard uses.
 * - #6: one 0/1 digit per column made a fingerprint entry grow without bound: a valid
 *   query joining ten 500-column tables exceeded the fingerprint column's VARCHAR(4096)
 *   and could not be persisted at all.
 */
public class SPMRound25SafetyTest {

    private static final int WIDE_TABLES = 10;
    private static final int WIDE_COLUMNS = 500;
    /** The fingerprint column's persisted width (InternalSchemaInitializer). */
    private static final int FINGERPRINT_COLUMN_WIDTH = 4096;

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** The first project item of a tree (the root of a parse is a sink wrapper). */
    private static NamedExpression firstProjectItem(Plan plan) {
        if (plan instanceof LogicalProject) {
            return ((LogicalProject<?>) plan).getProjects().get(0);
        }
        for (Plan child : plan.children()) {
            NamedExpression item = firstProjectItem(child);
            if (item != null) {
                return item;
            }
        }
        return null;
    }

    // ==================== #3: a bare bind column still gets its label back ====================

    /**
     * The caller's item is a BARE column while the frozen manual plan pinned another
     * label for it. Aligning must replace the captured label with the caller's column
     * name - a null label skipped exactly that realignment, so the JDBC result column
     * reported the manual plan's alias.
     */
    @Test
    public void testBareColumnLabelIsRestoredOnAReplay() {
        LogicalPlan rewritten = parse("SELECT k AS other FROM t");
        LogicalPlan user = parse("SELECT k FROM t");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertNotSame(rewritten, aligned,
                "the captured alias must be realigned to the caller's bare column");
        NamedExpression item = firstProjectItem(aligned);
        Assertions.assertTrue(item instanceof UnboundAlias, "unexpected item: " + item);
        Assertions.assertEquals("k", ((UnboundAlias) item).getAlias().orElse(null),
                "the caller's own column name is the visible label: " + aligned);
        // the substituted expression itself is untouched (the slot stays `k`)
        Assertions.assertEquals("k", ((UnboundAlias) item).child().toSql());

        // an identical bare label stays untouched (no needless rewrite)
        Assertions.assertSame(rewritten,
                SPMPlanTreeSupport.alignRootOutputLabels(rewritten,
                        parse("SELECT k AS other FROM t")));
    }

    /**
     * The frozen plan may project * while the caller names a bare column (the
     * decompiled single-table plan does exactly that). A star carries NO label - its
     * expansion happens at binding - so the position must be left alone: wrapping it in a
     * rename built an Alias over an unbound star and the analyzer rejected the whole tree
     * ("Invalid call to k.getDataType() on unbound object").
     */
    @Test
    public void testStarProjectionIsLeftAlone() {
        LogicalPlan rewritten = parse("SELECT * FROM t");
        Assertions.assertSame(rewritten,
                SPMPlanTreeSupport.alignRootOutputLabels(rewritten, parse("SELECT k FROM t")),
                "a bare caller column must not be aligned onto a star projection");
        Assertions.assertSame(rewritten,
                SPMPlanTreeSupport.alignRootOutputLabels(rewritten,
                        parse("SELECT k AS total FROM t")),
                "an aliased caller column must not be aligned onto a star projection either");
    }

    // ==================== #4: the top-level limit shape probe ====================

    /**
     * The probe must report the limit the CALLER would observe: the outermost LIMIT above
     * any projection. A limit below a projection-like wrapper (the DISTINCT case) is not
     * the caller's top-level limit and must not be reported as one.
     */
    @Test
    public void testTopLevelLimitOfFollowsTheWrapperChain() {
        Assertions.assertArrayEquals(new long[] {2L, 0L},
                SPMPlanTreeSupport.topLevelLimitOf(parse("SELECT k FROM t LIMIT 2")));
        Assertions.assertArrayEquals(new long[] {2L, 3L},
                SPMPlanTreeSupport.topLevelLimitOf(parse("SELECT k FROM t LIMIT 2 OFFSET 3")));
        Assertions.assertNull(SPMPlanTreeSupport.topLevelLimitOf(parse("SELECT k FROM t")),
                "no limit anywhere");
        Assertions.assertNull(SPMPlanTreeSupport.topLevelLimitOf(
                        parse("SELECT DISTINCT k FROM (SELECT k FROM t LIMIT 1) s")),
                "a limit under a DISTINCT is not the caller's top-level limit");
        Assertions.assertNull(SPMPlanTreeSupport.topLevelLimitOf(
                        parse("SELECT k FROM (SELECT k FROM t LIMIT 1) s")),
                "a limit under a projection is not the caller's top-level limit either");
        // a limit that only differs in the offset is a different contract
        Assertions.assertFalse(java.util.Arrays.equals(
                SPMPlanTreeSupport.topLevelLimitOf(parse("SELECT k FROM t LIMIT 2")),
                SPMPlanTreeSupport.topLevelLimitOf(parse("SELECT k FROM t LIMIT 2 OFFSET 1"))));
    }

    // ==================== #6: the fingerprint stays within the persisted column ====================

    private static TableIf wideTable(String name, long id, int columns, boolean nullableTail) {
        List<Column> schema = new ArrayList<>(columns);
        for (int i = 0; i < columns; i++) {
            Column column = new Column("c" + i, Type.INT);
            column.setIsAllowNull(i == columns - 1 && nullableTail);
            schema.add(column);
        }
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getBaseSchema()).thenReturn(schema);
        return table;
    }

    /** A session resolving each fully-qualified relation to its own mock table. */
    private static ConnectContext contextResolving(Map<String, TableIf> byName) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getAndCacheTable(
                        Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> tableOf(invocation.getArgument(1), byName));
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> tableOf(invocation.getArgument(1), byName));
        ctx.setStatementContext(statementContext);
        return ctx;
    }

    private static TableIf tableOf(Object relationArg, Map<String, TableIf> byName) {
        Optional<?> relation = (Optional<?>) relationArg;
        List<String> nameParts = ((UnboundRelation) relation.get()).getNameParts();
        return byName.get(nameParts.get(nameParts.size() - 1));
    }

    /**
     * The reviewer's example: a valid query joining ten 500-column tables. One digit per
     * column per table added more than 5000 characters - the CREATE could not persist the
     * fingerprint at all. The per-table section is now a fixed-size digest, so the whole
     * fingerprint of the example fits, while a nullability change still separates.
     */
    @Test
    public void testWideMultiTableFingerprintStaysWithinThePersistedWidth() {
        Map<String, TableIf> tables = new LinkedHashMap<>();
        StringBuilder sql = new StringBuilder("SELECT 1 FROM internal.spm_db.t_wide_0");
        tables.put("t_wide_0", wideTable("t_wide_0", 100L, WIDE_COLUMNS, false));
        for (int table = 1; table < WIDE_TABLES; table++) {
            String name = "t_wide_" + table;
            tables.put(name, wideTable(name, 100L + table, WIDE_COLUMNS, false));
            sql.append(", internal.spm_db.").append(name);
        }
        LogicalPlan bindPlan = parse(sql.toString());
        ConnectContext ctx = contextResolving(tables);
        try {
            String fingerprint = SPMPlanTreeSupport.schemaFingerprintForCreate(
                    ctx, bindPlan, null, null);
            String[] entries = fingerprint.split(";");
            Assertions.assertEquals(WIDE_TABLES, entries.length,
                    "one entry per resolved table: " + fingerprint);
            for (String entry : entries) {
                Assertions.assertTrue(entry.length() < 128,
                        "a table entry must stay small regardless of its column count: "
                                + entry.length() + " chars");
            }
            Assertions.assertTrue(fingerprint.length() < FINGERPRINT_COLUMN_WIDTH,
                    "the reviewer's example must fit VARCHAR(" + FINGERPRINT_COLUMN_WIDTH
                            + "), got " + fingerprint.length());

            // the digest section still discriminates: flipping ONE column's nullability
            Map<String, TableIf> changed = new LinkedHashMap<>();
            for (int table = 0; table < WIDE_TABLES; table++) {
                String name = "t_wide_" + table;
                changed.put(name, wideTable(name, 100L + table, WIDE_COLUMNS, true));
            }
            String changedFingerprint = SPMPlanTreeSupport.schemaFingerprintForCreate(
                    contextResolving(changed), bindPlan, null, null);
            Assertions.assertNotEquals(fingerprint, changedFingerprint,
                    "a nullability change must still change the fingerprint");
            Assertions.assertFalse(SPMPlanTreeSupport.schemaFingerprintEquivalent(
                            fingerprint, changedFingerprint),
                    "the post-plan comparison must reject it as well");
            Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprintEquivalent(
                            fingerprint, fingerprint),
                    "an unchanged schema stays equivalent");
        } finally {
            ConnectContext.remove();
        }
    }
}
