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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintOrdered;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Twenty-fourth review round: the fallback-replay hint contract, the catalog-aware
 * bind-table lookup of the schema fingerprint and the linear subquery walk.
 *
 * - {@link SPMPlanTreeSupport#stripSelectHints} removed EVERY LogicalSelectHint from the
 *   in-memory fallback tree, so a plan SQL with {@code /*+ ORDERED *}{@code /} (stored as
 *   the authored fallback whenever the decompiler rejects a node, e.g. the
 *   PhysicalAssertNumRows of a scalar subquery) lost the join-order hint the baseline
 *   exists to enforce. Only the captured SET_VAR payloads may go.
 * - the db-qualified suffix branch of the locked-table lookup ignored the CATALOG: a
 *   baseline binding cat1.db.t to a plan over cat2.db.t fingerprinted cat2's table, and
 *   the correct cat1 fingerprint then failed the pre-match containment check - the
 *   baseline could never match again.
 * - a LogicalFilter exposes its predicate's subquery plans BOTH through extraPlans() and
 *   through the predicate expression, so a nested-subquery chain of depth n was walked
 *   2^n times (and every repeated table resolved again after the match deadline).
 */
public class SPMRound24SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** The LogicalSelectHint nodes of a tree (empty when no hint survived). */
    private static List<LogicalSelectHint<?>> selectHints(LogicalPlan plan) {
        List<LogicalSelectHint<?>> hints = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (node instanceof LogicalSelectHint) {
                hints.add((LogicalSelectHint<?>) node);
            }
        });
        return hints;
    }

    // ==================== #2: plan-selection hints survive the SET_VAR strip ====================

    @Test
    public void testPlanSelectionHintsSurviveTheSetVarStrip() {
        LogicalPlan plan = parse("SELECT /*+ SET_VAR(exec_mem_limit=1024) ORDERED */ k"
                + " FROM internal.spm_db.t1 JOIN internal.spm_db.t2 ON t1.k = t2.k");
        LogicalPlan stripped = SPMPlanTreeSupport.stripSelectHints(plan);
        List<LogicalSelectHint<?>> hints = selectHints(stripped);
        Assertions.assertEquals(1, hints.size(),
                "the ORDERED hint must stay wrapped: " + stripped.treeString());
        List<SelectHint> kept = hints.get(0).getHints();
        Assertions.assertTrue(kept.stream().anyMatch(h -> h instanceof SelectHintOrdered),
                "ORDERED is the plan the baseline was created to enforce: " + kept);
        Assertions.assertTrue(kept.stream().noneMatch(h -> h instanceof SelectHintSetVar),
                "the captured SET_VAR payload belongs to the CREATOR's session and must go: "
                        + kept);
        // LEADING(...) is a plan-selection hint as well
        LogicalPlan leading = SPMPlanTreeSupport.stripSelectHints(parse(
                "SELECT /*+ LEADING(t2, t1) */ k FROM internal.spm_db.t1 JOIN internal.spm_db.t2"
                        + " ON t1.k = t2.k"));
        List<LogicalSelectHint<?>> leadingHints = selectHints(leading);
        Assertions.assertEquals(1, leadingHints.size(), leading.treeString());
        Assertions.assertTrue(leadingHints.get(0).getHints().stream()
                        .anyMatch(h -> h instanceof SelectHintLeading),
                "LEADING must survive too: " + leadingHints.get(0).getHints());
        // a SET_VAR-only tree keeps its original, hint-free shape
        Assertions.assertTrue(selectHints(SPMPlanTreeSupport.stripSelectHints(parse(
                "SELECT /*+ SET_VAR(exec_mem_limit=1024) */ k FROM internal.spm_db.t1")))
                .isEmpty(), "a wrapper left without hints must be dropped entirely");
    }

    @Test
    public void testNestedSetVarIsStrippedWhileThePlanHintsSurvive() {
        LogicalPlan nested = parse("SELECT /*+ ORDERED */ x.k FROM (SELECT /*+"
                + " SET_VAR(time_zone='+08:00') */ k FROM internal.spm_db.t1) x"
                + " JOIN internal.spm_db.t2 ON x.k = t2.k");
        LogicalPlan stripped = SPMPlanTreeSupport.stripSelectHints(nested);
        List<LogicalSelectHint<?>> hints = selectHints(stripped);
        Assertions.assertEquals(1, hints.size(),
                "only the root's ORDERED wrapper may remain: " + stripped.treeString());
        Assertions.assertTrue(hints.get(0).getHints().stream()
                        .anyMatch(h -> h instanceof SelectHintOrdered),
                "the nested SET_VAR must go while ORDERED stays: " + hints.get(0).getHints());
        Assertions.assertTrue(stripped.treeString().toUpperCase(java.util.Locale.ROOT)
                        .contains("ORDERED"), stripped.treeString());
    }

    // ==================== #3: the bind-table lookup verifies the catalog ====================

    /** A table mock carrying its own database / catalog identity. */
    private static TableIf table(String catalogName, String dbFullName, String name, long id) {
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getFullName()).thenReturn(dbFullName);
        if (catalogName != null) {
            CatalogIf catalog = Mockito.mock(CatalogIf.class);
            Mockito.when(catalog.getName()).thenReturn(catalogName);
            Mockito.when(database.getCatalog()).thenReturn(catalog);
        }
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getBaseSchema()).thenReturn(List.of(
                new Column("k", org.apache.doris.catalog.Type.INT)));
        return table;
    }

    /** A physical catalog relation carrying its OWN metadata snapshot. */
    private static Plan planOver(TableIf table) {
        org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation relation =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation.class);
        Mockito.when(relation.getTable()).thenReturn(table);
        return relation;
    }

    /** A session whose statement context resolves every bind relation to {@code real}. */
    private static ConnectContext contextResolvingTo(TableIf real) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenReturn(real);
        ctx.setStatementContext(statementContext);
        return ctx;
    }

    /**
     * The planned map carries only the db-qualified key (the planned table's database
     * exposes no catalog-prefixed full name): stripping the CATALOG off the bind
     * qualifier used to hit a table of ANOTHER catalog that happens to share database +
     * table name. The stored entry then pinned cat2's table id, the correct cat1
     * fingerprint failed the pre-match containment check and the baseline never matched
     * again.
     */
    @Test
    public void testBindTableLookupVerifiesTheCatalog() {
        TableIf plannedOtherCatalog = table("cat2", "db", "t", 8L);
        TableIf realBindTable = table("cat1", "db", "t", 7L);
        ConnectContext ctx = contextResolvingTo(realBindTable);
        LogicalPlan bind = parse("SELECT k FROM cat1.db.t");

        String stored = SPMPlanTreeSupport.schemaFingerprintForCreate(
                ctx, bind, planOver(plannedOtherCatalog), null);
        Assertions.assertTrue(stored.contains("t|7|"),
                "the bind entry must pin the REAL cat1 table (the plan side legitimately"
                        + " contributes cat2's entry): " + stored);
        Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                        stored, SPMPlanTreeSupport.schemaFingerprint(ctx, bind)),
                "the next query's correct bind fingerprint must be contained: " + stored);
    }

    /** The catalog check must not over-reject: same catalog + database still pins the snapshot. */
    @Test
    public void testBindTableLookupKeepsThePlannedSnapshotOfTheSameCatalog() {
        TableIf planned = table("internal", "db", "t", 18L);
        TableIf realBindTable = table("internal", "db", "t", 17L);
        ConnectContext ctx = contextResolvingTo(realBindTable);
        LogicalPlan bind = parse("SELECT k FROM internal.db.t");

        String stored = SPMPlanTreeSupport.schemaFingerprintForCreate(
                ctx, bind, planOver(planned), null);
        Assertions.assertTrue(stored.contains("t|18|"),
                "the under-lock planned snapshot must be pinned: " + stored);
        Assertions.assertFalse(stored.contains("t|17|"),
                "the cache-less re-resolution must not leak into the fingerprint: " + stored);
    }

    // ==================== #5: every subquery plan is walked once ====================

    /**
     * A nested scalar-subquery chain used to be walked 2^n times (each LogicalFilter
     * exposes its predicate's subquery through extraPlans() AND through the predicate
     * expression); the bind-side fingerprint resolved every repeated table again, the
     * last of them after the match deadline. The walker now visits each plan object once.
     */
    @Test
    public void testNestedScalarSubqueriesAreWalkedOnce() {
        int depth = 12;
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM internal.spm_db.t0 WHERE k = ");
        for (int i = 0; i < depth; i++) {
            sql.append("(SELECT k FROM internal.spm_db.t").append(i + 1).append(" WHERE k = ");
        }
        sql.append("1");
        for (int i = 0; i < depth; i++) {
            sql.append(')');
        }
        LogicalPlan plan = parse(sql.toString());

        AtomicInteger resolutions = new AtomicInteger();
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenAnswer(invocation -> {
                    resolutions.incrementAndGet();
                    return table("internal", "internal.spm_db", "t", 7L);
                });
        ctx.setStatementContext(statementContext);

        String fingerprint = SPMPlanTreeSupport.schemaFingerprintForCreate(ctx, plan, null, null);
        Assertions.assertFalse(fingerprint.isEmpty(), "the walk must resolve the tables");
        Assertions.assertTrue(resolutions.get() <= 4 * (depth + 1),
                "each subquery plan may be walked once per scope - " + resolutions.get()
                        + " resolutions for depth " + depth
                        + " (the exponential walk would resolve 2^depth times)");
    }
}
