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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Round-77 review fixes on the capture side.
 *
 * <ul>
 *   <li>#1 the function-identity and hint collectors now traverse {@code * REPLACE(...)}
 *       payloads, which live OUTSIDE {@code UnboundStar.children()}: a manual plan replacing with
 *       {@code db2.f(k)} used to compare equal to a bind text naming {@code db1.f(k)};</li>
 *   <li>#2 a multi-part slot is resolved as a RELATION-qualified column only when its leading part
 *       names a relation of the caller's block - a STRUCT field path ({@code s.a}) keeps its full
 *       name instead of being renamed to {@code a};</li>
 *   <li>#3 {@code NULLIF} joins the all-NULL conditional family: {@code nullif(NULL, NULL)} has
 *       NullType and a Boolean consumer erased its markers;</li>
 *   <li>#6 the schema fingerprint length-prefixes a table whose column NAMES contain the section
 *       separators, so two different schemas can no longer serialize identically.</li>
 * </ul>
 */
public class SPMRound77PlannerTest {

    // ==================== #1: * REPLACE payload functions ==============================

    /**
     * `SELECT * REPLACE(db1.f(k) AS x) FROM t` vs its db2 twin: the payload sits outside
     * children(), its toSql() omits the database, and both the slot and the function collectors
     * used to skip it - so the pair was accepted and the frozen replay executed db2 with the
     * caller's values.
     */
    @Test
    public void testStarReplacePayloadFunctionIdentityIsCompared() {
        LogicalPlan bind = parse("SELECT * REPLACE(db1.f(k) AS x) FROM t");
        LogicalPlan manual = parse("SELECT * REPLACE(db2.f(k) AS x) FROM t");

        Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(bind, manual,
                        "SELECT * REPLACE(db1.f(k) AS x) FROM t"),
                "another database's same-named function is a different call target even"
                        + " inside a star REPLACE payload");
    }

    /** The SAME qualified function on both sides is still accepted. */
    @Test
    public void testStarReplacePayloadWithSameFunctionIsAccepted() {
        LogicalPlan bind = parse("SELECT * REPLACE(db1.f(k) AS x) FROM t");
        LogicalPlan manual = parse("SELECT * REPLACE(db1.f(k) AS x) FROM t");

        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(bind, manual,
                        "SELECT * REPLACE(db1.f(k) AS x) FROM t"),
                "the same qualified function in the same payload position is one call target");
    }

    // ==================== #2: nested field reference labels ==========================

    /**
     * With t(s STRUCT&lt;a:INT&gt;), the analyzer binds `SELECT s.a` to
     * `Alias(ElementAt(s,'a'),'s.a')`: both ordinary execution and the frozen sink expose `s.a`,
     * so the alignment must leave it alone. Returning the last component renamed it to `a` even
     * for the IDENTICAL query.
     */
    @Test
    public void testNestedFieldReferenceKeepsItsFullHeader() {
        LogicalPlan user = parse("SELECT s.a, s.a + 1 AS z FROM t");
        LogicalPlan frozenReplay = parse("SELECT s.a, s.a + 1 AS z FROM t");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, user);
        String first = outputItemsOf(aligned).isEmpty() ? "" : outputItemsOf(aligned).get(0);
        Assertions.assertTrue(first.startsWith("s.a"),
                "a struct field path is not a relation-qualified column: the correct frozen"
                        + " header must survive an identical replay, got: "
                        + outputItemsOf(aligned));
        Assertions.assertFalse(first.endsWith("AS a"),
                "the alignment must not rename s.a to a: " + outputItemsOf(aligned));
    }

    /** A real relation qualifier is still dropped (the repaired scalar-column case). */
    @Test
    public void testRelationQualifiedColumnStillDropsItsQualifier() {
        LogicalPlan user = parse("SELECT t.k FROM t");
        LogicalPlan frozenReplay = parse("SELECT t.k FROM t");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, user);
        Assertions.assertFalse(outputItemsOf(aligned).isEmpty(),
                "the aligned tree still exposes its output list");
        Assertions.assertTrue(outputItemsOf(aligned).get(0).contains("k"),
                "`t.k` names column k of relation t, got: " + outputItemsOf(aligned));
    }

    // ==================== #3: NULLIF producers =======================================

    /**
     * `k > 0 OR nullif(NULL, NULL)`: NullIf.customSignature returns the arguments' common
     * NullType, so the analyzer replaces the whole call with a typed constant NULL and both
     * markers vanished - the matching `nullif(TRUE, NULL)` variant then had nothing to
     * substitute.
     */
    @Test
    public void testAllNullNullIfOperandParameterizesBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR nullif(NULL, NULL)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int nullMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                nullMarkers++;
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof BooleanType,
                        "an argument of a Boolean NULLIF operand is parameterized BOOLEAN, got: "
                                + ((SpmConstVar) placeholder).getDataType());
            }
        }
        Assertions.assertEquals(2, nullMarkers,
                "the all-NULL NULLIF carries two argument markers: "
                        + builder.getPlaceholderExprs());
    }

    /** A non-all-NULL NULLIF keeps its argument types (the producer is BOUND, not erased). */
    @Test
    public void testNonNullNullIfKeepsItsMarkerTypes() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR nullif(k, NULL) IS NULL");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar
                    && ((SpmConstVar) marker.getPlaceholderExpr()).getValue()
                            instanceof NullLiteral) {
                Assertions.assertFalse(((SpmConstVar) marker.getPlaceholderExpr()).getDataType()
                                instanceof BooleanType,
                        "only an all-NULL NULLIF is a NullType producer");
            }
        }
    }

    // ==================== #6: schema fingerprint column boundaries ===================

    /**
     * Column names may contain ':' and ',' - the characters the schema section uses as its own
     * separators. Before: [k, `a:int,b`, c]; after a light RENAME: [k, a, `b:int,c`]. Both
     * serialized as exactly `k:int,a:int,b:int,c:int,`, so the table-ID-preserving rename kept
     * the fingerprint and an identical caller replayed a projection of removed columns.
     */
    @Test
    public void testAmbiguousColumnNamesCannotCollideInTheFingerprint() {
        String before = SPMPlanTreeSupport.schemaSectionForFingerprint(List.of(
                column("k", Type.INT), column("a:int,b", Type.INT), column("c", Type.INT)));
        String after = SPMPlanTreeSupport.schemaSectionForFingerprint(List.of(
                column("k", Type.INT), column("a", Type.INT), column("b:int,c", Type.INT)));

        Assertions.assertNotEquals(before, after,
                "two DIFFERENT schemas must not serialize identically: " + before);
    }

    /** An ordinary schema keeps the legacy encoding, so persisted baselines stay valid. */
    @Test
    public void testOrdinarySchemaKeepsTheLegacyFingerprint() {
        String section = SPMPlanTreeSupport.schemaSectionForFingerprint(List.of(
                column("k", Type.INT), column("v", Type.INT)));

        Assertions.assertEquals("k:int,v:int,", section,
                "a schema without separator characters keeps the legacy encoding exactly");
    }

    /** A schema with separators in a name switches the whole section to the marked form. */
    @Test
    public void testSeparatorNameSwitchesToTheMarkedEncoding() {
        String section = SPMPlanTreeSupport.schemaSectionForFingerprint(List.of(
                column("a,b", Type.INT)));

        Assertions.assertTrue(section.startsWith("#L"),
                "the length-prefixed form is marked so the two encodings cannot coincide: "
                        + section);
    }

    // ==================== #4: the capture-time function snapshot =====================

    /**
     * The bind text's FUNCTION entries are snapshotted before analysis; when the entries derived
     * at CREATE differ, the definitions changed inside the capture window (the frozen body was
     * inlined from the OLD definition) and nothing may be certified.
     */
    @Test
    public void testRedefinedBindFunctionIsRejectedAtCreate() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        LogicalPlan bind = parse("SELECT f(k) FROM t");

        Assertions.assertThrows(IllegalStateException.class,
                () -> SPMPlanTreeSupport.schemaFingerprintForCreateFromLockedEntries(
                        ctx, bind, "SELECT f(k) FROM t", Map.of(),
                        Set.of("fn:stale|udf|u=1")),
                "a function redefined while the plan was captured must fail the CREATE");
    }

    /** No snapshot (the pre-fix call shape) keeps the historical behaviour. */
    @Test
    public void testMissingSnapshotKeepsTheHistoricalFingerprintPath() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        LogicalPlan bind = parse("SELECT f(k) FROM t");

        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.schemaFingerprintForCreateFromLockedEntries(
                        ctx, bind, "SELECT f(k) FROM t", Map.of()));
    }

    // ==================== helpers ====================

    private static Column column(String name, Type type) {
        return new Column(name, type);
    }

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static List<String> outputItemsOf(LogicalPlan plan) {
        final List<String> items = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (items.isEmpty() && node instanceof LogicalProject) {
                for (NamedExpression item : ((LogicalProject<?>) node).getProjects()) {
                    items.add(item.toSql());
                }
            }
        });
        return items;
    }
}
