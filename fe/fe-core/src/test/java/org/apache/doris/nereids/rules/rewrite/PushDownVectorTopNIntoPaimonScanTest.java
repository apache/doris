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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.source.PaimonVectorSearch;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AnnTopNInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TVectorMetric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link PushDownVectorTopNIntoPaimonScan}.
 *
 * <p>The rule turns {@code TopN[dis] -> Project[dis := dist_fn(vec, q)] -> [Filter] -> PaimonScan}
 * into an index-only ANN scan that produces {@code __paimon_search_score_to_dis}, and replaces every
 * occurrence of {@code dist_fn(vec, q)} with that column's slot. No arithmetic is emitted: the BE
 * reader converts the score into the distance the function is defined to return while filling the
 * block. These tests drive the two rules directly instead of going through the planner, so no
 * catalog/BE is needed.
 */
public class PushDownVectorTopNIntoPaimonScanTest {

    private static final List<Literal> QUERY_VECTOR =
            ImmutableList.of(new FloatLiteral(1.0f), new FloatLiteral(2.0f));

    private ConnectContext ctx;
    private CascadesContext cascadesContext;

    @BeforeEach
    public void setUp() {
        // The rule reads enable_paimon_rust_reader / enable_file_scanner_v2 off the
        // thread-local ConnectContext. Deliberately no StatementContext:
        // MvccUtil.getSnapshotFromContext then short-circuits to Optional.empty(),
        // so the mocked table never needs a database/catalog.
        ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.getSessionVariable().setEnablePaimonRustReader(true);
        ctx.getSessionVariable().enableFileScannerV2 = true;
        ctx.setThreadLocalInfo();
        // Rule.transform only stuffs the context into a MatchingContext; the rule body ignores it.
        cascadesContext = Mockito.mock(CascadesContext.class);
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    // ------------------------------------------------------------------ positive cases

    @Test
    public void testL2AscPushedDown() {
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan),
                dis, true);

        Plan rewritten = applyRule(0, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFileScan newScan = (LogicalFileScan) project.child(0);

        // ANN info reached the scan, with the retrieval limit widened by the offset.
        Assertions.assertTrue(newScan.getAnnTopN().isPresent());
        AnnTopNInfo annTopN = newScan.getAnnTopN().get();
        Assertions.assertEquals("vec", annTopN.getColumnName());
        Assertions.assertEquals(15L, annTopN.getLimit());
        Assertions.assertArrayEquals(new float[] {1.0f, 2.0f}, annTopN.getQueryVector());
        // BE needs the metric to undo paimon-rust's score transform.
        Assertions.assertEquals(TVectorMetric.L2, annTopN.getMetric());

        // The distance column is appended as a virtual output column.
        Assertions.assertEquals(1, newScan.getVirtualColumns().size());
        Assertions.assertEquals(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN,
                newScan.getVirtualColumns().get(0).getName());

        // ... and it must actually reach the scan's OUTPUT, not just its virtualColumns
        // list. These are two different things: output is computed once and can be
        // served from a cache (LogicalProperties, or LogicalFileScan.cachedOutputs,
        // which short-circuits computeOutput() entirely). withVectorTopN therefore has
        // to invalidate both, and asserting only on getVirtualColumns() would pass even
        // if the new column never became visible to the parent project -- which is what
        // the plan actually consumes.
        Assertions.assertTrue(newScan.getOutput().contains(distanceSlotOf(newScan).toSlot()),
                () -> "distance slot missing from scan output: " + newScan.getOutput());
        Assertions.assertEquals(scan.getOutput().size() + 1, newScan.getOutput().size());

        // The pre-existing base columns must keep their ExprIds. The rule reparents the
        // EXISTING projections onto the new scan, so a regenerated ExprId is not a
        // cosmetic renumbering -- it substitutes a different slot identity and leaves the
        // project referencing something the scan no longer produces. This fails if
        // withVectorTopN lets computeOutput() rebuild the output: computeOutput calls
        // exprIdGenerator.getNextId() per column with no reuse cache.
        Assertions.assertEquals(
                scan.getOutput().stream().map(Slot::getExprId).collect(Collectors.toList()),
                newScan.getOutput().subList(0, scan.getOutput().size()).stream()
                        .map(Slot::getExprId).collect(Collectors.toList()),
                () -> "base column exprIds changed: " + scan.getOutput()
                        + " -> " + newScan.getOutput());

        // The consequence the above protects, asserted directly: every slot the parent
        // project reads has to be produced by the scan underneath it.
        Set<ExprId> scanOutputIds = newScan.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toSet());
        for (NamedExpression projection : project.getProjects()) {
            for (Slot input : projection.getInputSlots()) {
                Assertions.assertTrue(scanOutputIds.contains(input.getExprId()),
                        () -> "project reads " + input + " which the scan does not output: "
                                + newScan.getOutput());
            }
        }

        // `dis` keeps its alias identity (the TopN order key still resolves) ...
        Alias newDis = (Alias) project.getProjects().get(1);
        Assertions.assertEquals(dis.getExprId(), newDis.getExprId());
        Assertions.assertEquals("dis", newDis.getName());
        // ... and its value is the bare distance slot: the BE reader already converted it
        // into the Euclidean distance, so no arithmetic is planned here.
        Assertions.assertEquals(distanceSlotOf(newScan), newDis.child());
        Assertions.assertFalse(containsDistanceFunction(newDis));
    }

    @Test
    public void testInnerProductDescPushedDown() {
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "inner_product"));
        SlotReference vec = vecSlot(scan);
        Alias dis = new Alias(new InnerProductApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan),
                dis, false);

        Plan rewritten = applyRule(0, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFileScan newScan = (LogicalFileScan) project.child(0);
        Assertions.assertTrue(newScan.getAnnTopN().isPresent());
        Assertions.assertEquals(TVectorMetric.DOT_PRODUCT, newScan.getAnnTopN().get().getMetric());

        // inner_product: the score IS the distance, so the alias wraps the bare distance slot.
        Alias newDis = (Alias) project.getProjects().get(1);
        Assertions.assertEquals(dis.getExprId(), newDis.getExprId());
        Assertions.assertEquals(distanceSlotOf(newScan), newDis.child());
    }

    @Test
    public void testOutputColumnNameKeepsTheFunctionCallWithoutAlias() {
        // SQL: SELECT id, l2_distance_approximate(vec, q) FROM xx ORDER BY l2_distance_approximate(vec, q)
        // No AS alias, so the binder builds an Alias whose name comes from the child's toSql
        // (nameFromChild=true). The result column header must be "l2_distance_approximate(...)",
        // NOT "__paimon_search_score_to_dis" -- the user asked for a distance function, not an
        // internal virtual column.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()));
        String expectedName = dis.getName();
        Assertions.assertTrue(dis.isNameFromChild());
        Assertions.assertTrue(expectedName.startsWith("l2_distance_approximate("), expectedName);

        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true);

        Plan rewritten = applyRule(0, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFileScan newScan = (LogicalFileScan) project.child(0);
        Alias newDis = (Alias) project.getProjects().get(1);

        // The header keeps the original function text, and the value is the bare
        // distance slot underneath it.
        Assertions.assertEquals(expectedName, newDis.getName());
        Assertions.assertNotEquals(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN, newDis.getName());
        Assertions.assertEquals(dis.getExprId(), newDis.getExprId());
        Assertions.assertEquals(distanceSlotOf(newScan), newDis.child());
        // The name was captured from the pre-rewrite child, so it must not have followed
        // the child down to the distance slot.
        Assertions.assertFalse(newDis.isNameFromChild());
        Assertions.assertEquals(expectedName, newDis.toSlot().getName());
    }

    @Test
    public void testFilterConjunctReusesScore() {
        // WHERE l2_distance_approximate(vec, q) < 10 ORDER BY the same expression.
        // Mirroring the internal-table rule, the conjunct is rewritten to reuse the index value
        // instead of recomputing the distance from the raw vector.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Expression distance = new L2DistanceApproximate(vec, queryVector());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(distance, new IntegerLiteral(10))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true);

        Plan rewritten = applyRule(1, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFilter<?> newFilter = (LogicalFilter<?>) project.child(0);
        LogicalFileScan newScan = (LogicalFileScan) newFilter.child(0);
        Assertions.assertTrue(newScan.getAnnTopN().isPresent());

        Assertions.assertEquals(1, newFilter.getConjuncts().size());
        Expression conjunct = newFilter.getConjuncts().iterator().next();
        Assertions.assertFalse(containsDistanceFunction(conjunct),
                "the filter must not recompute the distance from the raw vector");
        Assertions.assertEquals(
                new LessThan(distanceSlotOf(newScan), new IntegerLiteral(10)),
                conjunct);
    }

    @Test
    public void testNestedProjectionOccurrenceReusesScore() {
        // A second projection embeds the same distance call: dis2 := dist_fn(vec, q) * 2.
        // The nested occurrence is replaced while the surrounding alias keeps its identity.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        Alias doubled = new Alias(
                new Multiply(new L2DistanceApproximate(vec, queryVector()), new IntegerLiteral(2)),
                "dis2");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis, doubled), scan), dis, true);

        Plan rewritten = applyRule(0, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFileScan newScan = (LogicalFileScan) project.child(0);
        Alias newDoubled = (Alias) project.getProjects().get(2);
        Assertions.assertEquals(doubled.getExprId(), newDoubled.getExprId());
        Assertions.assertEquals("dis2", newDoubled.getName());
        Assertions.assertEquals(
                new Multiply(distanceSlotOf(newScan), new IntegerLiteral(2)),
                newDoubled.child());
        Assertions.assertFalse(containsDistanceFunction(newDoubled));
    }

    @Test
    public void testSelectStarDoesNotSurfaceDistanceColumn() {
        // SELECT * FROM xx ORDER BY l2_distance_approximate(vec, q)
        // The distance column must not appear in the result set. The rule appends it
        // to the scan output but deliberately does NOT extend the asterisk output,
        // so a downstream `SELECT *` expansion never picks it up.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), vecSlot(scan), dis), scan),
                dis, true);

        Plan rewritten = applyRule(0, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFileScan newScan = (LogicalFileScan) project.child(0);

        // The scan output has the distance column appended ...
        Assertions.assertTrue(newScan.getOutput().stream()
                        .anyMatch(s -> s.getName().equals(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN)),
                "distance slot must be in scan output");

        // ... but the asterisk output must NOT contain it.
        List<Slot> asterisk = newScan.getLogicalProperties().getAsteriskOutput();
        boolean distanceInAsterisk = asterisk.stream()
                .anyMatch(s -> s.getName().equals(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN));
        Assertions.assertFalse(distanceInAsterisk,
                "__paimon_search_score_to_dis must not appear in SELECT * output: " + asterisk);

        // The project also must not leak the internal column name as a visible output.
        for (NamedExpression proj : project.getProjects()) {
            Assertions.assertNotEquals(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN, proj.getName(),
                    "project output must not expose internal distance column name: " + proj);
        }
    }

    @Test
    public void testAliasedOutputColumnNameKeepsUserAlias() {
        // SQL: SELECT id, l2_distance_approximate(vec, q) AS dis FROM xx ORDER BY dis
        // The result column header must be "dis", NOT "__paimon_search_score_to_dis".
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true);

        Plan rewritten = applyRule(0, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFileScan newScan = (LogicalFileScan) project.child(0);

        Alias rewrittenDis = (Alias) project.getProjects().get(1);
        // The alias name stays "dis", not the internal column name.
        Assertions.assertEquals("dis", rewrittenDis.getName());
        Assertions.assertNotEquals(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN, rewrittenDis.getName());
        // The alias still wraps the bare distance slot (no arithmetic).
        Assertions.assertEquals(distanceSlotOf(newScan), rewrittenDis.child());
        Assertions.assertFalse(containsDistanceFunction(rewrittenDis));
        // The slot produced by the alias also carries the user-visible name.
        Assertions.assertEquals("dis", rewrittenDis.toSlot().getName());
    }

    @Test
    public void testPredicateOnAliasedDistanceIsRewritten() {
        // SELECT id, l2_distance_approximate(vec, q) AS dis
        // FROM xx WHERE l2_distance_approximate(vec, q) < 1 ORDER BY dis
        // The filter conjunct uses the bare distance function, which the rule
        // replaces with the index-produced distance slot.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Expression distanceExpr = new L2DistanceApproximate(vec, queryVector());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(distanceExpr, new FloatLiteral(1.0f))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true);

        Plan rewritten = applyRule(1, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFilter<?> newFilter = (LogicalFilter<?>) project.child(0);
        LogicalFileScan newScan = (LogicalFileScan) newFilter.child(0);
        Assertions.assertTrue(newScan.getAnnTopN().isPresent());

        // The filter conjunct now references the distance slot, not the function call.
        Assertions.assertEquals(1, newFilter.getConjuncts().size());
        Expression conjunct = newFilter.getConjuncts().iterator().next();
        Assertions.assertFalse(containsDistanceFunction(conjunct),
                "filter must not recompute the distance from the raw vector");
        Assertions.assertEquals(
                new LessThan(distanceSlotOf(newScan), new FloatLiteral(1.0f)),
                conjunct);

        // The alias output name stays "dis".
        Alias rewrittenDis = (Alias) project.getProjects().get(1);
        Assertions.assertEquals("dis", rewrittenDis.getName());
    }

    @Test
    public void testMergedSubqueryRewriteStillTriggers() {
        // SELECT id, dis FROM (
        //     SELECT id, l2_distance_approximate(vec, q) AS dis FROM xx
        // ) WHERE dis < 1 ORDER BY dis
        //
        // After MergeProjects / MergeFilters runs in the planner, the inner and outer
        // projects collapse, and the filter's reference to the `dis` alias is resolved
        // back to the underlying L2DistanceApproximate. The rule then sees:
        //   TopN -> Project[id, dis := l2_distance_approximate(vec,q)]
        //        -> Filter[l2_distance_approximate(vec,q) < 1]
        //        -> PaimonScan
        // which matches the second rule variant (with filter).
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Expression distanceExpr = new L2DistanceApproximate(vec, queryVector());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(distanceExpr, new FloatLiteral(1.0f))), scan);
        Alias dis = new Alias(distanceExpr, "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true);

        Plan rewritten = applyRule(1, topN);
        Assertions.assertNotSame(topN, rewritten);

        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFilter<?> newFilter = (LogicalFilter<?>) project.child(0);
        LogicalFileScan newScan = (LogicalFileScan) newFilter.child(0);

        // ANN rewrite fired.
        Assertions.assertTrue(newScan.getAnnTopN().isPresent());
        Assertions.assertEquals(TVectorMetric.L2, newScan.getAnnTopN().get().getMetric());

        // The filter now uses the distance slot.
        Expression conjunct = newFilter.getConjuncts().iterator().next();
        Assertions.assertFalse(containsDistanceFunction(conjunct));
        Assertions.assertEquals(
                new LessThan(distanceSlotOf(newScan), new FloatLiteral(1.0f)),
                conjunct);

        // The alias keeps its user name "dis" and wraps the distance slot.
        Alias rewrittenDis = (Alias) project.getProjects().get(1);
        Assertions.assertEquals("dis", rewrittenDis.getName());
        Assertions.assertEquals(distanceSlotOf(newScan), rewrittenDis.child());
    }

    // ------------------------------------------------------------------ negative cases

    @Test
    public void testRustReaderDisabledNotPushedDown() {
        // Only the BE paimon-rust reader can execute the index-only search.
        ctx.getSessionVariable().setEnablePaimonRustReader(false);
        assertNotPushedDown(l2Plan("l2", true));
    }

    @Test
    public void testFileScannerV2DisabledNotPushedDown() {
        // FileScannerV1 cannot select the paimon-rust reader, so an ANN request would be
        // one the selected scanner cannot consume.
        ctx.getSessionVariable().enableFileScannerV2 = false;
        assertNotPushedDown(l2Plan("l2", true));
    }

    @Test
    public void testMetricMismatchNotPushedDown() {
        // inner_product_approximate over an l2 index would return a different ranking.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        Alias dis = new Alias(new InnerProductApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, false));
    }

    @Test
    public void testL2DescNotPushedDown() {
        // l2 top-N is only an ANN search when ordered ascending (nearest first).
        assertNotPushedDown(l2Plan("l2", false));
    }

    @Test
    public void testInnerProductAscNotPushedDown() {
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "inner_product"));
        Alias dis = new Alias(new InnerProductApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testCosineMetricNotPushedDown() {
        // v1 supports l2 / inner_product only.
        assertNotPushedDown(l2Plan("cosine", true));
    }

    @Test
    public void testNoVectorIndexNotPushedDown() {
        LogicalFileScan scan = paimonScan(ImmutableMap.of());
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testUnindexedColumnNotPushedDown() {
        // The index covers `vec`, but the query orders by a distance over `vec2`.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec2 = (SlotReference) scan.getOutput().get(2);
        Assertions.assertEquals("vec2", vec2.getName());
        Alias dis = new Alias(new L2DistanceApproximate(vec2, queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testNonConstantQueryVectorNotPushedDown() {
        // The query vector must be a literal to be handed to the index.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        SlotReference vec2 = (SlotReference) scan.getOutput().get(2);
        Alias dis = new Alias(new L2DistanceApproximate(vec, vec2), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testOrderKeyNotFromProjectionNotPushedDown() {
        // Ordering by a plain column is not a vector top-N.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference id = idSlot(scan);
        LogicalProject<Plan> project = new LogicalProject<>(ImmutableList.of(id), scan);
        assertNotPushedDown(new LogicalTopN<>(
                ImmutableList.of(new OrderKey(id, true, true)), 10, 5, project));
    }

    // ------------------------------------------------------------------ helpers

    private Plan applyRule(int ruleIndex, LogicalTopN<Plan> topN) {
        Rule rule = new PushDownVectorTopNIntoPaimonScan().buildRules().get(ruleIndex);
        List<Plan> result = rule.transform(topN, cascadesContext);
        Assertions.assertEquals(1, result.size());
        return result.get(0);
    }

    /** A no-op rule returns the origin plan unchanged (see PatternMatcher#toRule). */
    private void assertNotPushedDown(LogicalTopN<Plan> topN) {
        int ruleIndex = topN.child().child(0) instanceof LogicalFilter ? 1 : 0;
        Assertions.assertSame(topN, applyRule(ruleIndex, topN));
    }

    private LogicalTopN<Plan> l2Plan(String metric, boolean asc) {
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", metric));
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        return topN(new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, asc);
    }

    private LogicalTopN<Plan> topN(LogicalProject<? extends Plan> project, Alias orderBy, boolean asc) {
        // limit 10 offset 5 -> the scan must retrieve 15 candidates.
        return new LogicalTopN<>(ImmutableList.of(new OrderKey(orderBy.toSlot(), asc, asc)),
                10, 5, project);
    }

    /**
     * A scan over a mocked Paimon table {@code (id INT, vec ARRAY<FLOAT>, vec2 ARRAY<FLOAT>)}
     * whose Paimon options are exactly {@code options}.
     */
    private LogicalFileScan paimonScan(Map<String, String> options) {
        List<Column> schema = ImmutableList.of(
                new Column("id", Type.INT, true),
                new Column("vec", new ArrayType(Type.FLOAT), true),
                new Column("vec2", new ArrayType(Type.FLOAT), true));

        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("paimon_tbl");
        Mockito.when(table.getBaseSchema()).thenReturn(schema);
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getFullSchema(Mockito.<Optional<org.apache.doris.datasource.mvcc.MvccSnapshot>>any()))
                .thenReturn(schema);
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.NOT_PRUNED);

        Table paimonTable = Mockito.mock(Table.class);
        Mockito.when(paimonTable.options()).thenReturn(options);
        Mockito.when(table.getPaimonTable(Mockito.<Optional<org.apache.doris.datasource.mvcc.MvccSnapshot>>any()))
                .thenReturn(paimonTable);

        return new LogicalFileScan(StatementScopeIdGenerator.newRelationId(), table,
                ImmutableList.of("paimon_ctl", "paimon_db"), ImmutableList.of(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private SlotReference idSlot(LogicalFileScan scan) {
        return (SlotReference) scan.getOutput().get(0);
    }

    /** Must come from the scan output: the rule requires the slot's original column/table. */
    private SlotReference vecSlot(LogicalFileScan scan) {
        return (SlotReference) scan.getOutput().get(1);
    }

    private ArrayLiteral queryVector() {
        return new ArrayLiteral(QUERY_VECTOR);
    }

    private NamedExpression distanceSlotOf(LogicalFileScan scan) {
        return scan.getVirtualColumns().get(0);
    }

    private boolean containsDistanceFunction(Expression expression) {
        return expression.anyMatch(e ->
                e instanceof L2DistanceApproximate || e instanceof InnerProductApproximate);
    }
}
