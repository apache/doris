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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.source.PaimonVectorSearch;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.AnnTopNInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TVectorMetric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import mockit.MockUp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
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

    private static SystemInfoService systemInfo;
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
        // The rewrite's pre-flight (PaimonVectorSearch.vectorScanEligible) requires a
        // rust-capable BE via the all-backend capability negotiation, mirroring
        // PaimonScanNodeTest's routing fixture — the vector protocol rides the
        // same bit, since the rust reader and the vector protocol ship together.
        Backend rustBackend = GsonUtils.GSON.fromJson(
                "{\"supportsPaimonRustReader\":true}",
                Backend.class);
        systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getAllClusterBackends(true))
                .thenReturn(ImmutableList.of(rustBackend));
        new MockUp<Env>() {
            @mockit.Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfo;
            }
        };
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
    public void testDistancePredicateFilterNotPushedDown() {
        // WHERE l2_distance_approximate(vec, q) < 10 ORDER BY the same expression.
        // A predicate on the distance cannot ride into the BE vector search (the
        // synthetic distance column is not in the paimon-rust predicate converter's
        // field registry), so it would run as a residual AFTER the bounded top-N
        // fetch -- with candidate distances 0, 2 and 3, a `dis > 1 LIMIT 1` would
        // fetch only the distance-0 row, filter it out and return nothing instead
        // of the distance-2 row. An upper bound happens to be monotone in the
        // ASC order, but the rewrite cannot rely on residual monotonicity, so the
        // rewrite declines and the query computes the distance in Doris.
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

        assertNotPushedDown(topN);
    }

    @Test
    public void testSimpleColumnFilterStillPushedDown() {
        // WHERE tag < 3 ORDER BY dis: a comparison on a plain supported-type column
        // provably rides into the BE vector search (PaimonRustPredicateConverter
        // pushes comparisons on BOOLEAN..BIGINT/STRING/DATE/DATEV2 columns), so the
        // bounded top-N sees the predicate and the rewrite stays sound.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2",
                "deletion-vectors.enabled", "true"));
        SlotReference vec = vecSlot(scan);
        SlotReference tag = (SlotReference) scan.getOutput().get(3);
        Assertions.assertEquals("tag", tag.getName());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(tag, new IntegerLiteral(3))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true);

        Plan rewritten = applyRule(1, topN);
        Assertions.assertNotSame(topN, rewritten);
        LogicalProject<?> project = (LogicalProject<?>) rewritten.child(0);
        LogicalFileScan newScan = (LogicalFileScan) ((LogicalFilter<?>) project.child(0)).child(0);
        Assertions.assertTrue(newScan.getAnnTopN().isPresent(),
                "the simple column filter must not block the bounded ANN rewrite");
    }

    @Test
    public void testUnsupportedColumnTypeFilterNotPushedDown() {
        // WHERE score < 1.5 with score being a DOUBLE column: the paimon-rust
        // predicate converter keeps IEEE-ordering types (DOUBLE/FLOAT/DECIMAL/
        // VARCHAR/DATETIME) residual, and a residual after the bounded top-N is
        // unsound, so the rewrite declines.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        SlotReference score = (SlotReference) scan.getOutput().get(4);
        Assertions.assertEquals("score", score.getName());
        Assertions.assertEquals(Type.DOUBLE, score.getOriginalColumn().get().getType());
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(score, new FloatLiteral(1.5f))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true);

        assertNotPushedDown(topN);
    }

    @Test
    public void testExpressionFilterNotPushedDown() {
        // WHERE id + 1 < 10: an arithmetic expression cannot ride into the vector
        // search, so the rewrite declines instead of filtering after the limit.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        SlotReference id = idSlot(scan);
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(
                        new Add(id, new IntegerLiteral(1)), new IntegerLiteral(10))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        LogicalTopN<Plan> topN = topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true);

        assertNotPushedDown(topN);
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
    public void testPredicateOnAliasedDistanceNotPushedDown() {
        // SELECT id, l2_distance_approximate(vec, q) AS dis
        // FROM xx WHERE l2_distance_approximate(vec, q) < 1 ORDER BY dis
        // The predicate is on the distance: it cannot ride into the BE vector
        // search (the synthetic distance column is outside the paimon-rust
        // predicate converter's field registry), and applying it as a residual
        // after the bounded top-N fetch is unsound, so the rewrite declines.
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

        assertNotPushedDown(topN);
    }

    @Test
    public void testMergedSubqueryRewriteStillTriggers() {
        // SELECT id, dis FROM (
        //     SELECT id, l2_distance_approximate(vec, q) AS dis FROM xx
        // ) WHERE tag < 3 ORDER BY dis
        //
        // After MergeProjects / MergeFilters runs in the planner, the inner and outer
        // projects collapse. The rule then sees:
        //   TopN -> Project[id, dis := l2_distance_approximate(vec,q)]
        //        -> Filter[tag < 3]
        //        -> PaimonScan
        // which matches the second rule variant (with filter). The simple column
        // comparison rides into the vector search, so the rewrite stays sound.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2",
                "deletion-vectors.enabled", "true"));
        SlotReference vec = vecSlot(scan);
        SlotReference tag = (SlotReference) scan.getOutput().get(3);
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(tag, new IntegerLiteral(3))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
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

        // The filter keeps its column predicate (nothing to replace).
        Expression conjunct = newFilter.getConjuncts().iterator().next();
        Assertions.assertFalse(containsDistanceFunction(conjunct));
        Assertions.assertEquals(new LessThan(tag, new IntegerLiteral(3)), conjunct);

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

    @Test
    public void testIncrementalScanNotPushedDown() {
        // t@incr(...): the PK-vector search path has no incremental semantics (the
        // VectorScan builder has no incremental mode), so an incremental query must
        // keep its ordinary scan and compute the distance in Doris — a rewritten
        // vector split would silently return snapshot rows for a query that asked
        // for changes.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"),
                Optional.of(new TableScanParams("incr",
                        ImmutableMap.of("start", "1"), ImmutableList.of())));
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testFileCreationTimeScanNotPushedDown() {
        // t@options('scan.file-creation-time-millis'='...'): the ordinary path drops
        // manifest entries older than the bound, but the PK-vector search plans its
        // own splits and never consults that bound -- old files' nearest neighbors
        // would leak into the bounded result. Snapshot/tag selectors are resolved
        // into the table before the vector search and stay eligible; only the
        // file-level bound declines.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"),
                Optional.of(new TableScanParams("options",
                        ImmutableMap.of("scan.file-creation-time-millis", "1791793167062"),
                        ImmutableList.of())));
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testCreationTimeScanNotPushedDown() {
        // t@options('scan.creation-time-millis'='...'): when a snapshot exists past
        // the bound this resolves into an ordinary snapshot pin (safe), but when none
        // does it resolves into the SAME internal file-creation bound the vector
        // search never consults — which case applies depends on the snapshot
        // manager's state, so the raw key declines unconditionally (contrast with
        // scan.snapshot-id, which stays eligible).
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"),
                Optional.of(new TableScanParams("options",
                        ImmutableMap.of("scan.creation-time-millis", "1791793167062"),
                        ImmutableList.of())));
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testNullQueryVectorElementNotPushedDown() {
        // [CAST(NULL AS FLOAT), 0.0]: a NullLiteral's getDouble() is 0, so the
        // rewrite would silently search a zero vector and return rows for a
        // different input, while the ordinary path raises (the distance functions
        // reject NULL elements). Any null element declines.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        ArrayLiteral nullVector = new ArrayLiteral(ImmutableList.of(
                new NullLiteral(), new FloatLiteral(2.0f)));
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), nullVector), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testInPredicateWithNullOptionNotPushedDown() {
        // tag IN (1, 2, NULL): every option is a Literal so the shape whitelist
        // passes, but the BE converter rejects the null literal and with it the
        // WHOLE predicate, leaving the IN in the post-fetch residual — the bounded
        // fetch then returns the wrong rows. A null option never rides.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2",
                "deletion-vectors.enabled", "true"));
        SlotReference tag = (SlotReference) scan.getOutput().get(3);
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(ImmutableSet.of(
                new InPredicate(tag, ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2),
                        new NullLiteral()))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true));
    }

    @Test
    public void testInPredicateStillPushedDown() {
        // Control for the NULL-option case: tag IN (1, 2) with every option a
        // non-null literal on a supported column rides into the vector search.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2",
                "deletion-vectors.enabled", "true"));
        SlotReference tag = (SlotReference) scan.getOutput().get(3);
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(ImmutableSet.of(
                new InPredicate(tag, ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)))),
                scan);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        Plan rewritten = applyRule(1, topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true));
        Assertions.assertNotSame(scan, ((LogicalFileScan) ((LogicalFilter<?>)
                ((LogicalProject<?>) rewritten.child(0)).child(0)).child(0)));
        Assertions.assertTrue(((LogicalFileScan) ((LogicalFilter<?>)
                ((LogicalProject<?>) rewritten.child(0)).child(0)).child(0)).getAnnTopN().isPresent());
    }

    @Test
    public void testFilterWithoutDeletionVectorsNotPushedDown() {
        // WHERE tag < 3 on a table without deletion vectors: the predicate
        // converts fine, but paimon-rust's PkVectorSearchParams::resolve rejects
        // every data predicate unless deletion vectors are enabled and
        // merge-on-read is disabled — so the failure would land AFTER the
        // one-way rewrite, as a user-facing error on a query the ordinary path
        // answers. Decline instead (first-row tables allow the index without DV).
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference tag = (SlotReference) scan.getOutput().get(3);
        LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(tag, new IntegerLiteral(3))), scan);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true));
    }

    @Test
    public void testSyntheticDistanceColumnCollisionNotPushedDown() {
        // __paimon_search_score_to_dis is also a legal paimon user column name; a
        // table that HAS such a column collides with the synthetic slot's
        // block-column matching, so the rewrite declines.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"),
                Optional.empty(),
                ImmutableList.of("id", "vec", PaimonVectorSearch.SEARCH_DISTANCE_COLUMN));
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testPrunedComplexProjectionNotPushedDown() {
        // Project(pmap['k'], dis): nested pruning materializes the scan tuple with
        // a REDUCED type for pmap, but the vector reader projects only root names
        // and returns the full Arrow value, whose SerDe binds children by ordinal —
        // the wrong child comes back. Only a whole-column (bare slot) projection
        // keeps the rewrite sound.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference pmap = (SlotReference) scan.getOutput().get(6);
        Assertions.assertEquals("pmap", pmap.getName());
        Assertions.assertTrue(pmap.getDataType().isComplexType());
        Alias field = new Alias(new ElementAt(pmap, new StringLiteral("k")), "pk");
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), field, dis), scan), dis, true));
    }

    @Test
    public void testWholeComplexProjectionStillPushedDown() {
        // Control for the pruned case: the complex column rides as a whole column
        // (bare slot output), which nested pruning leaves untouched.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference pmap = (SlotReference) scan.getOutput().get(6);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        Plan rewritten = applyRule(0, topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), pmap, dis), scan), dis, true));
        Assertions.assertNotSame(scan, ((LogicalFileScan) ((LogicalProject<?>) rewritten.child(0)).child(0)));
        Assertions.assertTrue(((LogicalFileScan) ((LogicalProject<?>) rewritten.child(0)).child(0))
                .getAnnTopN().isPresent());
    }

    @Test
    public void testSnapshotScanStillPushedDown() {
        // t@options('scan.snapshot-id'='1'): snapshot selectors are resolved into
        // the table handle before the vector search plans its splits, so they do
        // not block the rewrite (contrast with the file-creation-time bound).
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"),
                Optional.of(new TableScanParams("options",
                        ImmutableMap.of("scan.snapshot-id", "1"), ImmutableList.of())));
        SlotReference vec = vecSlot(scan);
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        Plan rewritten = applyRule(0, topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
        Assertions.assertNotSame(scan, ((LogicalProject<?>) rewritten.child(0)).child(0));
        Assertions.assertTrue(((LogicalFileScan) ((LogicalProject<?>) rewritten.child(0))
                .child(0)).getAnnTopN().isPresent());
    }

    @Test
    public void testCastOnIndexedVectorNotPushedDown() {
        // l2_distance_approximate(CAST(vec AS ARRAY<DOUBLE>), q): a legal cast chain
        // can change vector values (an ARRAY<FLOAT> -> ARRAY<INT> -> ARRAY<FLOAT>
        // round trip loses precision), so searching the original vectors while the
        // query means the casted ones would change the ranking and the distance.
        // The rewrite only accepts a direct indexed column slot.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        Cast castVec = new Cast(vec, org.apache.doris.nereids.types.ArrayType.of(
                org.apache.doris.nereids.types.DoubleType.INSTANCE));
        Alias dis = new Alias(new L2DistanceApproximate(castVec, queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testVariantProjectionNotPushedDown() {
        // SELECT payload, dis ... ORDER BY dis with a VARIANT payload column: the
        // rust reader has no Variant decoder, so the scan node's projectedVariant
        // bound would reject the vector split -- and a vector split has no JNI
        // fallback, so the rewrite would turn a supported query into an error.
        // Pre-flight the projection and decline instead; the query then scans
        // through the ordinary path.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec = vecSlot(scan);
        SlotReference payload = (SlotReference) scan.getOutput().get(5);
        Assertions.assertEquals("payload", payload.getName());
        Assertions.assertTrue(payload.getOriginalColumn().get().getType().isVariantType());
        Alias dis = new Alias(new L2DistanceApproximate(vec, queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), payload, dis), scan), dis, true));
    }

    @Test
    public void testBranchTableWithDifferentMetricNotPushedDown() {
        // t@branch(b): the relation's bound table is the branch, which can carry an
        // independently configured index metric. The pre-flight must validate the
        // BOUND table: an l2 query against an inner-product branch would score with
        // the branch's metric and reorder with l2's inverse -- wrong results even
        // at full recall.
        // field names of the fixture schema (see below): the branch table's rowType
        // drives the same synthetic-distance-collision pre-flight as the main table.
        List<String> fieldNames = ImmutableList.of("id", "vec", "vec2", "tag", "score",
                "payload", "pmap");
        FileStoreTable branch = mockFileStoreTable(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "inner_product"), fieldNames, null);
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"), Optional.empty(), null, false,
                null, branch);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testBranchTableWithMatchingMetricStillPushedDown() {
        // Control: the bound branch table carries the same l2 metric, so the
        // rewrite must fire exactly as it does against the main table.
        List<String> fieldNames = ImmutableList.of("id", "vec", "vec2", "tag", "score",
                "payload", "pmap");
        FileStoreTable branch = mockFileStoreTable(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"), fieldNames, null);
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"), Optional.empty(), null, false,
                null, branch);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        Plan rewritten = applyRule(0, topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
        Assertions.assertTrue(((LogicalFileScan) ((LogicalProject<?>) rewritten.child(0))
                .child(0)).getAnnTopN().isPresent());
    }

    @Test
    public void testHistoricalSchemaEvolutionNotPushedDown() {
        // A TIMESTAMP(3) -> TIMESTAMP(9) widening with old files retained: the
        // vector reader's Arrow safe cast NULLs out-of-range values the ordinary
        // path preserves, so the rewrite must decline and let the query run
        // ordinary (see PaimonRustReaderCapabilities.hasRustIncompatibleSchemaHistory).
        TableSchema widened = new TableSchema(4,
                Collections.singletonList(new DataField(0, "ts", new TimestampType(9))),
                0, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null);
        TableSchema millis = new TableSchema(5,
                Collections.singletonList(new DataField(0, "ts", new TimestampType(3))),
                0, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null);
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"), Optional.empty(), null, false,
                ImmutableList.of(widened, millis), null);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
    }

    @Test
    public void testNonMovableProjectExpressionNotPushedDown() {
        // c := assert_true(id > 0): on the ordinary path the Project evaluates before
        // the TopN and raises for a far row the bounded fetch would never deliver;
        // withVectorTopN truncates first, so the assertion never fires and the query
        // silently succeeds. A NoneMovableFunction in the projection keeps the
        // ordinary path.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        Alias guard = new Alias(
                new AssertTrue(new GreaterThan(idSlot(scan), new IntegerLiteral(0)),
                        new StringLiteral("bad id")),
                "c");
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(new LogicalProject<>(
                ImmutableList.of(idSlot(scan), guard, dis), scan), dis, true));
    }

    @Test
    public void testPrunedBareComplexSlotNotPushedDown() {
        // An outer field projection can leave an already-pruned bare slot inside the
        // ANN Project (nested pruning materialized its type to the accessed subset).
        // The desc-side backstop would fail loudly, so the pre-flight declines: a
        // bare slot only rides when its type still equals the original column's.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference pmap = (SlotReference) scan.getOutput().get(6);
        Assertions.assertEquals("pmap", pmap.getName());
        SlotReference pruned = new SlotReference(
                StatementScopeIdGenerator.getExprIdGenerator().getNextId(), "pmap",
                org.apache.doris.nereids.types.ArrayType.of(
                        org.apache.doris.nereids.types.DoubleType.INSTANCE),
                pmap.nullable(), ImmutableList.of(), scan.getTable(),
                pmap.getOriginalColumn().get(), scan.getTable(),
                pmap.getOriginalColumn().get(), ImmutableList.of());
        Alias prunedAlias = new Alias(pruned, "p");
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(new LogicalProject<>(
                ImmutableList.of(idSlot(scan), prunedAlias, dis), scan), dis, true));
    }

    @Test
    public void testInPredicateBeyondConverterCapNotPushedDown() {
        // With expr_children_limit raised, an IN can exceed the BE rust predicate
        // converter's 16-bit child count: the converter drops the whole predicate,
        // and the residual re-applies it AFTER the bounded Top-K — the truncation
        // the whitelist exists to prevent. Options whose slot+options children
        // exceed the cap keep the ordinary path.
        int originalChildrenLimit = Config.expr_children_limit;
        Config.expr_children_limit = 0xFFFF + 64;
        LogicalFileScan scan;
        SlotReference tag;
        try {
            scan = paimonScan(ImmutableMap.of(
                    "pk-vector.index.columns", "vec",
                    "fields.vec.pk-vector.distance.metric", "l2",
                    "deletion-vectors.enabled", "true"));
            tag = (SlotReference) scan.getOutput().get(3);
            List<Expression> options = new java.util.ArrayList<>();
            for (int i = 0; i < 0xFFFF; i++) {
                options.add(new IntegerLiteral(i));
            }
            LogicalFilter<LogicalFileScan> filter =
                    new LogicalFilter<>(ImmutableSet.of(new InPredicate(tag, options)), scan);
            Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
            assertNotPushedDown(topN(
                    new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), filter), dis, true));
        } finally {
            Config.expr_children_limit = originalChildrenLimit;
        }
    }

    @Test
    public void testReservedMaterializerNamesNotPushedDown() {
        // __paimon_search_score and _PKEY_VECTOR_POSITION are reserved by the rust
        // materializer: a user column named either makes the rust table open fail
        // after the one-way rewrite, so the pre-flight declines for both.
        for (String reserved : ImmutableList.of("__paimon_search_score", "_PKEY_VECTOR_POSITION")) {
            LogicalFileScan scan = paimonScan(ImmutableMap.of(
                    "pk-vector.index.columns", "vec",
                    "fields.vec.pk-vector.distance.metric", "l2"),
                    Optional.empty(),
                    ImmutableList.of("id", "vec", reserved));
            Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
            assertNotPushedDown(topN(
                    new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
        }
    }

    @Test
    public void testNanosecondColumnNotPushedDown() {
        // A TIMESTAMP_NS column in the projection keeps the reader family that
        // preserves nanoseconds (JNI); a vector split has no fallback, so the
        // rewrite declines. The schema-history gate does not subsume this —
        // the table carries the column in its CURRENT schema, no evolution.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference tsns = (SlotReference) scan.getOutput().get(7);
        Assertions.assertEquals("tsns", tsns.getName());
        Assertions.assertTrue(tsns.getOriginalColumn().get().getType().isTimeStampNs());
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(new LogicalProject<>(
                ImmutableList.of(idSlot(scan), new Alias(tsns, "t"), dis), scan), dis, true));
    }

    @Test
    public void testDistinctDistanceCallNotExempted() {
        // extra := inner_product(vec2, q): only the order-key call is replaced by
        // the synthetic slot; a DISTINCT distance call stays in the projection and
        // keeps its complex (ARRAY<FLOAT>) slot, so the whole-column guard applies
        // to it — the rewrite declines instead of shipping a vector split whose
        // projection rust cannot materialize.
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2"));
        SlotReference vec2 = (SlotReference) scan.getOutput().get(2);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        Alias extra = new Alias(new InnerProductApproximate(vec2, queryVector()), "extra");
        assertNotPushedDown(topN(new LogicalProject<>(
                ImmutableList.of(idSlot(scan), extra, dis), scan), dis, true));
    }

    @Test
    public void testOrcEraRetainedFilesNotPushedDown() {
        // The table is Parquet NOW, but an ORC schema era with TIMESTAMP_LTZ left
        // retained files: setPaimonParams still detects ORC files, so the rewrite
        // must decline even though the current format is Parquet.
        TableSchema orcEra = new TableSchema(4,
                Collections.singletonList(new DataField(0, "t", new LocalZonedTimestampType())),
                0, Collections.emptyList(), Collections.emptyList(),
                ImmutableMap.of("file.format", "orc"), null);
        LogicalFileScan scan = paimonScan(ImmutableMap.of(
                "pk-vector.index.columns", "vec",
                "fields.vec.pk-vector.distance.metric", "l2",
                "file.format", "parquet"), Optional.empty(), null, false,
                ImmutableList.of(orcEra), null);
        Alias dis = new Alias(new L2DistanceApproximate(vecSlot(scan), queryVector()), "dis");
        assertNotPushedDown(topN(
                new LogicalProject<>(ImmutableList.of(idSlot(scan), dis), scan), dis, true));
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
        return paimonScan(options, Optional.empty());
    }

    private LogicalFileScan paimonScan(Map<String, String> options,
            Optional<TableScanParams> scanParams) {
        return paimonScan(options, scanParams, null);
    }

    /** {@code paimonFieldNames} overrides the schema-derived names the collision
     *  pre-flight reads off the mocked paimon rowType. */
    private LogicalFileScan paimonScan(Map<String, String> options,
            Optional<TableScanParams> scanParams, List<String> paimonFieldNames) {
        return paimonScan(options, scanParams, paimonFieldNames, false);
    }

    /** {@code nullableVector} makes the fixture's vec column nullable (the stored-NULL
     *  contract tests). */
    private LogicalFileScan paimonScan(Map<String, String> options,
            Optional<TableScanParams> scanParams, List<String> paimonFieldNames,
            boolean nullableVector) {
        return paimonScan(options, scanParams, paimonFieldNames, nullableVector, null, null);
    }

    /**
     * Full fixture. {@code extraHistory} appends rust-incompatible historical schemas
     * to the bound table's schema history (evolution tests); {@code boundTable} binds
     * a branch-style relation snapshot whose getPaimonTable resolves to that table —
     * the main-table stub stays for the anything-else lookups.
     */
    private LogicalFileScan paimonScan(Map<String, String> options,
            Optional<TableScanParams> scanParams, List<String> paimonFieldNames,
            boolean nullableVector, List<TableSchema> extraHistory,
            org.apache.paimon.table.Table boundTable) {
        List<Column> schema = ImmutableList.of(
                new Column("id", Type.INT, true),
                new Column("vec", new ArrayType(Type.FLOAT), nullableVector),
                new Column("vec2", new ArrayType(Type.FLOAT), nullableVector),
                new Column("tag", Type.INT, true),
                new Column("score", Type.DOUBLE, true),
                new Column("payload", Type.VARIANT, true),
                // A complex column the pruned-projection tests access with
                // ElementAt (the former struct_element): only the isComplexType()
                // verdict matters to the pre-flight, so a Doris MAP stands in for
                // a STRUCT this fixture cannot express through catalog types.
                new Column("pmap", new org.apache.doris.catalog.MapType(), true),
                // A nanosecond TIMESTAMP column: the scan node's nanosecond-reader
                // gate (JNI-only family) must be pre-flighted by the rewrite too.
                new Column("tsns", ScalarType.createType(PrimitiveType.TIMESTAMP_NS), true));
        if (paimonFieldNames == null) {
            paimonFieldNames = schema.stream().map(Column::getName).collect(Collectors.toList());
        }

        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("paimon_tbl");
        Mockito.when(table.getBaseSchema()).thenReturn(schema);
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getFullSchema(Mockito.<Optional<org.apache.doris.datasource.mvcc.MvccSnapshot>>any()))
                .thenReturn(schema);
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.NOT_PRUNED);

        FileStoreTable paimonTable = mockFileStoreTable(options, paimonFieldNames, extraHistory);
        Mockito.when(table.getPaimonTable(Mockito.<Optional<org.apache.doris.datasource.mvcc.MvccSnapshot>>any()))
                .thenReturn(paimonTable);
        Mockito.when(table.getPaimonTable(Mockito.<TableScanParams>any())).thenReturn(paimonTable);
        // An options-type TableScanParams binds its schema through
        // getFullSchema(TableScanParams) (the relation-option path inside
        // PaimonExternalTable, which would otherwise hit the unmocked package
        // private helpers). Stub it directly so the options tests get real
        // output slots; the rule itself only reads the params' raw map.
        Mockito.when(table.getFullSchema(Mockito.<TableScanParams>any())).thenReturn(schema);

        Optional<org.apache.doris.datasource.mvcc.MvccSnapshot> relationSnapshot = Optional.empty();
        if (boundTable != null) {
            // Stub AFTER the any() stub: Mockito resolves the last matching stub,
            // so this relation's snapshot resolves to the branch/bound table while
            // everything else keeps the main table.
            org.apache.doris.datasource.mvcc.MvccSnapshot snapshot =
                    Mockito.mock(org.apache.doris.datasource.mvcc.MvccSnapshot.class);
            Mockito.when(table.getPaimonTable(Mockito.eq(Optional.of(snapshot))))
                    .thenReturn(boundTable);
            relationSnapshot = Optional.of(snapshot);
        }

        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        // A filesystem metastore (empty properties, benign type): no vended-credential
        // provider, so the factory returns the base (empty) storage map.
        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Mockito.when(metastoreProperties.getType()).thenReturn(MetastoreProperties.Type.FILE_SYSTEM);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(metastoreProperties);
        Mockito.when(catalogProperty.getStoragePropertiesMap())
                .thenReturn(java.util.Collections.emptyMap());

        return new LogicalFileScan(StatementScopeIdGenerator.newRelationId(), table,
                ImmutableList.of("paimon_ctl", "paimon_db"), ImmutableList.of(),
                Optional.empty(), Optional.empty(), scanParams, Optional.empty(),
                relationSnapshot);
    }

    /** A FileStoreTable mock: local file:// location, no REST FileIO, and the
     *  per-test options map as the only input the pre-flight gates see.
     *  {@code extraHistory} appends historical schemas to the schema-manager's
     *  listAll, so evolution tests can supply rust-incompatible versions;
     *  null means an empty history (only the current schema). */
    private FileStoreTable mockFileStoreTable(Map<String, String> options,
            List<String> paimonFieldNames, List<TableSchema> extraHistory) {
        FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(paimonTable.options()).thenReturn(options);
        Mockito.when(paimonTable.location())
                .thenReturn(new Path("file:///warehouse/wh/paimon_db.db/paimon_tbl"));
        // The schema-history pre-flight: a current schema whose id is alone in the
        // history means no evolution, so the positive cases pass it.
        TableSchema currentSchema = new TableSchema(
                5, Collections.singletonList(new DataField(0, "id", new IntType())),
                0, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null);
        Mockito.when(paimonTable.schema()).thenReturn(currentSchema);
        List<TableSchema> history = new java.util.ArrayList<>();
        history.add(currentSchema);
        if (extraHistory != null) {
            history.addAll(extraHistory);
        }
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        Mockito.when(schemaManager.listAll()).thenReturn(history);
        Mockito.when(paimonTable.schemaManager()).thenReturn(schemaManager);
        // rowType drives the synthetic-distance-column collision pre-flight; its
        // field names track the fixture schema (or the test's explicit names).
        org.apache.paimon.types.RowType rowType = Mockito.mock(
                org.apache.paimon.types.RowType.class);
        Mockito.when(rowType.getFieldNames()).thenReturn(paimonFieldNames);
        Mockito.when(paimonTable.rowType()).thenReturn(rowType);
        Mockito.when(paimonTable.fileIO()).thenReturn(null);
        Mockito.when(paimonTable.coreOptions()).thenReturn(null);
        return paimonTable;
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
