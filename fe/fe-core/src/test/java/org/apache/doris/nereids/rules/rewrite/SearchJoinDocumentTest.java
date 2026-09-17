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

import org.apache.doris.analysis.SearchDslParser.QsFieldBinding;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class SearchJoinDocumentTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("search_join_document");
        connectContext.setDatabase("search_join_document");
        // Mocked tables have no rowsets. Preserve scans so these tests exercise
        // field binding, virtual columns and translation rather than empty plans.
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("CREATE TABLE objects (id BIGINT, v VARIANT, "
                + "INDEX idx_v(v) USING INVERTED PROPERTIES('parser'='english')) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE lists (object_id BIGINT, list_id BIGINT) "
                + "DUPLICATE KEY(object_id) DISTRIBUTED BY HASH(object_id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE associations (to_id BIGINT, from_id BIGINT) "
                + "DUPLICATE KEY(to_id) DISTRIBUTED BY HASH(to_id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE notes (id BIGINT, title TEXT, "
                + "INDEX idx_title(title) USING INVERTED PROPERTIES('parser'='english')) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE docs (id BIGINT, title TEXT, "
                + "INDEX idx_title_en(title) USING INVERTED PROPERTIES('parser'='english'), "
                + "INDEX idx_title_uni(title) USING INVERTED PROPERTIES('parser'='unicode')) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testDocumentSearchAboveTwoLeftJoins() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM (SELECT id, v FROM objects) o "
                + "LEFT JOIN lists l ON o.id=l.object_id "
                + "LEFT JOIN associations a ON o.id=a.to_id "
                + "WHERE search('john', '{\"default_field\":\"v.string_17\",\"mode\":\"lucene\"}')");
    }

    @Test
    public void testDocumentMatchExistsOr() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "WITH contacts AS (SELECT id, CAST(v['string_8'] AS VARCHAR) firstname FROM objects), "
                + "members AS (SELECT object_id FROM lists) "
                + "SELECT id, firstname FROM contacts c WHERE "
                + "(firstname MATCH_ANY 'keyur patel' AND EXISTS "
                + "(SELECT 1 FROM members l WHERE l.object_id=c.id)) OR c.id>1 LIMIT 10");
    }

    @Test
    public void testSearchOrAssociation() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o LEFT JOIN lists l ON o.id=l.object_id "
                + "WHERE search('v.string_8:john') OR l.list_id=12", planner -> {
                    Assertions.assertTrue(planner.getPhysicalPlan().anyMatch(plan ->
                            plan instanceof PhysicalOlapScan && ((PhysicalOlapScan) plan).getVirtualColumns()
                                    .stream().anyMatch(column -> column.anyMatch(e -> e instanceof SearchExpression))),
                            planner.getPhysicalPlan().treeString());
                });
    }

    @Test
    public void testMatchResidualUsesVirtualColumn() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o FULL OUTER JOIN lists l ON o.id=l.object_id "
                + "WHERE (CAST(o.v['string_8'] AS VARCHAR) MATCH_ANY 'john' "
                + "AND CAST(o.v['string_17'] AS VARCHAR) MATCH_ALL 'smith') OR l.list_id=12", planner -> {
                    Assertions.assertTrue(planner.getPhysicalPlan().anyMatch(plan ->
                            plan instanceof PhysicalOlapScan && ((PhysicalOlapScan) plan).getVirtualColumns()
                                    .stream().anyMatch(column -> column.anyMatch(e -> e instanceof Match))),
                            planner.getPhysicalPlan().treeString());
                });
    }

    @Test
    public void testAmbiguousSearchFieldRejected() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                "SELECT a.id FROM objects a JOIN objects b ON a.id=b.id WHERE search('v.string_8:john')"));
        Assertions.assertTrue(exception.getMessage().contains("Ambiguous field 'v.string_8'"),
                exception.getMessage());
    }

    @Test
    public void testSelfJoinFieldQualifiedByTableAlias() {
        // alias.field resolves like a SQL column reference, so each side of a self join can be searched.
        for (String side : new String[] {"a", "b"}) {
            String otherSide = side.equals("a") ? "b" : "a";
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT a.id FROM notes a JOIN notes b ON a.id=b.id "
                    + "WHERE search('" + side + ".title:john') OR " + otherSide + ".id=1", planner -> {
                        List<Plan> scans = scansWithSearchVirtualColumn(planner.getPhysicalPlan());
                        Assertions.assertEquals(1, scans.size(), planner.getPhysicalPlan().treeString());
                        SearchExpression search = searchExpressions(planner.getPhysicalPlan()).get(0);
                        Assertions.assertTrue(((PhysicalOlapScan) scans.get(0)).getOutputSet()
                                .containsAll(search.getInputSlots()));
                        Assertions.assertEquals(ImmutableList.of(side),
                                search.getInputSlots().stream().map(slot -> slot.getQualifier()
                                        .get(slot.getQualifier().size() - 1)).collect(Collectors.toList()));
                    });
        }
        // alias.variant.subcolumn
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id FROM objects a JOIN objects b ON a.id=b.id WHERE search('b.v.string_8:john')",
                planner -> Assertions.assertEquals("v.string_8", searchExpressions(planner.getPhysicalPlan())
                        .get(0).getQsPlan().getFieldBindings().get(0).getFieldName()));
    }

    @Test
    public void testFieldAliasBindsPhysicalColumn() {
        // The DSL names the visible column; index validation and the field sent to BE use the physical column.
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT n.id FROM (SELECT id, title AS headline FROM notes) n "
                + "LEFT JOIN lists l ON n.id=l.object_id WHERE search('headline:john') OR l.list_id=12",
                planner -> assertSingleBinding(planner.getPhysicalPlan(), "title"));
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM (SELECT id, v AS props FROM objects) o "
                + "JOIN lists l ON o.id=l.object_id WHERE search('props.string_8:john')",
                planner -> assertSingleBinding(planner.getPhysicalPlan(), "v.string_8"));
        // An alias that hides a column without an inverted index must not pass as the indexed column.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                "SELECT id FROM (SELECT o.id, l.list_id AS title FROM notes o JOIN lists l ON o.id=l.object_id) t "
                + "WHERE search('title:john')"));
        Assertions.assertTrue(exception.getMessage().contains("'list_id' has no inverted index"),
                exception.getMessage());
    }

    @Test
    public void testNestedPathBindsPhysicalColumn() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM (SELECT id, v AS props FROM objects) o JOIN lists l ON o.id=l.object_id "
                + "WHERE search('NESTED(props.items, name:john)')", planner -> {
                    SearchExpression search = searchExpressions(planner.getPhysicalPlan()).get(0);
                    Assertions.assertEquals("v.items", search.getQsPlan().getRoot().getNestedPath());
                    Assertions.assertEquals("v.items.name",
                            search.getQsPlan().getFieldBindings().get(0).getFieldName());
                });
    }

    @Test
    public void testSearchIsNotInferredForEqualColumn() {
        // a.title = b.title lets predicates on a.title be inferred for b.title, but a SEARCH is bound to the
        // inverted index of a.title and must stay there.
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT a.id FROM notes a JOIN docs b ON a.title=b.title "
                + "WHERE search('a.title:john') OR a.title='x'", planner -> {
                    List<SearchExpression> searches = searchExpressions(planner.getPhysicalPlan());
                    Assertions.assertEquals(1, searches.size(), planner.getPhysicalPlan().treeString());
                    Slot field = searches.get(0).getInputSlots().iterator().next();
                    Assertions.assertEquals("a", field.getQualifier().get(field.getQualifier().size() - 1));
                });
    }

    @Test
    public void testSearchIsNotInferredThroughSetOperation() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "(SELECT title FROM notes WHERE search('title:john')) INTERSECT (SELECT title FROM docs)",
                planner -> Assertions.assertEquals(1, searchExpressions(planner.getPhysicalPlan()).size(),
                        planner.getPhysicalPlan().treeString()));
    }

    @Test
    public void testAnalyzerSelectorUsesIndexIdentity() {
        // The selector is matched like the index lookup: case-insensitively, and the default analyzer of a
        // field is the index it resolves to.
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT id FROM docs WHERE search('title@English:john AND title@english:smith')");
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT id FROM notes WHERE search('title:john AND title@english:smith')");
        AnalysisException conflict = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                "SELECT id FROM docs WHERE search('title@english:john AND title@unicode:smith')"));
        Assertions.assertTrue(conflict.getMessage().contains("one analyzer per field"), conflict.getMessage());
        AnalysisException missing = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                "SELECT id FROM docs WHERE search('title@chinese:john')"));
        Assertions.assertTrue(missing.getMessage().contains("No inverted index found for SEARCH analyzer"),
                missing.getMessage());
    }

    @Test
    public void testNestedIndexSearchInProjectionUsesVirtualColumn() {
        // Direct scans and joins share one materialization path, so a wrapped MATCH is handled by both.
        for (String from : new String[] {"notes n", "notes n LEFT JOIN lists l ON n.id=l.object_id"}) {
            PlanChecker.from(connectContext).checkPlannerResult(
                    "SELECT n.id, CASE WHEN n.title MATCH_ANY 'john' THEN 1 ELSE 0 END FROM " + from,
                    planner -> {
                        Assertions.assertTrue(hasMatchVirtualColumn(planner.getPhysicalPlan()),
                                planner.getPhysicalPlan().treeString());
                        assertNoIndexSearchOutsideScan(planner.getPhysicalPlan());
                    });
        }
    }

    @Test
    public void testVirtualColumnReusedBySelectAndWhere() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT n.id, n.title MATCH_ANY 'john' FROM notes n LEFT JOIN lists l ON n.id=l.object_id "
                + "WHERE n.title MATCH_ANY 'john' OR l.list_id=12", planner -> {
                    Plan plan = planner.getPhysicalPlan();
                    List<Plan> scans = new ArrayList<>();
                    collectPlans(plan, scans);
                    scans.removeIf(node -> !(node instanceof PhysicalOlapScan)
                            || ((PhysicalOlapScan) node).getVirtualColumns().isEmpty());
                    Assertions.assertEquals(1, scans.size(), plan.treeString());
                    Assertions.assertEquals(1, ((PhysicalOlapScan) scans.get(0)).getVirtualColumns().size(),
                            plan.treeString());
                    assertNoIndexSearchOutsideScan(plan);
                    List<Plan> nodes = new ArrayList<>();
                    collectPlans(plan, nodes);
                    for (Plan node : nodes) {
                        List<Slot> output = node.getOutput();
                        Assertions.assertEquals(new HashSet<>(output).size(), output.size(),
                                "duplicate output slot in " + node + "\n" + plan.treeString());
                    }
                });
    }

    @Test
    public void testIndexSearchNotReachingScanIsRejected() {
        // The filter cannot move below the TopN and materialization does not cross it.
        String from = "FROM (SELECT id, title FROM notes ORDER BY id LIMIT 10) t WHERE ";
        assertRejected("SELECT id " + from + "search('title:john')", "SEARCH must be evaluated by an OLAP scan");
        assertRejected("SELECT id " + from + "title MATCH_ANY 'john'", "only support in olapScan filter");
        // Fields of two tables can never be evaluated by one scan.
        assertRejected("SELECT n.id FROM notes n JOIN objects o ON n.id=o.id "
                + "WHERE search('title:john OR v.string_8:john')", "SEARCH must be evaluated by an OLAP scan");
    }

    @Test
    public void testPushDownIsIdempotent() {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(
                "SELECT n.id, CASE WHEN n.title MATCH_ANY 'john' THEN 1 ELSE 0 END FROM notes n "
                + "LEFT JOIN lists l ON n.id=l.object_id WHERE search('title:smith') OR l.list_id=12").rewrite();
        String rewritten = checker.getPlan().treeString();
        Assertions.assertTrue(checker.getPlan().anyMatch(node -> node instanceof LogicalOlapScan
                && ((LogicalOlapScan) node).getVirtualColumns().size() == 2), rewritten);
        Assertions.assertEquals(rewritten,
                checker.applyTopDown(new PushDownIndexSearchAsVirtualColumn()).getPlan().treeString());
    }

    @Test
    public void testSearchWithFieldSelector() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o LEFT JOIN lists l ON o.id=l.object_id "
                + "WHERE search('v.string_8@english:john')");
    }

    @Test
    public void testSearchConstantPredicatePreservesField() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o LEFT JOIN lists l ON o.id=l.object_id "
                + "WHERE CAST(o.v['string_8'] AS VARCHAR)='john' "
                + "AND (search('v.string_8:john') OR l.list_id=12)");
    }

    @Test
    public void testQuotedAtSignIsLiteralVariantPath() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT id FROM objects WHERE search('\"v.email@work\":john')");
    }

    @Test
    public void testSearchOrMovedIntoInnerJoin() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o JOIN lists l ON o.id=l.object_id "
                + "WHERE search('v.string_8:john') OR l.list_id=12", planner -> {
                    Assertions.assertTrue(planner.getPhysicalPlan().anyMatch(plan ->
                            plan instanceof PhysicalOlapScan && ((PhysicalOlapScan) plan).getVirtualColumns()
                                    .stream().anyMatch(column -> column.anyMatch(e -> e instanceof SearchExpression))),
                            planner.getPhysicalPlan().treeString());
                });
    }

    @Test
    public void testNullPropagatingMatchOnNullExtendedSideUsesVirtualColumn() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT l.object_id FROM lists l LEFT JOIN objects o ON l.object_id=o.id "
                + "WHERE CAST(o.v['string_8'] AS VARCHAR) MATCH_ANY 'john' OR l.list_id=12", planner -> {
                    Assertions.assertTrue(hasMatchVirtualColumn(planner.getPhysicalPlan()),
                            planner.getPhysicalPlan().treeString());
                });
    }

    @Test
    public void testNonNullPropagatingMatchStaysAboveOuterJoin() {
        // nvl(NULL, 'john') matches, so the MATCH cannot be computed below the NULL-extended side.
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT l.object_id FROM lists l LEFT JOIN objects o ON l.object_id=o.id "
                + "WHERE nvl(CAST(o.v['string_8'] AS VARCHAR), 'john') MATCH_ANY 'john' OR l.list_id=12",
                planner -> {
                    Assertions.assertFalse(hasMatchVirtualColumn(planner.getPhysicalPlan()),
                            planner.getPhysicalPlan().treeString());
                });
    }

    @Test
    public void testSearchOnNullPaddedSideRejected() {
        // LEFT JOIN ... ON false becomes Project(l.*, NULL AS o.*).
        assertNullSideSearchRejected("SELECT l.object_id FROM lists l LEFT JOIN notes n ON false "
                + "WHERE NOT search('title:john') OR l.list_id=8");
        // LEFT JOIN ... WHERE n.id IS NULL becomes a left anti join with NULL aliases for n.*.
        assertNullSideSearchRejected("SELECT l.object_id FROM lists l LEFT JOIN notes n ON l.object_id=n.id "
                + "WHERE n.id IS NULL AND (NOT search('title:john') OR l.list_id=8)");
        // A derived table exposing both sides lets materialize() inline NULL into a scan virtual column.
        assertNullSideSearchRejected("SELECT s.id FROM (SELECT o.id, o.v, n.title FROM objects o "
                + "LEFT JOIN notes n ON false) s JOIN lists l ON s.id=l.object_id "
                + "WHERE NOT search('title:john OR v.string_8:hello') OR l.list_id=1");
    }

    private void collectPlans(Plan plan, List<Plan> plans) {
        plans.add(plan);
        plan.children().forEach(child -> collectPlans(child, plans));
    }

    private List<Expression> scanEvaluatedExpressions(Plan node) {
        return node instanceof PhysicalOlapScan
                ? new ArrayList<>(((PhysicalOlapScan) node).getVirtualColumns()) : new ArrayList<>();
    }

    private List<Plan> scansWithSearchVirtualColumn(Plan plan) {
        List<Plan> plans = new ArrayList<>();
        collectPlans(plan, plans);
        return plans.stream().filter(node -> scanEvaluatedExpressions(node).stream()
                .anyMatch(column -> column.anyMatch(e -> e instanceof SearchExpression)))
                .collect(Collectors.toList());
    }

    private List<SearchExpression> searchExpressions(Plan plan) {
        List<Plan> plans = new ArrayList<>();
        collectPlans(plan, plans);
        List<SearchExpression> result = new ArrayList<>();
        for (Plan node : plans) {
            List<Expression> expressions = scanEvaluatedExpressions(node);
            expressions.addAll(node.getExpressions());
            expressions.forEach(e -> result.addAll(e.<SearchExpression>collect(SearchExpression.class::isInstance)));
        }
        return result;
    }

    private void assertSingleBinding(Plan plan, String physicalField) {
        List<SearchExpression> searches = searchExpressions(plan);
        Assertions.assertEquals(1, searches.size(), plan.treeString());
        List<QsFieldBinding> bindings = searches.get(0).getQsPlan().getFieldBindings();
        Assertions.assertEquals(1, bindings.size());
        Assertions.assertEquals(physicalField, bindings.get(0).getFieldName());
        Assertions.assertEquals(physicalField, searches.get(0).getQsPlan().getRoot().getField());
    }

    // Every MATCH/SEARCH of these plans can be materialized, so none may be left for row evaluation.
    private void assertNoIndexSearchOutsideScan(Plan plan) {
        List<Plan> plans = new ArrayList<>();
        collectPlans(plan, plans);
        for (Plan node : plans) {
            Assertions.assertFalse(node.getExpressions().stream().anyMatch(expression -> expression.anyMatch(
                    e -> e instanceof Match || e instanceof SearchExpression)), plan.treeString());
        }
    }

    private void assertRejected(String sql, String message) {
        Throwable thrown = Assertions.assertThrows(Throwable.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql));
        Throwable cause = thrown;
        while (cause != null && (cause.getMessage() == null || !cause.getMessage().contains(message))) {
            cause = cause.getCause();
        }
        Assertions.assertNotNull(cause, sql + " failed with: " + thrown);
    }

    private boolean hasMatchVirtualColumn(Plan plan) {
        return plan.anyMatch(node -> node instanceof PhysicalOlapScan && ((PhysicalOlapScan) node).getVirtualColumns()
                .stream().anyMatch(column -> column.anyMatch(e -> e instanceof Match)));
    }

    private void assertNullSideSearchRejected(String sql) {
        assertRejected(sql, "null-generating side of an outer join");
    }
}
