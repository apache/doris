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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.search.ConnectorSearch;
import org.apache.doris.connector.spi.search.ConnectorSearchLazyMaterialization;
import org.apache.doris.connector.spi.search.ConnectorSearchOps;
import org.apache.doris.connector.spi.search.ConnectorSearchRequest;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Where the filters of a statement end up around the TopN that merges a search table function's candidates, after
 * the analyzer and the whole rewriter: only the WHERE written on the function filters the candidates before the
 * merge. An outer WHERE over the user's own ORDER BY ... LIMIT, the filters of a CTE's consumers, the WHERE of a
 * query that joins the search and the predicates the optimizer infers all apply to the search's result. A mocked
 * connector stands for any connector that serves the search SPI.
 */
public class SearchWherePlacementTest extends TestWithFeService {
    private static final String CATALOG = "search_ctl";
    private Map<String, CatalogIf<?>> catalogsByName;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        // The joined table stays empty: keep its scan rather than an empty relation that prunes the join away.
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("CREATE TABLE dim (k BIGINT, name VARCHAR(16)) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        registerSearchCatalog();
    }

    @Override
    protected void runAfterAll() throws Exception {
        catalogsByName.remove(CATALOG);
    }

    @Test
    public void testTheWhereWrittenOnTheSearchFiltersItsCandidates() {
        for (String sql : Arrays.asList(
                "SELECT id FROM " + search("") + " WHERE k > 5",
                "SELECT v.id FROM " + search("") + " v WHERE v.k > 5",
                "SELECT id FROM " + search("") + " WHERE k > 5 ORDER BY _distance NULLS LAST LIMIT 3")) {
            Plan plan = rewrite(sql);
            Assertions.assertEquals(Collections.singleton("k"), searchColumns(plan, conjunctsBelowTheMerge(plan)),
                    plan.treeString());
            Assertions.assertTrue(conjunctsAboveTheMerge(plan).isEmpty(), plan.treeString());
        }
    }

    @Test
    public void testAnOuterWhereStaysAboveTheUsersOwnLimit() {
        // The user's TopN has the merge's order (ascending, nulls last), so MergeTopNs folds it into the merge.
        Plan plan = rewrite("SELECT * FROM (SELECT id, k, _distance FROM " + search("")
                + " ORDER BY _distance NULLS LAST LIMIT 3) s WHERE k > 5");
        List<LogicalTopN<?>> topNs = plan.collectToList(LogicalTopN.class::isInstance);
        Assertions.assertEquals(1, topNs.size(), plan.treeString());
        Assertions.assertEquals(3, topNs.get(0).getLimit(), plan.treeString());
        Assertions.assertTrue(conjunctsBelowTheMerge(plan).isEmpty(), plan.treeString());
        Assertions.assertEquals(Collections.singleton("k"), searchColumns(plan, conjunctsAboveTheMerge(plan)),
                plan.treeString());
    }

    @Test
    public void testTheFiltersOfACteUsedTwiceStayAboveTheMerge() {
        // Both consumers filter, so the rewriter puts the OR of their filters into the producer, over the search.
        Plan plan = rewrite("WITH c AS (SELECT id, k FROM " + search("") + ")"
                + " SELECT id FROM c WHERE k > 5 UNION ALL SELECT id FROM c WHERE k < 0");
        Assertions.assertTrue(conjunctsBelowTheMerge(plan).isEmpty(), plan.treeString());
        Assertions.assertTrue(searchColumns(plan, conjunctsAboveTheMerge(plan)).contains("k"), plan.treeString());
    }

    @Test
    public void testWhatAJoinAddsOrInfersStaysAboveTheMerge() {
        // InferJoinNotNull adds k IS NOT NULL, InferPredicates k > 5 from d.k > 5, and the WHERE is over the join:
        // below the merge, each would skip the 5 best rows among those it keeps rather than the 5 best rows.
        Plan plan = rewrite("SELECT v.id, d.name FROM " + search(", 'offset'='5'") + " v"
                + " JOIN dim d ON v.k = d.k WHERE d.k > 5 AND v.id > 1");
        LogicalTopN<?> merge = merge(plan);
        Assertions.assertEquals(5, merge.getOffset(), plan.treeString());
        Assertions.assertTrue(conjunctsBelowTheMerge(plan).isEmpty(), plan.treeString());
        Assertions.assertTrue(searchColumns(plan, conjunctsAboveTheMerge(plan)).containsAll(Arrays.asList("k", "id")),
                plan.treeString());
    }

    private static String search(String moreArguments) {
        return "vector_search('table'='" + CATALOG + ".db.t', 'column'='embedding', 'query_vector'='[1, 2]',"
                + " 'top_k'='10'" + moreArguments + ")";
    }

    private Plan rewrite(String sql) {
        return PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
    }

    /** The TopN that merges the search's candidates: the lowest TopN above the search relation. */
    private static LogicalTopN<?> merge(Plan plan) {
        List<LogicalTopN<?>> merges = plan.collectToList(node -> node instanceof LogicalTopN
                && ((LogicalTopN<?>) node).child().anyMatch(LogicalTVFRelation.class::isInstance)
                && !((LogicalTopN<?>) node).child().anyMatch(LogicalTopN.class::isInstance));
        Assertions.assertEquals(1, merges.size(), plan.treeString());
        return merges.get(0);
    }

    private static List<Expression> conjunctsBelowTheMerge(Plan plan) {
        return conjuncts(merge(plan).child().collectToList(LogicalFilter.class::isInstance));
    }

    private static List<Expression> conjunctsAboveTheMerge(Plan plan) {
        Map<Object, Boolean> below = new IdentityHashMap<>();
        merge(plan).child().collectToList(LogicalFilter.class::isInstance).forEach(filter -> below.put(filter, true));
        return conjuncts(plan.collectToList(node -> node instanceof LogicalFilter && !below.containsKey(node)));
    }

    private static List<Expression> conjuncts(List<Object> filters) {
        return filters.stream().flatMap(filter -> ((LogicalFilter<?>) filter).getConjuncts().stream())
                .collect(Collectors.toList());
    }

    /** The search's columns the conjuncts read, by name; a joined table's columns are not among them. */
    private static Set<String> searchColumns(Plan plan, List<Expression> conjuncts) {
        List<LogicalTVFRelation> searches = plan.collectToList(LogicalTVFRelation.class::isInstance);
        Assertions.assertEquals(1, searches.size(), plan.treeString());
        Set<ExprId> searchSlots = searches.get(0).getOutputExprIdSet();
        return conjuncts.stream().flatMap(conjunct -> conjunct.getInputSlots().stream())
                .filter(slot -> searchSlots.contains(slot.getExprId())).map(Slot::getName)
                .collect(Collectors.toSet());
    }

    private void registerSearchCatalog() throws Exception {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class,
                Mockito.withSettings().extraInterfaces(ConnectorSearchOps.class));
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(CATALOG);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.doReturn(database).when(catalog).getDbOrAnalysisException("db");
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.doReturn(database).when(table).getDatabase();
        Mockito.doReturn(table).when(database).getTableOrAnalysisException("t");
        Mockito.when(table.resolveConnectorTableHandle(session, metadata))
                .thenReturn(Optional.of(Mockito.mock(ConnectorTableHandle.class)));
        ConnectorTableHandle scanHandle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(((ConnectorSearchOps) metadata).prepareSearch(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    Map<String, String> arguments = ((ConnectorSearchRequest) invocation.getArgument(2))
                            .getArguments();
                    return new ConnectorSearch(scanHandle, Arrays.asList(
                            new ConnectorColumn("id", ConnectorType.of("BIGINT"), null, true, null),
                            new ConnectorColumn("k", ConnectorType.of("BIGINT"), null, true, null),
                            new ConnectorColumn("_distance", ConnectorType.of("FLOAT"), null, true, null)),
                            Long.parseLong(arguments.get("top_k")),
                            Long.parseLong(arguments.getOrDefault("offset", "0")),
                            ConnectorSearchLazyMaterialization.NONE);
                });
        catalogsByName = Deencapsulation.getField(Env.getCurrentEnv().getCatalogMgr(), "nameToCatalog");
        catalogsByName.put(CATALOG, catalog);
    }
}
