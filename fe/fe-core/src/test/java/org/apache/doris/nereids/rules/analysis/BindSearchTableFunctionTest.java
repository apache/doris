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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
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
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.table.FullTextSearch;
import org.apache.doris.nereids.trees.expressions.functions.table.Numbers;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The analyzer's side of a search table function: it wraps the relation in the TopN that merges the candidates of
 * the scan's ranges in the function's order, a PREPARE defers the parameterized arguments to the connector, and each
 * EXECUTE binds its own literal values. A mocked connector stands for any connector that serves the search SPI.
 */
public class BindSearchTableFunctionTest {
    private ConnectContext previousContext;
    private MockedStatic<Env> envStatic;
    private ConnectContext context;
    private ConnectorMetadata metadata;
    private ConnectorTableHandle scanHandle;
    private ConnectorSearch search;

    @BeforeEach
    public void setUp() throws Exception {
        previousContext = ConnectContext.get();
        context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("searcher", "%"));

        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getFunctionRegistry()).thenReturn(new FunctionRegistry());
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(env.getSqlCacheManager()).thenReturn(Mockito.mock(NereidsSqlCacheManager.class));
        envStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.eq("ctl"),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);
        context.setEnv(env);
        context.setThreadLocalInfo();

        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        metadata = Mockito.mock(ConnectorMetadata.class,
                Mockito.withSettings().extraInterfaces(ConnectorSearchOps.class));
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("ctl");
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.doReturn(database).when(catalog).getDbOrAnalysisException("db");
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.doReturn(database).when(table).getDatabase();
        Mockito.doReturn(table).when(database).getTableOrAnalysisException("t");
        ConnectorTableHandle tableHandle = Mockito.mock(ConnectorTableHandle.class);
        scanHandle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(table.resolveConnectorTableHandle(session, metadata)).thenReturn(Optional.of(tableHandle));
        search = new ConnectorSearch(scanHandle, Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), null, true, null),
                new ConnectorColumn("_distance", ConnectorType.of("FLOAT"), null, true, null)),
                5, 2, ConnectorSearchLazyMaterialization.NONE);
        Mockito.when(searchOps().prepareSearch(Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(
                invocation -> search);
    }

    @AfterEach
    public void tearDown() {
        envStatic.close();
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testVectorSearchMergesTheRangesByAscendingDistance() {
        Plan plan = bind(new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(), "vector_search",
                vectorSearchArguments(), ImmutableMap.of()), newStatement());

        LogicalTVFRelation relation = assertMergedBy(plan, "_distance", true, 5, 2);
        Assertions.assertInstanceOf(VectorSearch.class, relation.getFunction());
    }

    @Test
    public void testFullTextSearchMergesTheRangesByDescendingScore() {
        search = new ConnectorSearch(scanHandle, Arrays.asList(
                new ConnectorColumn("body", ConnectorType.of("STRING"), null, true, null),
                new ConnectorColumn("_score", ConnectorType.of("FLOAT"), null, true, null)),
                10, 0, ConnectorSearchLazyMaterialization.NONE);

        Plan plan = bind(new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(), "full_text_search",
                new Properties(ImmutableMap.of("table", "ctl.db.t", "column", "body", "query", "lance")),
                ImmutableMap.of()), newStatement());

        LogicalTVFRelation relation = assertMergedBy(plan, "_score", false, 10, 0);
        Assertions.assertInstanceOf(FullTextSearch.class, relation.getFunction());
    }

    @Test
    public void testOtherTableFunctionsGetNoMergeTopN() {
        Plan plan = bind(new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(), "numbers",
                new Properties(ImmutableMap.of("number", "3")), ImmutableMap.of()), newStatement());

        Assertions.assertInstanceOf(LogicalTVFRelation.class, plan);
        Assertions.assertInstanceOf(Numbers.class, ((LogicalTVFRelation) plan).getFunction());
        Mockito.verifyNoInteractions(searchOps());
    }

    @Test
    public void testPrepareHandsTheParameterizedArgumentsToTheConnectorAsDeferred() {
        StatementContext statement = newStatement();
        statement.setPrepareStage(true);
        Placeholder vector = new Placeholder(statement.getNextPlaceholderId());
        Placeholder topK = new Placeholder(statement.getNextPlaceholderId());

        Plan plan = bind(new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(), "vector_search",
                new Properties(ImmutableMap.of("table", "ctl.db.t", "column", "embedding")),
                ImmutableMap.of("query_vector", vector, "top_k", topK)), statement);

        // Called once: the search is built for the connector only after the deferred names are known.
        ConnectorSearchRequest request = capturedRequests(1).get(0);
        Assertions.assertEquals(ImmutableMap.of("column", "embedding"), request.getArguments());
        Assertions.assertEquals(ImmutableSet.of("query_vector", "top_k"), request.getDeferredArguments());
        assertMergedBy(plan, "_distance", true, 5, 2);
    }

    @Test
    public void testEachExecuteBindsItsOwnValuesIntoACopyOfTheArguments() {
        StatementContext statement = newStatement();
        Placeholder vector = new Placeholder(statement.getNextPlaceholderId());
        Placeholder topK = new Placeholder(statement.getNextPlaceholderId());
        UnboundTVFRelation unbound = new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(),
                "vector_search", new Properties(ImmutableMap.of("table", "ctl.db.t", "column", "embedding")),
                ImmutableMap.of("query_vector", vector, "top_k", topK));

        statement.getIdToPlaceholderRealExpr().put(vector.getPlaceholderId(), new StringLiteral("[3, 4]"));
        statement.getIdToPlaceholderRealExpr().put(topK.getPlaceholderId(), new IntegerLiteral(7));
        bind(unbound, statement);
        statement.getIdToPlaceholderRealExpr().put(vector.getPlaceholderId(), new StringLiteral("[5, 6]"));
        statement.getIdToPlaceholderRealExpr().put(topK.getPlaceholderId(), new IntegerLiteral(8));
        bind(unbound, statement);

        List<ConnectorSearchRequest> requests = capturedRequests(2);
        Assertions.assertEquals(ImmutableMap.of("column", "embedding", "query_vector", "[3, 4]", "top_k", "7"),
                requests.get(0).getArguments());
        Assertions.assertEquals(ImmutableMap.of("column", "embedding", "query_vector", "[5, 6]", "top_k", "8"),
                requests.get(1).getArguments());
        Assertions.assertTrue(requests.get(1).getDeferredArguments().isEmpty());
        // The plan kept for the next EXECUTE still has only its own arguments.
        Assertions.assertEquals(ImmutableMap.of("table", "ctl.db.t", "column", "embedding"),
                unbound.getProperties().getMap());
    }

    @Test
    public void testExecuteRefusesANullOrMissingParameterValue() {
        StatementContext statement = newStatement();
        Placeholder topK = new Placeholder(statement.getNextPlaceholderId());
        UnboundTVFRelation unbound = new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(),
                "vector_search", vectorSearchArguments(), ImmutableMap.of("top_k", topK));

        statement.getIdToPlaceholderRealExpr().put(topK.getPlaceholderId(), new NullLiteral());
        AnalysisException nullValue = Assertions.assertThrows(AnalysisException.class,
                () -> bind(unbound, statement));
        Assertions.assertTrue(nullValue.getMessage().contains(
                "vector_search parameter 'top_k' must be a non-null literal"), nullValue.getMessage());

        statement.getIdToPlaceholderRealExpr().clear();
        AnalysisException missingValue = Assertions.assertThrows(AnalysisException.class,
                () -> bind(unbound, statement));
        Assertions.assertTrue(missingValue.getMessage().contains(
                "vector_search parameter 'top_k' must be a non-null literal"), missingValue.getMessage());
        Mockito.verifyNoInteractions(searchOps());
    }

    private static Properties vectorSearchArguments() {
        return new Properties(ImmutableMap.of("table", "ctl.db.t", "column", "embedding", "query_vector", "[1, 2]"));
    }

    private StatementContext newStatement() {
        StatementContext statement = new StatementContext(context, new OriginStatement("", 0));
        context.setStatementContext(statement);
        return statement;
    }

    /** Runs only the analyzer rule that binds a table function, as the analysis of a statement would. */
    private static Plan bind(UnboundTVFRelation unbound, StatementContext statement) {
        CascadesContext cascadesContext = CascadesContext.initContext(statement, unbound, PhysicalProperties.ANY);
        Rule rule = new BindExpression().buildRules().stream()
                .filter(candidate -> candidate.getRuleType() == RuleType.BINDING_UNBOUND_TVF_RELATION_FUNCTION)
                .findFirst()
                .orElseThrow(IllegalStateException::new);
        List<Plan> plans = rule.transform(unbound, cascadesContext);
        Assertions.assertEquals(1, plans.size());
        return plans.get(0);
    }

    private static LogicalTVFRelation assertMergedBy(Plan plan, String resultColumn, boolean ascending, long limit,
            long offset) {
        Assertions.assertInstanceOf(LogicalTopN.class, plan);
        LogicalTopN<?> topN = (LogicalTopN<?>) plan;
        Assertions.assertEquals(limit, topN.getLimit());
        Assertions.assertEquals(offset, topN.getOffset());
        Assertions.assertEquals(1, topN.getOrderKeys().size());
        OrderKey key = topN.getOrderKeys().get(0);
        Assertions.assertEquals(ascending, key.isAsc());
        Assertions.assertFalse(key.isNullFirst());
        Assertions.assertInstanceOf(SlotReference.class, key.getExpr());
        Assertions.assertEquals(resultColumn, ((SlotReference) key.getExpr()).getName());
        Assertions.assertInstanceOf(LogicalTVFRelation.class, topN.child());
        LogicalTVFRelation relation = (LogicalTVFRelation) topN.child();
        Assertions.assertTrue(relation.getOutput().contains(key.getExpr()));
        return relation;
    }

    private List<ConnectorSearchRequest> capturedRequests(int times) {
        ArgumentCaptor<ConnectorSearchRequest> request = ArgumentCaptor.forClass(ConnectorSearchRequest.class);
        Mockito.verify(searchOps(), Mockito.times(times)).prepareSearch(Mockito.any(), Mockito.any(),
                request.capture());
        return request.getAllValues();
    }

    private ConnectorSearchOps searchOps() {
        return (ConnectorSearchOps) metadata;
    }
}
