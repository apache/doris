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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.search.ConnectorSearch;
import org.apache.doris.connector.spi.search.ConnectorSearchFunction;
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
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The engine's side of a search table function: it reads only {@code table}, checks the user's SELECT privilege on
 * that table, routes the search to the connector of the table's catalog with every other argument unread, and
 * checks what the engine's rules need of the columns the connector returns. It then requires SELECT on every column
 * and refuses the search while a row policy or a data mask applies to the user, which it could not apply. No Lance
 * here: a mocked connector stands for any connector that serves the search SPI.
 */
public class ExternalSearchTableValuedFunctionTest {
    private ConnectContext previousContext;
    private MockedStatic<Env> envStatic;
    private AccessControllerManager accessManager;
    private CatalogMgr catalogMgr;
    private PluginDrivenExternalCatalog catalog;
    private PluginDrivenExternalTable table;
    private ConnectorMetadata metadata;
    private ConnectorTableHandle tableHandle;
    private ConnectorTableHandle scanHandle;
    private ConnectorSearch search;

    @BeforeEach
    public void setUp() throws Exception {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("searcher", "%"));
        context.setThreadLocalInfo();

        Env env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        envStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.eq("ctl"),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);

        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        Mockito.when(session.getUser()).thenReturn("searcher");
        metadata = Mockito.mock(ConnectorMetadata.class,
                Mockito.withSettings().extraInterfaces(ConnectorSearchOps.class));
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("ctl");
        // The lookup matches names case-insensitively, as under lower_case_table_names.
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.doReturn(database).when(catalog).getDbOrAnalysisException(
                Mockito.argThat(name -> "db".equalsIgnoreCase(name)));
        table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.doReturn(database).when(table).getDatabase();
        Mockito.doReturn(table).when(database).getTableOrAnalysisException(
                Mockito.argThat(name -> "t".equalsIgnoreCase(name)));
        tableHandle = Mockito.mock(ConnectorTableHandle.class);
        scanHandle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(table.resolveConnectorTableHandle(session, metadata)).thenReturn(Optional.of(tableHandle));
        search = new ConnectorSearch(scanHandle, Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), null, true, null),
                new ConnectorColumn("Embedding", ConnectorType.arrayOf(ConnectorType.of("FLOAT")), null, true, null),
                new ConnectorColumn("_distance", ConnectorType.of("FLOAT"), null, true, null)),
                5, 2, ConnectorSearchLazyMaterialization.TOP_LEVEL_COLUMNS);
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
    public void testRoutesTheSearchToTheConnectorOfTheTablesCatalog() throws Exception {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("Table", " ctl.db.t ");
        properties.put("COLUMN", "embedding");
        properties.put("query_vector", " [1, 2] ");

        VectorSearchTableValuedFunction function = new VectorSearchTableValuedFunction(properties,
                ImmutableSet.of("top_k"));

        ArgumentCaptor<ConnectorSearchRequest> request = ArgumentCaptor.forClass(ConnectorSearchRequest.class);
        Mockito.verify(searchOps()).prepareSearch(Mockito.any(), Mockito.same(tableHandle), request.capture());
        // Every argument but table, by its lower-case name, its value untouched; the deferred names as given.
        Assertions.assertEquals(ConnectorSearchFunction.VECTOR_SEARCH, request.getValue().getFunction());
        Assertions.assertEquals(ImmutableMap.of("column", "embedding", "query_vector", " [1, 2] "),
                request.getValue().getArguments());
        Assertions.assertEquals(Collections.singleton("top_k"), request.getValue().getDeferredArguments());
        Assertions.assertEquals(Arrays.asList("id", "Embedding", "_distance"),
                function.getTableColumns().stream().map(Column::getName).collect(Collectors.toList()));
        Assertions.assertEquals(PrimitiveType.FLOAT, function.getTableColumns().get(2).getType().getPrimitiveType());
        Assertions.assertEquals("_distance", function.getResultColumn());
        Assertions.assertEquals(5, function.getTopK());
        Assertions.assertEquals(2, function.getOffset());
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.TOP_LEVEL_COLUMNS,
                function.getLazyMaterialization());
        Assertions.assertEquals("VectorSearchTableValuedFunction<ctl.db.t>", function.getTableName());
    }

    @Test
    public void testFullTextSearchIsTheOtherFunctionOfTheSameSpi() throws Exception {
        search = new ConnectorSearch(scanHandle, Arrays.asList(
                new ConnectorColumn("body", ConnectorType.of("STRING"), null, true, null),
                new ConnectorColumn("_score", ConnectorType.of("FLOAT"), null, true, null)),
                10, 0, ConnectorSearchLazyMaterialization.NONE);

        FullTextSearchTableValuedFunction function = new FullTextSearchTableValuedFunction(
                ImmutableMap.of("table", "ctl.db.t", "column", "body", "query", "lance"), ImmutableSet.of());

        ArgumentCaptor<ConnectorSearchRequest> request = ArgumentCaptor.forClass(ConnectorSearchRequest.class);
        Mockito.verify(searchOps()).prepareSearch(Mockito.any(), Mockito.any(), request.capture());
        Assertions.assertEquals(ConnectorSearchFunction.FULL_TEXT_SEARCH, request.getValue().getFunction());
        Assertions.assertTrue(request.getValue().getDeferredArguments().isEmpty());
        Assertions.assertEquals("_score", function.getResultColumn());
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.NONE, function.getLazyMaterialization());
    }

    @Test
    public void testRejectsATableArgumentTheEngineCannotResolve() {
        assertRejected(ImmutableMap.of("column", "v"), ImmutableSet.of(),
                "Missing required vector_search() property 'table'");
        assertRejected(ImmutableMap.of("table", "db.t"), ImmutableSet.of(),
                "'table' must be a fully qualified catalog.database.table name");
        assertRejected(ImmutableMap.of("table", "ctl.db.t@tag(v1)"), ImmutableSet.of(),
                "'table' must be a fully qualified catalog.database.table name");
        // The parser refuses a parameter in place of 'table'.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new VectorSearchTableValuedFunction(ImmutableMap.of("column", "v"), ImmutableSet.of("table")));
        assertRejected(ImmutableMap.of("table", "ctl.db.t", "Column", "a", "column", "b"), ImmutableSet.of(),
                "Duplicate vector_search() property 'column'");
        Mockito.verifyNoInteractions(searchOps());
    }

    @Test
    public void testChecksTheSelectPrivilegeOnTheSearchedTable() {
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.eq("ctl"),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(PrivPredicate.SELECT))).thenReturn(false);

        AnalysisException denied = assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(),
                "SELECT command denied to user 'searcher'");
        Assertions.assertTrue(denied.getMessage().contains("db: t"), denied.getMessage());
        Mockito.verifyNoInteractions(searchOps());

        // Checked on the table found, not on the names as written: an authorizer may grant DB.T and deny db.t.
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.eq("ctl"),
                Mockito.eq("DB"), Mockito.eq("T"), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);
        assertRejected(ImmutableMap.of("table", "ctl.DB.T"), ImmutableSet.of(), "db: t");
        Mockito.verifyNoInteractions(searchOps());
    }

    @Test
    public void testRequiresSelectOnEveryColumnTheSearchMayRead() throws Exception {
        vectorSearch();
        // Every column of the table, not only those a query selects: the arguments the connector interprets (the
        // searched column, a filter) may read any of them. The result column is not one of the table's.
        Mockito.verify(accessManager).checkColumnsPriv(Mockito.any(ConnectContext.class), Mockito.eq("ctl"),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(ImmutableSet.of("id", "Embedding")),
                Mockito.eq(PrivPredicate.SELECT));

        Mockito.doThrow(new AuthorizationException("Permission denied: user [searcher] does not have privilege"
                        + " for [SELECT] command on [ctl].[db].[t].[Embedding]"))
                .when(accessManager).checkColumnsPriv(Mockito.any(ConnectContext.class), Mockito.eq("ctl"),
                        Mockito.eq("db"), Mockito.eq("t"), Mockito.anySet(), Mockito.eq(PrivPredicate.SELECT));
        assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(),
                "does not have privilege for [SELECT] command on [ctl].[db].[t].[Embedding]");
    }

    @Test
    public void testRefusesTheSearchWhileARowPolicyOrADataMaskAppliesToTheUser() throws Exception {
        Mockito.when(accessManager.evalRowFilterPolicies(Mockito.any(UserIdentity.class), Mockito.eq("ctl"),
                Mockito.eq("db"), Mockito.eq("t"))).thenReturn(
                        Collections.singletonList(RowFilterSpec.restrictive("p", "id > 5")));
        assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(),
                "vector_search() is not supported on table 'ctl.db.t' while row policies apply to it for the"
                        + " current user");

        Mockito.when(accessManager.evalRowFilterPolicies(Mockito.any(UserIdentity.class), Mockito.eq("ctl"),
                Mockito.eq("db"), Mockito.eq("t"))).thenReturn(Collections.emptyList());
        // The masks of the table's columns the search returns.
        Mockito.when(accessManager.evalDataMaskPolicies(Mockito.any(UserIdentity.class), Mockito.eq("ctl"),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(ImmutableSet.of("id", "Embedding")))).thenReturn(
                        ImmutableMap.of("embedding", new DataMaskSpec("m", "NULL")));
        assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(),
                "vector_search() is not supported on table 'ctl.db.t' while data masking policies apply to it for"
                        + " the current user");
    }

    @Test
    public void testRootAndAdminAreSubjectToNoRowPolicyOrDataMask() throws Exception {
        Mockito.when(accessManager.evalRowFilterPolicies(Mockito.any(UserIdentity.class), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(
                        Collections.singletonList(RowFilterSpec.restrictive("p", "id > 5")));
        Mockito.when(accessManager.evalDataMaskPolicies(Mockito.any(UserIdentity.class), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.anySet())).thenReturn(
                        ImmutableMap.of("id", new DataMaskSpec("m", "NULL")));
        // As for a scan of the table (LogicalCheckPolicy#findPolicy): the literal root and admin accounts.
        for (UserIdentity user : Arrays.asList(UserIdentity.ROOT, UserIdentity.ADMIN)) {
            ConnectContext.get().setCurrentUserIdentity(user);
            Assertions.assertEquals(3, vectorSearch().getTableColumns().size());
        }
        Mockito.verify(accessManager, Mockito.never()).evalRowFilterPolicies(Mockito.any(UserIdentity.class),
                Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testRoutesOnlyToAPluginCatalogWhoseConnectorServesTheSearchSpi() throws Exception {
        assertRejected(ImmutableMap.of("table", "nope.db.t"), ImmutableSet.of(), "Catalog 'nope' does not exist",
                "nope");
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        Mockito.doReturn(internal).when(catalogMgr).getCatalog("internal");
        assertRejected(ImmutableMap.of("table", "internal.db.t"), ImmutableSet.of(),
                "Catalog 'internal' does not support vector_search()", "internal");

        ConnectorMetadata plain = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(catalog.getConnector().getMetadata(Mockito.any())).thenReturn(plain);
        assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(),
                "Catalog 'ctl' does not support vector_search()");
        // Refused before the table is looked up, which may ask a remote metastore.
        Mockito.verify(catalog, Mockito.never()).getDbOrAnalysisException(Mockito.any());
    }

    @Test
    public void testReportsAMissingTableAndTheConnectorsOwnMessage() {
        Mockito.when(table.resolveConnectorTableHandle(Mockito.any(), Mockito.any())).thenReturn(Optional.empty());
        assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(), "Table 'ctl.db.t' does not exist");

        Mockito.when(table.resolveConnectorTableHandle(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.of(tableHandle));
        Mockito.when(searchOps().prepareSearch(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenThrow(new DorisConnectorException("'top_k' must be between 1 and 9223372036854775807"));
        AnalysisException error = assertRejected(ImmutableMap.of("table", "ctl.db.t", "top_k", "0"),
                ImmutableSet.of(), "'top_k' must be between 1 and 9223372036854775807");
        Assertions.assertTrue(error.getCause() instanceof DorisConnectorException);
    }

    @Test
    public void testChecksTheColumnsTheEnginesRulesNeed() {
        // The result column must come last as FLOAT: the merge TopN and the lazy fetch rely on it.
        search = searchWith(column("id", "BIGINT"), column("_distance", "DOUBLE"));
        Assertions.assertThrows(IllegalStateException.class, this::vectorSearch);
        search = searchWith(column("_distance", "FLOAT"), column("id", "BIGINT"));
        Assertions.assertThrows(IllegalStateException.class, this::vectorSearch);

        // Doris resolves names case-insensitively, and the lazy fetch adds a column with a reserved prefix.
        search = searchWith(column("Id", "BIGINT"), column("id", "BIGINT"), column("_distance", "FLOAT"));
        assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(), "has a column named 'id' twice");
        search = searchWith(column(Column.GLOBAL_ROWID_COL + "x", "BIGINT"), column("_distance", "FLOAT"));
        assertRejected(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of(),
                "using reserved Doris internal column prefix");
    }

    private VectorSearchTableValuedFunction vectorSearch() throws AnalysisException {
        return new VectorSearchTableValuedFunction(ImmutableMap.of("table", "ctl.db.t"), ImmutableSet.of());
    }

    private AnalysisException assertRejected(Map<String, String> properties, Set<String> deferred, String message) {
        return assertRejected(properties, deferred, message, null);
    }

    private AnalysisException assertRejected(Map<String, String> properties, Set<String> deferred, String message,
            String catalogName) {
        if (catalogName != null) {
            Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class), Mockito.eq(catalogName),
                    Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);
        }
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> new VectorSearchTableValuedFunction(properties, deferred));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
        return error;
    }

    private ConnectorSearch searchWith(ConnectorColumn... columns) {
        return new ConnectorSearch(scanHandle, new ArrayList<>(Arrays.asList(columns)), 10, 0,
                ConnectorSearchLazyMaterialization.NONE);
    }

    private static ConnectorColumn column(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), null, true, null);
    }

    private ConnectorSearchOps searchOps() {
        return (ConnectorSearchOps) metadata;
    }
}
