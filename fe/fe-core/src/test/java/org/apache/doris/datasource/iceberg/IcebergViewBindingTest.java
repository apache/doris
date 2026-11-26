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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.SqlDialectHelper;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class IcebergViewBindingTest extends TestWithFeService {
    private static final String VIEW_DB = "iceberg_view_store";
    private static final String SOURCE_DB = "iceberg_view_source";
    private static final String TRINO_SQL = "SELECT \"name\" AS person FROM people";

    @Override
    protected void runBeforeAll() throws Exception {
        for (String database : Arrays.asList(VIEW_DB, SOURCE_DB)) {
            createDatabase(database);
            createTable("CREATE TABLE " + database + ".people (id INT, name STRING) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        }
    }

    @Override
    protected void runBeforeEach() {
        connectContext.changeDefaultCatalog("internal");
        connectContext.setDatabase(VIEW_DB);
        connectContext.getState().setIsQuery(true);
    }

    @Test
    public void bindUnqualifiedNameInSavedNamespace() {
        Plan plan = query(null, "doris", "SELECT name AS person FROM people", null, Namespace.of(SOURCE_DB))
                .analyze().getPlan();
        Assertions.assertEquals(SOURCE_DB, scanDatabase(plan));
        Assertions.assertEquals(VIEW_DB, connectContext.getDatabase());
    }

    @Test
    public void declaredEngineKeepsContainingDatabase() {
        Plan plan = query("spark", "spark", "SELECT name AS person FROM people", null, Namespace.of(SOURCE_DB))
                .analyze().getPlan();
        Assertions.assertEquals(VIEW_DB, scanDatabase(plan));
    }

    @Test
    public void emptyNamespaceAllowsQualifiedReferencesOnly() {
        Plan plan = query(null, "doris", "SELECT name AS person FROM " + SOURCE_DB + ".people",
                null, Namespace.empty()).analyze().getPlan();
        Assertions.assertEquals(SOURCE_DB, scanDatabase(plan));
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class, () ->
                query(null, "doris", "SELECT name AS person FROM people", null, Namespace.empty()).analyze());
        Assertions.assertTrue(error.getMessage().contains("Current database is not set"));
        Assertions.assertEquals(VIEW_DB, connectContext.getDatabase());
    }

    @Test
    public void rejectUnmappedNamespaceAndCatalog() {
        for (Namespace namespace : Arrays.asList(Namespace.of("missing"), Namespace.of("nested", SOURCE_DB))) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class, () ->
                    query(null, "doris", "SELECT name AS person FROM people", null, namespace).analyze());
            Assertions.assertTrue(error.getMessage().contains("Cannot map default namespace '" + namespace + "'"));
        }
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, () ->
                query("", "doris", "SELECT name AS person FROM people", "remote", Namespace.of(SOURCE_DB)).analyze());
        Assertions.assertTrue(error.getMessage().contains("Cannot map default catalog 'remote'"));
        Assertions.assertEquals(VIEW_DB, connectContext.getDatabase());
    }

    @Test
    public void refuseUnconvertedQuotedIdentifier() {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, () ->
                query(null, "trino", TRINO_SQL, null, Namespace.of(SOURCE_DB)).analyze());
        Assertions.assertTrue(error.getMessage().contains("Cannot convert trino SQL for Iceberg view"));
    }

    @Test
    public void bindConvertedQuotedIdentifierAsColumn() {
        try (MockedStatic<SqlDialectHelper> converters = Mockito.mockStatic(SqlDialectHelper.class)) {
            converters.when(() -> SqlDialectHelper.tryConvertSqlByDialect(Mockito.eq(TRINO_SQL), Mockito.any()))
                    .thenAnswer(invocation -> {
                        SessionVariable variables = invocation.getArgument(1);
                        Assertions.assertEquals("trino", variables.getSqlDialect());
                        return Optional.of("SELECT `name` AS person FROM people");
                    });
            Plan plan = query("", "TrInO", TRINO_SQL, "internal", Namespace.of(SOURCE_DB)).analyze().getPlan();
            Assertions.assertEquals(SOURCE_DB, scanDatabase(plan));
            Assertions.assertTrue(plan.anyMatch(node -> node instanceof LogicalProject
                    && ((LogicalProject<?>) node).getProjects().stream().anyMatch(project ->
                            project.getInputSlots().stream().anyMatch(slot -> slot.getName().equals("name")))));
            Assertions.assertEquals("doris", connectContext.getSessionVariable().getSqlDialect());
        }
    }

    private String scanDatabase(Plan plan) {
        LogicalOlapScan scan = (LogicalOlapScan) plan.collectToList(node -> node instanceof LogicalOlapScan).get(0);
        return scan.getTable().getDatabase().getFullName();
    }

    private PlanChecker query(String engine, String dialect, String sql, String savedCatalog, Namespace namespace) {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
        Mockito.when(catalog.fromRemoteDatabaseName(Mockito.anyString())).thenCallRealMethod();
        IcebergExternalDatabase viewDb = new IcebergExternalDatabase(catalog, 101L, VIEW_DB, VIEW_DB);
        IcebergExternalDatabase sourceDb = new IcebergExternalDatabase(catalog, 102L, SOURCE_DB, SOURCE_DB);
        Mockito.doReturn(sourceDb).when(catalog).getDbNullable(SOURCE_DB);
        IcebergMetadataOps ops = new IcebergMetadataOps(catalog, new InMemoryCatalog());
        Mockito.when(catalog.getMetadataOps()).thenReturn(ops);
        IcebergExternalTable table = Mockito.spy(new IcebergExternalTable(103L, "v", "v", catalog, viewDb));
        Mockito.doNothing().when(table).makeSureInitialized();
        Mockito.doReturn(true).when(table).isView();
        Mockito.doReturn(Collections.singletonList(new Column("person", Type.STRING))).when(table).getFullSchema();
        ImmutableViewVersion.Builder builder = ImmutableViewVersion.builder().versionId(1).timestampMillis(1)
                .schemaId(0).defaultNamespace(namespace).defaultCatalog(savedCatalog)
                .addRepresentations(ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build());
        if (engine != null) {
            builder.putSummary("engine-name", engine);
        }
        View view = Mockito.mock(View.class);
        Mockito.when(view.currentVersion()).thenReturn(builder.build());
        Mockito.doReturn(view).when(table).getIcebergView();
        PlanChecker checker = PlanChecker.from(connectContext).parse("SELECT person FROM " + VIEW_DB + ".v");
        checker.getCascadesContext().getStatementContext().getTables()
                .put(ImmutableList.of("internal", VIEW_DB, "v"), table);
        return checker;
    }
}
