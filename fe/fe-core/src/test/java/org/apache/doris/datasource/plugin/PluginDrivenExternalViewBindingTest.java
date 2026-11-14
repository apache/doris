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

package org.apache.doris.datasource.plugin;

import org.apache.doris.catalog.Column;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.ConnectorViewDefinition;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** Exercise the real binder and body tables with a controlled connector view definition. */
public class PluginDrivenExternalViewBindingTest extends TestWithFeService {
    private static final String VIEW_DB = "external_view_store";
    private static final String SOURCE_DB = "external_view_source";
    private static final String CALLER_DB = "external_view_caller";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(VIEW_DB);
        createDatabase(SOURCE_DB);
        createDatabase(CALLER_DB);
        for (String db : Arrays.asList(VIEW_DB, SOURCE_DB)) {
            createTable("CREATE TABLE " + db + ".t (x INT, y INT) "
                    + "DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        }
    }

    @Override
    protected void runBeforeEach() {
        connectContext.changeDefaultCatalog("internal");
        connectContext.setDatabase(CALLER_DB);
        connectContext.getState().setIsQuery(true);
    }

    @Test
    public void declaredNamesBindByPosition() {
        Fixture f = new Fixture(definition("SELECT x, y FROM t", SOURCE_DB, "y", "x"));
        Plan plan = f.query("SELECT x, y FROM " + VIEW_DB + ".v").analyze().getPlan();
        Assertions.assertEquals(Arrays.asList("x", "y"), names(plan));
        Assertions.assertEquals(SOURCE_DB, findScan(plan).getTable().getDatabase().getFullName());
        f.verifyNoTableLoad();
    }

    @Test
    public void declaredRenameIsVisibleAndOldNameIsRejected() {
        Fixture f = new Fixture(definition("SELECT x FROM t", SOURCE_DB, "declared_name"));
        Assertions.assertEquals(Collections.singletonList("declared_name"),
                names(f.query("SELECT declared_name FROM " + VIEW_DB + ".v").analyze().getPlan()));
        Assertions.assertThrows(AnalysisException.class,
                () -> f.query("SELECT x FROM " + VIEW_DB + ".v").analyze());
    }

    @Test
    public void repeatedOutputsHaveIndependentIdentities() {
        Fixture f = new Fixture(definition("SELECT x, x FROM t", SOURCE_DB, "a", "b"));
        List<Slot> outputs = f.query("SELECT a, b FROM " + VIEW_DB + ".v").analyze().getPlan().getOutput();
        Assertions.assertNotEquals(outputs.get(0).getExprId(), outputs.get(1).getExprId());
    }

    @Test
    public void replaceAndExceptDoNotConflateRepeatedColumns() {
        Fixture f = new Fixture(definition("SELECT x, x FROM (SELECT CAST(10 AS INT) AS x) t", VIEW_DB, "a", "b"));
        assertConstants(f.query("SELECT * REPLACE (100 AS a) FROM " + VIEW_DB + ".v").analyze().rewrite().getPlan(),
                Arrays.asList("a", "b"), Arrays.asList("100", "10"));
        assertConstants(f.query("SELECT * EXCEPT (a) FROM " + VIEW_DB + ".v").analyze().rewrite().getPlan(),
                Collections.singletonList("b"), Collections.singletonList("10"));
    }

    @Test
    public void ctasRetainsTheDeclaredComputedColumnName() {
        Fixture f = new Fixture(definition("SELECT CAST(x + 1 AS INT) FROM t", SOURCE_DB, "declared_name"));
        f.query("SELECT declared_name FROM " + VIEW_DB + ".v");
        connectContext.getState().setIsQuery(false);
        CreateTableCommand command = (CreateTableCommand) new NereidsParser().parseSingle(
                "CREATE TABLE " + CALLER_DB + ".external_view_ctas "
                        + "PROPERTIES ('replication_num'='1') AS SELECT declared_name FROM " + VIEW_DB + ".v");
        command.validateCreateTableAsSelect(connectContext, command.getCtasQuery().get());
        Assertions.assertEquals(Collections.singletonList("declared_name"), command.getCreateTableInfo().getColumns()
                .stream().map(Column::getName).collect(Collectors.toList()));
    }

    @Test
    public void savedNamespaceIsIndependentOfTheViewLocation() {
        Fixture f = new Fixture(definition("SELECT x FROM t", SOURCE_DB, "x"));
        Plan plan = f.query("SELECT x FROM " + VIEW_DB + ".v").analyze().getPlan();
        Assertions.assertEquals(SOURCE_DB, findScan(plan).getTable().getDatabase().getFullName());
        assertCallerRestored();
    }

    @Test
    public void missingNamespaceKeepsTheExistingViewDatabaseDefault() {
        Fixture f = new Fixture(new ConnectorViewDefinition("SELECT x FROM t", "hive", columns("x")));
        Plan plan = f.query("SELECT x FROM " + VIEW_DB + ".v").analyze().getPlan();
        Assertions.assertEquals(VIEW_DB, findScan(plan).getTable().getDatabase().getFullName());
    }

    @Test
    public void emptyNamespaceDoesNotUseTheCallerOrViewDatabase() {
        Fixture f = new Fixture(definition("SELECT x FROM t", null, "x"));
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> f.query("SELECT x FROM " + VIEW_DB + ".v").analyze());
        Assertions.assertEquals("Current database is not set.", error.getMessage());
        assertCallerRestored();
    }

    @Test
    public void unusedDefaultCatalogDoesNotBlockConstantOrQualifiedSql() {
        Fixture constant = new Fixture(new ConnectorViewDefinition("SELECT CAST(10 AS INT) AS x", "spark",
                columns("x"), "unregistered_view_catalog", Collections.emptyList()));
        constant.query("SELECT x FROM " + VIEW_DB + ".v").analyze();
        Fixture qualified = new Fixture(new ConnectorViewDefinition("SELECT x FROM internal." + SOURCE_DB + ".t",
                "spark", columns("x"), "unregistered_view_catalog", Collections.emptyList()));
        qualified.query("SELECT x FROM " + VIEW_DB + ".v").analyze();
        assertCallerRestored();
    }

    @Test
    public void requiredMissingCatalogReportsAnAnalysisErrorAndRestoresTheCaller() {
        Fixture f = new Fixture(new ConnectorViewDefinition("SELECT x FROM t", "spark", columns("x"),
                "unregistered_view_catalog", Collections.singletonList(SOURCE_DB)));
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> f.query("SELECT x FROM " + VIEW_DB + ".v").analyze());
        Assertions.assertTrue(error.getMessage().contains("Unknown catalog 'unregistered_view_catalog'"));
        assertCallerRestored();
    }

    @Test
    public void oneCapturedDefinitionSuppliesSqlSchemaAndNamespace() {
        ConnectorViewDefinition first = definition("SELECT x FROM t", SOURCE_DB, "a");
        Fixture f = new Fixture(first);
        Mockito.doAnswer(invocation -> {
            ConnectorViewDefinition captured = f.current.get();
            f.current.set(definition("SELECT y FROM t", VIEW_DB, "b"));
            return captured;
        }).when(f.table).getViewDefinition();
        Plan plan = f.query("SELECT a FROM " + VIEW_DB + ".v").analyze().getPlan();
        Assertions.assertEquals(Collections.singletonList("a"), names(plan));
        Assertions.assertEquals(SOURCE_DB, findScan(plan).getTable().getDatabase().getFullName());
        Mockito.verify(f.table).getViewDefinition();
        f.verifyNoTableLoad();
    }

    private void assertCallerRestored() {
        Assertions.assertEquals("internal", connectContext.getDefaultCatalog());
        Assertions.assertEquals(CALLER_DB, connectContext.getDatabase());
    }

    private static LogicalOlapScan findScan(Plan plan) {
        return (LogicalOlapScan) plan.collectToList(node -> node instanceof LogicalOlapScan).get(0);
    }

    private static List<String> names(Plan plan) {
        return plan.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
    }

    private static void assertConstants(Plan plan, List<String> expectedNames, List<String> expectedValues) {
        Assertions.assertEquals(expectedNames, names(plan));
        LogicalOneRowRelation row = (LogicalOneRowRelation) plan
                .collectToList(node -> node instanceof LogicalOneRowRelation).get(0);
        Assertions.assertEquals(expectedValues, row.getProjects().stream().map(expression ->
                Assertions.assertInstanceOf(Literal.class,
                        Assertions.assertInstanceOf(Alias.class, expression).child()).getStringValue())
                .collect(Collectors.toList()));
    }

    private static List<ConnectorColumn> columns(String... names) {
        return Arrays.stream(names).map(name -> new ConnectorColumn(
                name, ConnectorType.of("INT"), "", true, null, true)).collect(Collectors.toList());
    }

    private static ConnectorViewDefinition definition(String sql, String namespace, String... names) {
        return new ConnectorViewDefinition(sql, "spark", columns(names), null,
                namespace == null ? Collections.emptyList() : Collections.singletonList(namespace));
    }

    private class Fixture {
        final AtomicReference<ConnectorViewDefinition> current;
        final ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        final PluginDrivenMvccExternalTable table;

        Fixture(ConnectorViewDefinition definition) {
            current = new AtomicReference<>(definition);
            ConnectorSession session = Mockito.mock(ConnectorSession.class);
            Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
            Connector connector = Mockito.mock(Connector.class);
            Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
            Mockito.when(metadata.fromRemoteColumnName(
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString()))
                    .thenAnswer(invocation -> invocation.getArgument(3));
            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(catalog.getId()).thenReturn(10001L);
            Mockito.when(catalog.getName()).thenReturn("internal");
            Mockito.when(catalog.getConnector()).thenReturn(connector);
            Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
            PluginDrivenExternalDatabase database = new PluginDrivenExternalDatabase(catalog, 10002L, VIEW_DB, VIEW_DB);
            table = Mockito.spy(new PluginDrivenMvccExternalTable(10003L, "v", "v", catalog, database));
            Mockito.doNothing().when(table).makeSureInitialized();
            Mockito.doReturn(true).when(table).isView();
            Mockito.doAnswer(invocation -> current.get()).when(table).getViewDefinition();
            Mockito.doThrow(new AssertionError("Binding must use the captured view schema"))
                    .when(table).getFullSchema();
        }

        PlanChecker query(String sql) {
            PlanChecker checker = PlanChecker.from(connectContext).parse(sql);
            checker.getCascadesContext().getStatementContext().getTables()
                    .put(ImmutableList.of("internal", VIEW_DB, "v"), table);
            return checker;
        }

        void verifyNoTableLoad() {
            Mockito.verify(metadata, Mockito.never()).getTableHandle(Mockito.any(), Mockito.any(), Mockito.any());
        }
    }
}
