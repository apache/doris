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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalDatabase;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class BindRelationTest extends TestWithFeService implements GeneratedPlanPatterns {
    private static final String DB1 = "db1";
    private static final String DB2 = "db2";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB1);
        createTable("CREATE TABLE db1.t ( \n"
                + " \ta INT,\n"
                + " \tb VARCHAR\n"
                + ")ENGINE=OLAP\n"
                + "DISTRIBUTED BY HASH(`a`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\"= \"1\");");
        createTable("CREATE TABLE db1.tagg ( \n"
                + " \ta INT,\n"
                + " \tb INT SUM\n"
                + ")ENGINE=OLAP AGGREGATE KEY(a)\n "
                + "DISTRIBUTED BY random BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\"= \"1\");");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void bindHiveViewPropagatesColumnAliases() throws Exception {
        String catalogName = "plugin_view_catalog";
        String dbName = "plugin_view_db";
        String viewName = "plugin_view";

        // The connector SPI is not exercised: every seam that would normally reach it
        // (isView/getFullSchema/getViewText) is stubbed directly on the table spy below.
        Connector connectorMock = Mockito.mock(Connector.class);
        Map<String, String> props = new HashMap<>();
        props.put("type", "hive");
        TestPluginCatalog catalog = new TestPluginCatalog(
                Env.getCurrentEnv().getNextId(), catalogName, props, connectorMock);
        // addCatalog() is private: it is the minimal registration primitive (map insertion +
        // resetToUninitialized) that createCatalogInternal() itself delegates to, without the
        // SQL-parsing/checkProperties overhead that createCatalogInternal adds on top.
        Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "addCatalog", catalog);
        // addCatalog() resets the catalog to uninitialized; force it back so buildMetaCache()
        // (invoked by addDatabaseForTest/addTableForTest below) is a no-op rather than a real
        // metadata refresh against the (mocked) connector.
        catalog.setInitializedForTest(true);

        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase(
                catalog, Util.genIdByName(catalogName, dbName), dbName, dbName);
        catalog.addDatabaseForTest(db);

        // Declared view schema uses col1/col2; the view body deliberately uses different
        // slot names (a/b) so alias propagation is observable in computeOutput().
        List<Column> viewSchema = ImmutableList.of(
                new Column("col1", PrimitiveType.INT),
                new Column("col2", PrimitiveType.INT));

        // The id must match ExternalDatabase.getTableNullable()'s expectedTableId
        // (Util.genIdByName(catalogName, dbName, tableName)), or the id/name identity check on lookup fails.
        PluginDrivenExternalTable pluginTable = Mockito.spy(new PluginDrivenExternalTable(
                Util.genIdByName(catalogName, dbName, viewName), viewName, viewName, catalog, db));
        // doReturn() stubs bypass the real method (and its makeSureInitialized() guard),
        // so no live connector metadata call is made.
        Mockito.doReturn(true).when(pluginTable).isView();
        Mockito.doReturn(viewSchema).when(pluginTable).getFullSchema();
        Mockito.doReturn("SELECT 1 AS a, 2 AS b").when(pluginTable).getViewText();
        db.addTableForTest(pluginTable);

        try {
            Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(
                    StatementScopeIdGenerator.newRelationId(),
                    ImmutableList.of(catalogName, dbName, viewName)),
                    connectContext, new BindRelation());

            Assertions.assertInstanceOf(LogicalSubQueryAlias.class, plan);
            LogicalSubQueryAlias<?> alias = (LogicalSubQueryAlias<?>) plan;
            Assertions.assertTrue(alias.getColumnAliases().isPresent());
            Assertions.assertEquals(ImmutableList.of("col1", "col2"), alias.getColumnAliases().get());

            List<String> outputNames = alias.computeOutput().stream()
                    .map(Slot::getName)
                    .collect(Collectors.toList());
            Assertions.assertEquals(ImmutableList.of("col1", "col2"), outputNames);
        } finally {
            // Mirror the original test's teardown, but via the same low-level primitive used to
            // register the catalog (removeCatalog is the private counterpart of addCatalog; the
            // public dropCatalog() expects a catalog created through the full SQL/edit-log path).
            Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "removeCatalog", catalog.getId());
        }
    }

    /**
     * getFullSchema() returns null when the plugin table's schema cache is empty (e.g. after a
     * catalog drop / cache-lifecycle miss). BindRelation must not dereference it: the alias list
     * stays absent and LogicalSubQueryAlias falls back to the analyzed view-body slot names.
     */
    @Test
    void bindHiveViewWithNullSchemaFallsBackToBodyNames() throws Exception {
        String catalogName = "plugin_view_null_catalog";
        String dbName = "plugin_view_null_db";
        String viewName = "plugin_view_null";

        Connector connectorMock = Mockito.mock(Connector.class);
        Map<String, String> props = new HashMap<>();
        props.put("type", "hive");
        TestPluginCatalog catalog = new TestPluginCatalog(
                Env.getCurrentEnv().getNextId(), catalogName, props, connectorMock);
        Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "addCatalog", catalog);
        catalog.setInitializedForTest(true);

        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase(
                catalog, Util.genIdByName(catalogName, dbName), dbName, dbName);
        catalog.addDatabaseForTest(db);

        PluginDrivenExternalTable pluginTable = Mockito.spy(new PluginDrivenExternalTable(
                Util.genIdByName(catalogName, dbName, viewName), viewName, viewName, catalog, db));
        Mockito.doReturn(true).when(pluginTable).isView();
        // Empty schema cache -> null schema, exactly as ExternalTable.getFullSchema() returns.
        Mockito.doReturn(null).when(pluginTable).getFullSchema();
        Mockito.doReturn("SELECT 1 AS a, 2 AS b").when(pluginTable).getViewText();
        db.addTableForTest(pluginTable);

        try {
            Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(
                    StatementScopeIdGenerator.newRelationId(),
                    ImmutableList.of(catalogName, dbName, viewName)),
                    connectContext, new BindRelation());

            Assertions.assertInstanceOf(LogicalSubQueryAlias.class, plan);
            LogicalSubQueryAlias<?> alias = (LogicalSubQueryAlias<?>) plan;
            // No declared schema -> no aliases; output falls back to the view-body slot names.
            Assertions.assertFalse(alias.getColumnAliases().isPresent());

            List<String> outputNames = alias.computeOutput().stream()
                    .map(Slot::getName)
                    .collect(Collectors.toList());
            Assertions.assertEquals(ImmutableList.of("a", "b"), outputNames);
        } finally {
            Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "removeCatalog", catalog.getId());
        }
    }

    /**
     * SPI-registered {@code ConnectorProvider} for the catalog's {@code type} property; no such provider
     * is registered in the fe-core unit-test classpath (plugin connectors are separate, external JARs).
     * {@code createConnectorFromProperties} is documented as "Extracted as a protected method so tests
     * can override without depending on the static ConnectorFactory registry" -- this mirrors the same
     * override used by {@code PluginDrivenExternalCatalogErrorMsgTest.TestErrorCatalog}: keep the connector
     * injected via the constructor and skip the real (SPI-backed) local-object initialization entirely.
     */
    private static final class TestPluginCatalog extends PluginDrivenExternalCatalog {
        TestPluginCatalog(long catalogId, String name, Map<String, String> props, Connector connector) {
            super(catalogId, name, null, props, "", connector);
        }

        @Override
        protected Connector createConnectorFromProperties() {
            return null;
        }

        @Override
        protected void initLocalObjectsImpl() {
            // Connector is already injected via the constructor; nothing to build.
        }
    }

    @Test
    void bindInCurrentDb() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB1);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("t")),
                connectContext, new BindRelation());

        Assertions.assertInstanceOf(LogicalOlapScan.class, plan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "t"),
                ((LogicalOlapScan) plan).qualified());
    }

    @Test
    void bindByDbQualifier() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB2);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("db1", "t")),
                connectContext, new BindRelation());

        Assertions.assertInstanceOf(LogicalOlapScan.class, plan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "t"),
                ((LogicalOlapScan) plan).qualified());
    }

    @Test
    void rejectIncrementalReadWithoutRowBinlog() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext)
                        .analyze("SELECT a, b, __DORIS_BINLOG_OP__ "
                                + "FROM db1.t@incr(\"incrementType\" = \"DETAIL\")"));

        Assertions.assertEquals("INCR query requires ROW binlog enabled on base table.", exception.getMessage());
    }

    @Test
    void rejectOptionsOnUnsupportedTableType() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext)
                        .analyze("SELECT * FROM db1.t@options('scan.snapshot-id'='1')"));

        // WHY the wording differs from upstream ("only supported for Paimon tables"): post-cutover the gate
        // is a CONNECTOR CAPABILITY (SUPPORTS_SCAN_PARAM_OPTIONS), not a table class, so any connector may
        // declare it and naming paimon would be wrong. The rejection itself is what matters and must stay:
        // @options only reaches a connector through the MVCC pin path, so a table that never enters it would
        // silently drop the clause and answer a historical query with latest data.
        // MUTATION: dropping validateOptionsTarget -> no exception -> red.
        Assertions.assertEquals(
                "OPTIONS scan params are not supported for table t.", exception.getMessage());
    }

    @Test
    void rejectOptionsOnCteReference() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext)
                        .analyze("WITH c AS (SELECT * FROM db1.t) "
                                + "SELECT * FROM c@options('scan.snapshot-id'='1')"));

        Assertions.assertEquals(
                "Table scan parameters are not supported on CTE references.", exception.getMessage());
    }

    @Test
    void bindSchemaTable() {
        boolean originValue = connectContext.getSessionVariable().isFetchAllFeForSystemTable();
        try {
            connectContext.getSessionVariable().setFetchAllFeForSystemTable(true);
            // test table which should fetch all fe
            Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                            ImmutableList.of("information_schema", "sql_block_rule_status")),
                    connectContext, new BindRelation());
            Assertions.assertInstanceOf(LogicalAggregate.class, plan);
            Assertions.assertInstanceOf(LogicalSubQueryAlias.class, plan.child(0));
            Assertions.assertInstanceOf(LogicalSchemaScan.class, plan.child(0).child(0));
            // test table which should not fetch all fe
            plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                            ImmutableList.of("information_schema", "tables")),
                    connectContext, new BindRelation());
            Assertions.assertInstanceOf(LogicalSubQueryAlias.class, plan);
            Assertions.assertInstanceOf(LogicalSchemaScan.class, plan.child(0));
            // test table which should fetch all fe but close session variable
            connectContext.getSessionVariable().setFetchAllFeForSystemTable(false);
            plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                            ImmutableList.of("information_schema", "sql_block_rule_status")),
                    connectContext, new BindRelation());
            Assertions.assertInstanceOf(LogicalSubQueryAlias.class, plan);
            Assertions.assertInstanceOf(LogicalSchemaScan.class, plan.child(0));
        } finally {
            connectContext.getSessionVariable().setFetchAllFeForSystemTable(originValue);
        }
    }

    @Test
    void bindRandomAggTable() {
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB1);
        connectContext.getState().setIsQuery(true);
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(StatementScopeIdGenerator.newRelationId(), ImmutableList.of("tagg")),
                connectContext, new BindRelation());

        Assertions.assertInstanceOf(LogicalAggregate.class, plan);
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "tagg"),
                plan.getOutput().get(0).getQualifier());
        Assertions.assertEquals(
                ImmutableList.of("internal", DEFAULT_CLUSTER_PREFIX + DB1, "tagg"),
                plan.getOutput().get(1).getQualifier());
    }

    @Test
    void testBindRandomAggTableExprIdSame() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getState().setIsQuery(true);
        PlanChecker.from(connectContext)
                .checkPlannerResult("select * from db1.tagg",
                        planner -> {
                            List<Alias> collectedAlias = new ArrayList<>();
                            planner.getCascadesContext().getRewritePlan().accept(
                                    new DefaultPlanVisitor<Void, List<Alias>>() {
                                        @Override
                                        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                                                List<Alias> context) {
                                            for (Expression expression : aggregate.getExpressions()) {
                                                collectedAlias.addAll(
                                                        expression.collectToList(Alias.class::isInstance));
                                            }
                                            return super.visitLogicalAggregate(aggregate, context);
                                        }
                                    }, collectedAlias);
                            for (Alias alias : collectedAlias) {
                                for (Expression child : alias.children()) {
                                    Set<ExprId> childExpressionSet =
                                            child.collectToSet(NamedExpression.class::isInstance).stream()
                                                    .map(expr -> ((NamedExpression) expr).getExprId())
                                                    .collect(Collectors.toSet());
                                    Assertions.assertFalse(childExpressionSet.contains(alias.getExprId()));
                                }
                            }
                        });
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }
}
