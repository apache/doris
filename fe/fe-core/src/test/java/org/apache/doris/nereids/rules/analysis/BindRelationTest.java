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
import org.apache.doris.common.Config;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.analyzer.UnboundRelation;
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
import org.apache.doris.qe.BDPAuthContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
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
    void bindHiveViewColumnAliases() throws Exception {
        // Enable hive view query path
        Config.enable_query_hive_views = true;

        // Prepare BDPAuthContext with view-based mode to satisfy HMSExternalDatabase table name resolution
        BDPAuthContext authContext = new BDPAuthContext("erp", "source", "bdp_user", "token", true);
        connectContext.setBdpAuthContext(authContext);
        authContext.setThreadLocalInfo();

        // Create HMS catalog
        String createStmt = "create catalog hms_ctl properties("
                + "'type' = 'hms', "
                + "'hive.metastore.uris' = 'thrift://192.168.0.1:9083'"
                + ");";
        createCatalog(createStmt);

        // Get catalog and mark initialized for test
        HMSExternalCatalog hmsCatalog = (HMSExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog("hms_ctl");
        hmsCatalog.setInitializedForTest(true);

        // Create HMS database and register into catalog
        HMSExternalDatabase hmsDb = new HMSExternalDatabase(hmsCatalog, 10000L, "hms_db", "hms_db");
        hmsCatalog.addDatabaseForTest(hmsDb);

        // Mock a HMS view with schema columns and view text
        java.util.List<Column> schema = new ArrayList<>();
        schema.add(new Column("col1", PrimitiveType.INT));
        schema.add(new Column("col2", PrimitiveType.INT));

        HMSExternalTable viewTbl = Mockito.mock(HMSExternalTable.class);

        long now = System.currentTimeMillis();
        Mockito.when(viewTbl.getId()).thenReturn(10002L);
        // IMPORTANT: use suffixed local name compatible with HMSExternalDatabase.getTableNullable()
        Mockito.when(viewTbl.getName()).thenReturn("hms_view1$bdp_user$true");
        Mockito.when(viewTbl.getDbName()).thenReturn("hms_db");
        Mockito.when(viewTbl.isView()).thenReturn(true);
        Mockito.when(viewTbl.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(viewTbl.getType())
                .thenReturn(org.apache.doris.catalog.TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(viewTbl.getFullSchema()).thenReturn(schema);
        Mockito.when(viewTbl.getViewText()).thenReturn("SELECT 1 AS col1, 2 AS col2");
        Mockito.when(viewTbl.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(viewTbl.getDlaType())
                .thenReturn(org.apache.doris.datasource.hive.HMSExternalTable.DLAType.HIVE);
        Mockito.when(viewTbl.getDatabase()).thenReturn(hmsDb);
        Mockito.when(viewTbl.getNewestUpdateVersionOrTime()).thenReturn(now);

        // Register view into DB meta cache
        hmsDb.addTableForTest(viewTbl);

        // Bind the external hive view with qualifier catalog.db.table
        Plan plan = PlanRewriter.bottomUpRewrite(new UnboundRelation(
                        StatementScopeIdGenerator.newRelationId(), ImmutableList.of("hms_ctl", "hms_db", "hms_view1")),
                        connectContext, new BindRelation());

        // Verify LogicalSubQueryAlias carries column aliases propagated from view schema
        Assertions.assertInstanceOf(LogicalSubQueryAlias.class, plan);
        LogicalSubQueryAlias<?> alias = (LogicalSubQueryAlias<?>) plan;

        Assertions.assertTrue(alias.getColumnAliases().isPresent(), "Column aliases should be present for hive view");
        Assertions.assertEquals(com.google.common.collect.ImmutableList.of("col1", "col2"),
                alias.getColumnAliases().get(), "Hive view column aliases mismatch");

        List<String> outputNames = alias.computeOutput().stream()
                .map(Slot::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(ImmutableList.of("col1", "col2"),
                outputNames, "Output slot names should match column aliases");

        // Qualifier should be catalog.db.table
        Assertions.assertEquals(ImmutableList.of("hms_ctl", "hms_db", "hms_view1"), alias.getQualifier());
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
