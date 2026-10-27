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

package org.apache.doris.nereids;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable.ConnectorFilteredPartitionView;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughProject;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** Exercises actual collection/preload and later semantic rewrites without a live metastore or cluster. */
public class StatementContextPartitionPredicateSafetyTest {
    @Test
    public void decimalCalculationDoesNotBecomeAPreCoercionPartitionValue() throws Exception {
        boolean previousConversion = Config.enable_decimal_conversion;
        Config.enable_decimal_conversion = true;
        try (Fixture fixture = new Fixture(ScalarType.createDecimalV3Type(2, 1), UserIdentity.ROOT,
                List.of("0.0", "0.5"), "0.0")) {
            fixture.collect("h.p = CAST(1.0 / 2.0 AS DECIMAL(2,1))");
            Expression initial = fixture.preload().getInitialScanPredicates().get(fixture.relation.getRelationId());
            Assertions.assertFalse(initial.child(1) instanceof DecimalV3Literal,
                    "collection must retain the calculation, not its pre-coercion value");
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Assertions.assertEquals(Set.of("p=0.0", "p=0.5"),
                    fixture.preload().getScanPartitionView().get().keySet());
            fixture.lock();

            Expression coerced = ExpressionAnalyzer.analyzeFunction(
                    fixture.plan, fixture.cascades, initial.child(1));
            Expression value = FoldConstantRule.evaluate(coerced, new ExpressionRewriteContext(fixture.cascades));
            Assertions.assertEquals(0, ((DecimalV3Literal) value).getValue().compareTo(new BigDecimal("0.5")));
            LogicalFileScan scan = fixture.scan();
            Expression predicate = new EqualTo(scan.getOutput().get(0), value);
            fixture.cascades.setRewritePlan(new LogicalFilter<>(Set.of(predicate), scan));
            fixture.prune();
            LogicalFilter<?> rewritten = (LogicalFilter<?>) fixture.cascades.getRewritePlan();
            Assertions.assertEquals(Set.of("p=0.5"),
                    ((LogicalFileScan) rewritten.child()).getSelectedPartitions().selectedPartitions.keySet());
            Assertions.assertEquals(predicate, rewritten.getPredicate());
            Mockito.verify(fixture.table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            Config.enable_decimal_conversion = previousConversion;
        }
    }

    @Test
    public void nonIdentityMaskDoesNotPrefilterTheRawPartitionValue() throws Exception {
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("partition_mask_user", "%");
        try (Fixture fixture = new Fixture(Type.INT, user, List.of("1"), "2")) {
            Mockito.when(fixture.access.evalDataMaskPolicies(Mockito.any(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anySet()))
                    .thenReturn(Map.of("p", new DataMaskSpec("synthetic-partition-mask", "p + 1")));
            Mockito.when(fixture.access.evalRowFilterPolicies(Mockito.any(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString())).thenReturn(List.of());
            fixture.collect("h.p = 2");
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertEquals(Set.of("p=1"), fixture.preload().getScanPartitionView().get().keySet());
            Mockito.verify(fixture.access, Mockito.never()).evalDataMaskPolicies(Mockito.any(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anySet());
            fixture.lock();

            LogicalFileScan rawScan = fixture.scan();
            // Reduced bound Hive branch of the parsed mixed query. The statement owns the internal lock;
            // actual CheckPolicy and BindExpression install and bind the nonidentity mask.
            fixture.cascades.setRewritePlan(new LogicalFilter<>(
                    Set.of(new EqualTo(new UnboundSlot("h", "p"), new IntegerLiteral(2))),
                    new LogicalCheckPolicy<>(rawScan)));
            Rewriter.getWholeTreeRewriterWithCustomJobs(fixture.cascades,
                    ImmutableList.of(Rewriter.bottomUp(new CheckPolicy(), new BindExpression()))).execute();
            Rewriter.getWholeTreeRewriterWithCustomJobs(fixture.cascades,
                    ImmutableList.of(Rewriter.topDown(new PushDownFilterThroughProject()))).execute();
            fixture.prune();

            LogicalProject<?> project = (LogicalProject<?>) fixture.cascades.getRewritePlan();
            LogicalFilter<?> rawFilter = (LogicalFilter<?>) project.child();
            LogicalFileScan selected = (LogicalFileScan) rawFilter.child();
            Assertions.assertEquals(Set.of("p=1"), selected.getSelectedPartitions().selectedPartitions.keySet());
            Assertions.assertFalse(rawFilter.getPredicate().getInputSlots().isEmpty());
            Expression maskedValue = project.getProjects().get(0).child(0).rewriteUp(
                    expression -> expression instanceof SlotReference ? new IntegerLiteral(1) : expression);
            Literal foldedMask = (Literal) FoldConstantRule.evaluate(
                    maskedValue, new ExpressionRewriteContext(fixture.cascades));
            Assertions.assertEquals("2", foldedMask.getStringValue());
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.verify(fixture.access, Mockito.times(1)).evalDataMaskPolicies(Mockito.any(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anySet());
        }
    }

    @Test
    public void fixedAccountLiteralKeepsTheSelectivePreloadFastPath() throws Exception {
        try (Fixture fixture = new Fixture(Type.INT, UserIdentity.ROOT, List.of("1", "2"), "2")) {
            fixture.collect("h.p = 2");
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertEquals(Set.of("p=2"),
                    fixture.preload().getFilteredScanView(fixture.relation.getRelationId()).get().getItems().keySet());
            Mockito.verify(fixture.table, Mockito.times(1)).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.verify(fixture.access, Mockito.never()).evalDataMaskPolicies(Mockito.any(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anySet());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final ConnectContext previousContext = ConnectContext.get();
        private final ConnectContext context = Mockito.mock(ConnectContext.class);
        private final SessionVariable variables = new SessionVariable();
        private final QueryState queryState = new QueryState();
        private final Env env = Mockito.mock(Env.class);
        private final AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        private final PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        private final TableIf internal = Mockito.mock(TableIf.class);
        private final AtomicBoolean locked = new AtomicBoolean();
        private final Map<String, PartitionItem> full = new LinkedHashMap<>();
        private final Column column;
        private final StatementContext statement;
        private LogicalPlan plan;
        private CascadesContext cascades;
        private UnboundRelation relation;

        @SuppressWarnings("unchecked")
        private Fixture(Type type, UserIdentity user, List<String> values, String rawFilterValue) throws Exception {
            column = new Column("p", type);
            variables.setEnableMaterializedViewRewrite(false);
            variables.setEnableDmlMaterializedViewRewrite(false);
            variables.setEnableFoldConstantByBe(false);
            variables.setEnableSqlCache(false);
            variables.enableBinarySearchFilteringPartitions = false;
            queryState.setIsQuery(true);
            Mockito.when(context.getSessionVariable()).thenReturn(variables);
            Mockito.when(context.getState()).thenReturn(queryState);
            Mockito.when(context.getCurrentUserIdentity()).thenReturn(user);
            Mockito.when(context.getEnv()).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(access);
            Mockito.doCallRealMethod().when(context).setThreadLocalInfo();
            statement = new StatementContext(context, new OriginStatement("select 1", 0));
            Mockito.when(context.getStatementContext()).thenReturn(statement);
            context.setThreadLocalInfo();
            for (String value : values) {
                full.put("p=" + value, new ListPartitionItem(List.of(
                        PartitionKey.createPartitionKey(List.of(new PartitionValue(value)), List.of(column)))));
            }
            DatabaseIf<?> db = Mockito.mock(DatabaseIf.class);
            CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
            Mockito.when(db.getCatalog()).thenReturn((CatalogIf) catalog);
            Mockito.when(db.getFullName()).thenReturn("db");
            Mockito.when(catalog.getName()).thenReturn("hive_catalog");
            Mockito.when(table.getDatabase()).thenReturn((DatabaseIf) db);
            Mockito.when(table.getId()).thenReturn(22L);
            Mockito.when(table.getName()).thenReturn("h");
            Mockito.when(table.getDbName()).thenReturn("db");
            Mockito.when(table.getFullQualifiers()).thenReturn(List.of("hive_catalog", "db", "h"));
            Mockito.when(table.supportsExternalMetadataPreload()).thenReturn(true);
            Mockito.when(table.supportsConnectorPartitionPruning()).thenReturn(true);
            Mockito.when(table.supportInternalPartitionPruned()).thenReturn(true);
            Mockito.when(table.supportInternalPartitionPruned(Mockito.any())).thenReturn(true);
            Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(List.of(column));
            Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(List.of(column));
            Mockito.when(table.initSelectedPartitions(Mockito.any()))
                    .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
            Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any())).thenAnswer(invocation -> {
                Assertions.assertFalse(locked.get(), "full HMS materialization must precede the internal lock");
                return Optional.of(full);
            });
            Mockito.when(table.applyPartitionFilterForScan(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
                Assertions.assertFalse(locked.get(), "selective HMS request must precede the internal lock");
                ConnectorExpression predicate = invocation.getArgument(1);
                ConnectorFilteredPartitionView view = Mockito.mock(ConnectorFilteredPartitionView.class);
                Mockito.when(view.getItems()).thenReturn(full.entrySet().stream()
                        .filter(entry -> entry.getKey().equals("p=" + rawFilterValue))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                Mockito.when(view.getFilterResult()).thenReturn(new FilterApplicationResult<>(
                        Mockito.mock(ConnectorTableHandle.class), predicate, false));
                return Optional.of(view);
            });
            Mockito.when(internal.getId()).thenReturn(21L);
            Mockito.when(internal.needReadLockWhenPlan()).thenReturn(true);
            Mockito.when(internal.tryReadLock(1, TimeUnit.MINUTES)).thenAnswer(invocation -> {
                locked.set(true);
                return true;
            });
            Mockito.doAnswer(invocation -> {
                locked.set(false);
                return null;
            }).when(internal).readUnlock();
            statement.getTables().put(List.of("internal_catalog", "db", "i"), internal);
            statement.getTables().put(List.of("hive_catalog", "db", "h"), table);
        }

        private void collect(String predicate) {
            plan = new NereidsParser().parseSingle("select h.p from internal_catalog.db.i i "
                    + "cross join hive_catalog.db.h h where " + predicate);
            relation = plan.collect(node -> node instanceof UnboundRelation).stream()
                    .map(node -> (UnboundRelation) node)
                    .filter(node -> node.getNameParts().equals(List.of("hive_catalog", "db", "h")))
                    .findFirst().get();
            cascades = CascadesContext.initContext(statement, plan, PhysicalProperties.ANY);
            cascades.newTableCollector(true).collect();
        }

        private ExternalTablePreloadInfo preload() {
            return statement.getExternalTablePreloadInfo(table.getId()).get();
        }

        private LogicalFileScan scan() {
            SlotReference slot = new SlotReference("p", DataType.fromCatalogType(column.getType()), false, List.of("h"));
            return new LogicalFileScan(relation.getRelationId(), table, List.of("hive_catalog", "db"), List.of(),
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(List.of(slot)));
        }

        private void lock() {
            statement.lock();
            Assertions.assertTrue(locked.get());
        }

        private void prune() {
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascades,
                    ImmutableList.of(Rewriter.topDown(new PruneFileScanPartition()))).execute();
        }

        @Override
        public void close() {
            try {
                statement.close();
            } finally {
                ConnectContext.remove();
                if (previousContext != null) {
                    previousContext.setThreadLocalInfo();
                }
            }
        }
    }
}
