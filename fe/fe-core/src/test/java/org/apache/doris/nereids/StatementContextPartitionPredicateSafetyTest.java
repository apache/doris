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
import org.apache.doris.authorization.RowFilterMergeType;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.hive.HiveCatalogProperties;
import org.apache.doris.connector.hive.HiveConnectorMetadata;
import org.apache.doris.connector.hive.HiveTableHandle;
import org.apache.doris.connector.hive.HiveTableType;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable.ConnectorFilteredPartitionView;
import org.apache.doris.datasource.plugin.PluginDrivenSchemaCacheValue;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
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
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.qe.protocol.ResultSender;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
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
    public void ordinaryNoMaskBindsTheCompleteMixedQueryAndReusesSelectiveView() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y", "z"), "x")) {
            fixture.bindMixed("h.p = 'x'");
            Assertions.assertFalse(fixture.context.getCurrentUserIdentity().isSystemUser());
            LogicalFileScan selected = fixture.cascades.getRewritePlan().<LogicalFileScan>collectToList(
                    LogicalFileScan.class::isInstance).get(0);
            Assertions.assertEquals(Set.of("p=x"), selected.getSelectedPartitions().selectedPartitions.keySet());
            Assertions.assertTrue(selected.getSelectedPartitions().isPruned);
            Assertions.assertTrue(selected.getSelectedPartitions().connectorFilterResult.isPresent());
            Mockito.verify(fixture.table, Mockito.times(1)).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.verify(fixture.table, Mockito.never()).getSortedPartitionRanges(Mockito.any());
            Mockito.verify(fixture.hms, Mockito.times(1)).listPartitionsByFilter(
                    Mockito.eq("db"), Mockito.eq("h"), Mockito.anyString());
            Mockito.verify(fixture.hms, Mockito.never()).listPartitionNames(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
            Mockito.verify(fixture.plugin, Mockito.times(1)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.eq(Set.of("p", "id")), Mockito.any());
            fixture.prune();
            Mockito.verify(fixture.table, Mockito.times(1)).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
        }
    }

    @Test
    public void changedSourceCannotApplyNewMasksToAnOldSelectiveSubset() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            fixture.collect("h.p = 'x'");
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            AuthorizationPlugin replacement = Mockito.mock(AuthorizationPlugin.class);
            Mockito.doReturn(replacement).when(fixture.access).getAccessControllerOrDefault(Mockito.anyString());
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> fixture.statement.validatePartitionPolicy(fixture.scan()));
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
            Assertions.assertFalse(fixture.preload().hasFilteredScanView(fixture.relation.getRelationId()));
            Mockito.verifyNoInteractions(replacement);
        }
    }

    @Test
    public void maskProviderFailureDoesNotBecomeANoMaskProof() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x"), "x")) {
            Mockito.when(fixture.plugin.getDataMasks(Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any()))
                    .thenThrow(new IllegalStateException("policy unavailable"));
            Assertions.assertThrows(IllegalStateException.class, () -> fixture.collect("h.p = 'x'"));
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void missingSchemaPolicyEvidenceRetainsTheFullFallback() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            Mockito.when(fixture.table.getSchemaCacheValue(Mockito.any())).thenReturn(Optional.empty());
            fixture.collect("h.p = 'x'");
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
            Mockito.verify(fixture.plugin, Mockito.never()).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void mvccResetRevokesPredicatesAndPreventsOldPlanReuse() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            fixture.bindMixed("h.p = 'x'");
            fixture.statement.releasePlannerResources();
            fixture.statement.resetMvccSnapshots();
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> fixture.statement.validateOrRevalidatePartitionPoliciesForExecution(Mockito.mock(Planner.class)));
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    fixture.statement::lock);
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void replanRetryStartsWithFreshPolicyStateAfterTheScopeReset() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            fixture.bindMixed("h.p = 'x'");
            fixture.statement.releasePlannerResources();
            // A replan retry first drops the replaced attempt's connector scope, which revokes its proofs...
            fixture.statement.resetConnectorStatementScope();
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    fixture.statement::lock);
            // ...and then plans again from the logical statement, so the new attempt must not inherit the
            // revocation of the attempt it replaced.
            fixture.statement.beginPartitionPolicyReplan();
            fixture.lock();
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
        }
    }

    @Test
    public void preparedNextContextDoesNotInheritPolicyProofs() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            fixture.bindMixed("h.p = 'x'");
            fixture.statement.releasePlannerResources();
            PreparedStatementContext prepared = new PreparedStatementContext(null, fixture.context,
                    fixture.statement, "select h.p");
            StatementContext next = prepared.nextStatementContext();
            Assertions.assertNotSame(fixture.statement, next);
            next.validateOrRevalidatePartitionPoliciesForExecution(Mockito.mock(Planner.class));
            Assertions.assertFalse(next.hasPartitionPolicyObservation(fixture.table));
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
        }
    }

    @Test
    public void realRpcRetryRevalidatesWithoutRelistingPartitions() throws Exception {
        assertRealRetry("none");
    }

    @Test
    public void realRpcRetryRejectsChangedMasksBeforeSecondDispatch() throws Exception {
        assertRealRetry("mask");
    }

    @Test
    public void realRpcRetryRejectsChangedRowsBeforeSecondDispatch() throws Exception {
        assertRealRetry("row");
    }

    @Test
    public void realRpcRetryRejectsChangedSchemaBeforeSecondDispatch() throws Exception {
        assertRealRetry("schema");
    }

    @Test
    public void realRpcRetryPreservesProviderFailure() throws Exception {
        assertRealRetry("failure");
    }

    @Test
    public void freshFiveArgumentEvaluationDoesNotReuseThePlanningMemo() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x"), "x")) {
            fixture.bindMixed("h.p = 'x'");
            fixture.statement.releasePlannerResources();
            Map<String, DataMaskSpec> changed = Map.of("p", new DataMaskSpec("new-mask", "'y'"));
            Mockito.when(fixture.plugin.getDataMasks(Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any()))
                    .thenReturn(changed);
            Assertions.assertEquals(changed, fixture.access.evalDataMaskPolicies(
                    ordinaryUser(), "hive_catalog", "db", "h", Set.of("p")));
            Mockito.verify(fixture.plugin, Mockito.times(2)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
        }
    }

    @Test
    public void ordinaryInUsesARealBoundedHmsResult() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y", "z"), "x")) {
            fixture.bindMixed("h.p in ('x', 'z')");
            LogicalFileScan selected = fixture.cascades.getRewritePlan().<LogicalFileScan>collectToList(
                    LogicalFileScan.class::isInstance).get(0);
            Assertions.assertEquals(Set.of("p=x", "p=z"), selected.getSelectedPartitions().selectedPartitions.keySet());
            Mockito.verify(fixture.hms, Mockito.times(1)).listPartitionsByFilter(
                    Mockito.eq("db"), Mockito.eq("h"), Mockito.anyString());
            Mockito.verify(fixture.hms, Mockito.never()).listPartitionNames(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void policyMemoSeparatesUserResourceColumnsAndQueryWhileNormalizingColumnCase() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x"), "x")) {
            fixture.collect("h.p = 'x'");
            AccessControllerManager.DataPolicyContext original = fixture.access.captureDataPolicyContext(
                    ordinaryUser(), "hive_catalog", "db", "h", Set.of("p", "id"), fixture.statement);
            fixture.access.evalDataMaskPolicies(original, fixture.statement);
            fixture.access.evalDataMaskPolicies(fixture.access.captureDataPolicyContext(
                    ordinaryUser(), "hive_catalog", "db", "h", Set.of("P", "ID"), fixture.statement), fixture.statement);
            Mockito.verify(fixture.plugin, Mockito.times(1)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
            for (AccessControllerManager.DataPolicyContext isolated : List.of(
                    fixture.access.captureDataPolicyContext(
                            UserIdentity.createAnalyzedUserIdentWithIp("other_user", "%"),
                            "hive_catalog", "db", "h", Set.of("p", "id"), fixture.statement),
                    fixture.access.captureDataPolicyContext(ordinaryUser(), "hive_catalog", "db", "other_table",
                            Set.of("p", "id"), fixture.statement),
                    fixture.access.captureDataPolicyContext(ordinaryUser(), "hive_catalog", "db", "h",
                            Set.of("p"), fixture.statement))) {
                fixture.access.evalDataMaskPolicies(isolated, fixture.statement);
            }
            fixture.context.setQueryId(new TUniqueId(100, 2));
            fixture.access.evalDataMaskPolicies(fixture.access.captureDataPolicyContext(
                    ordinaryUser(), "hive_catalog", "db", "h", Set.of("p", "id"), fixture.statement), fixture.statement);
            Mockito.verify(fixture.plugin, Mockito.times(5)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
        }
    }

    @Test
    public void nullMaskAnswerFailsBeforePartitionIo() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x"), "x")) {
            Mockito.when(fixture.plugin.getDataMasks(Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any()))
                    .thenReturn(null);
            Assertions.assertThrows(IllegalStateException.class, () -> fixture.collect("h.p = 'x'"));
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
        }
    }

    @Test
    public void evenANonPartitionMaskKeepsTheConservativePolicyBarrier() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            Mockito.when(fixture.plugin.getDataMasks(Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any()))
                    .thenReturn(Map.of("id", new DataMaskSpec("non-partition-mask", "id")));
            fixture.collect("h.p = 'x'");
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
            Mockito.verify(fixture.table, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void ordinaryAliasesKeepSeparateSelectiveViews() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y", "z"), "x")) {
            fixture.bindMixedSql("select h.p from internal_catalog.db.i i cross join hive_catalog.db.h h "
                    + "cross join hive_catalog.db.h h2 where h.p = 'x' and h2.p = 'y'", false);
            List<LogicalFileScan> scans = fixture.cascades.getRewritePlan().collectToList(LogicalFileScan.class::isInstance);
            Assertions.assertEquals(2, scans.size());
            Assertions.assertEquals(Set.of(Set.of("p=x"), Set.of("p=y")), scans.stream()
                    .map(scan -> scan.getSelectedPartitions().selectedPartitions.keySet()).collect(Collectors.toSet()));
            Mockito.verify(fixture.plugin, Mockito.times(1)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
            Mockito.verify(fixture.hms, Mockito.times(2)).listPartitionsByFilter(
                    Mockito.eq("db"), Mockito.eq("h"), Mockito.anyString());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    @Test
    public void ordinaryCteKeepsItsNoMaskOriginProof() throws Exception {
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            fixture.bindMixedSql("with c as (select h.p from hive_catalog.db.h h where h.p = 'x') "
                    + "select c.p from internal_catalog.db.i i cross join c", true);
            Mockito.verify(fixture.plugin, Mockito.times(1)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
            Mockito.verify(fixture.hms, Mockito.times(1)).listPartitionsByFilter(
                    Mockito.eq("db"), Mockito.eq("h"), Mockito.anyString());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        }
    }

    private void assertRealRetry(String change) throws Exception {
        int previousRetry = Config.max_query_retry_time;
        String previousCloud = Config.cloud_unique_id;
        Config.max_query_retry_time = 1;
        Config.cloud_unique_id = "";
        try (Fixture fixture = new Fixture(Type.STRING, ordinaryUser(), List.of("x", "y"), "x")) {
            fixture.bindMixed("h.p = 'x'");
            fixture.statement.releasePlannerResources();
            Planner planner = Mockito.mock(Planner.class);
            ProtocolAdapter protocol = Mockito.mock(ProtocolAdapter.class);
            Mockito.when(protocol.canRetryQuery(fixture.context)).thenReturn(true);
            Mockito.when(fixture.context.getProtocolAdapter()).thenReturn(protocol);
            Mockito.when(fixture.context.getResultSender()).thenReturn(Mockito.mock(ResultSender.class));
            Mockito.when(fixture.context.supportHandleByFe()).thenReturn(true);
            StmtExecutor executor = Mockito.spy(new StmtExecutor(fixture.context, "select h.p"));
            executor.setStatementContext(fixture.statement);
            executor.setParsedStmt(new org.apache.doris.nereids.glue.LogicalPlanAdapter(
                    (LogicalPlan) fixture.cascades.getRewritePlan(), fixture.statement));
            Deencapsulation.setField(executor, "planner", planner);
            Mockito.doNothing().when(executor).sendResultSet(Mockito.any(), Mockito.any());
            java.util.concurrent.atomic.AtomicInteger dispatched = new java.util.concurrent.atomic.AtomicInteger();
            Mockito.when(planner.handleQueryInFe(Mockito.any())).thenAnswer(invocation -> {
                if (dispatched.incrementAndGet() == 1) {
                    if ("mask".equals(change)) {
                        Mockito.when(fixture.plugin.getDataMasks(Mockito.any(), Mockito.any(),
                                Mockito.anySet(), Mockito.any()))
                                .thenReturn(Map.of("p", new DataMaskSpec("new-mask", "'y'")));
                    }
                    if ("row".equals(change)) {
                        Mockito.when(fixture.plugin.getRowFilters(Mockito.any(), Mockito.any(), Mockito.any()))
                                .thenReturn(List.of(new RowFilterSpec("new-row", "p = 'y'",
                                        RowFilterMergeType.RESTRICTIVE)));
                    }
                    if ("schema".equals(change)) {
                        Mockito.when(fixture.table.initSchema()).thenReturn(Optional.of(
                                new PluginDrivenSchemaCacheValue(List.of(new Column("p", Type.INT)),
                                        List.of(new Column("p", Type.INT)), List.of("p"), Map.of())));
                    }
                    if ("failure".equals(change)) {
                        Mockito.when(fixture.plugin.getDataMasks(Mockito.any(), Mockito.any(),
                                Mockito.anySet(), Mockito.any())).thenThrow(new IllegalStateException("policy offline"));
                    }
                    throw new RpcException("localhost", "synthetic retry");
                }
                return Optional.of(Mockito.mock(ResultSet.class));
            });
            Method retry = StmtExecutor.class.getDeclaredMethod("handleQueryWithRetry", TUniqueId.class);
            retry.setAccessible(true);
            if (!"none".equals(change)) {
                InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                        () -> retry.invoke(executor, fixture.context.queryId()));
                if ("failure".equals(change)) {
                    Assertions.assertInstanceOf(IllegalStateException.class, failure.getCause());
                } else {
                    Assertions.assertInstanceOf(org.apache.doris.nereids.exceptions.AnalysisException.class,
                            failure.getCause());
                }
                Assertions.assertEquals(1, dispatched.get());
            } else {
                retry.invoke(executor, fixture.context.queryId());
                Assertions.assertEquals(2, dispatched.get());
            }
            Mockito.verify(fixture.plugin, Mockito.times("schema".equals(change) ? 1 : 2)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.times(1)).initSchema();
            Mockito.verify(fixture.table, Mockito.times(1)).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
        } finally {
            Config.max_query_retry_time = previousRetry;
            Config.cloud_unique_id = previousCloud;
        }
    }

    private static UserIdentity ordinaryUser() {
        return UserIdentity.createAnalyzedUserIdentWithIp("ordinary_partition_user", "%");
    }

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
            Mockito.when(fixture.plugin.getDataMasks(Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any()))
                    .thenReturn(Map.of("p", new DataMaskSpec("synthetic-partition-mask", "p + 1")));
            fixture.collect("h.p = 2");
            Assertions.assertTrue(fixture.preload().getInitialScanPredicates().isEmpty());
            fixture.statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            Assertions.assertEquals(Set.of("p=1"), fixture.preload().getScanPartitionView().get().keySet());
            Mockito.verify(fixture.plugin, Mockito.times(1)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
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
            Mockito.verify(fixture.plugin, Mockito.times(1)).getDataMasks(
                    Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any());
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
        private final AccessControllerManager access = Mockito.spy(
                new AccessControllerManager(Mockito.mock(Auth.class), true));
        private final AuthorizationPlugin plugin = Mockito.mock(AuthorizationPlugin.class);
        private final PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        private final HmsClient hms = Mockito.mock(HmsClient.class);
        private final OlapTable internal = Mockito.mock(OlapTable.class);
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
            variables.setEnableFoldConstantByBe(false);
            variables.setEnableSqlCache(false);
            variables.enableBinarySearchFilteringPartitions = false;
            queryState.setIsQuery(true);
            Mockito.when(context.getSessionVariable()).thenReturn(variables);
            Mockito.when(context.getState()).thenReturn(queryState);
            Mockito.when(context.getCurrentUserIdentity()).thenReturn(user);
            Mockito.when(context.getEnv()).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(access);
            Mockito.doReturn(plugin).when(access).getAccessControllerOrDefault(Mockito.anyString());
            AuthorizationPlugin internalPlugin = Mockito.mock(AuthorizationPlugin.class);
            Mockito.when(internalPlugin.getDataMasks(Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any()))
                    .thenReturn(Map.of());
            Mockito.when(internalPlugin.getRowFilters(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(List.of());
            Mockito.doReturn(internalPlugin).when(access).getAccessControllerOrDefault("internal_catalog");
            Mockito.when(plugin.getDataMasks(Mockito.any(), Mockito.any(), Mockito.anySet(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        Assertions.assertFalse(locked.get(), "mask evaluation must precede internal locks");
                        return Map.of();
                    });
            Mockito.when(plugin.getRowFilters(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(List.of());
            Mockito.doCallRealMethod().when(context).setQueryId(Mockito.any());
            Mockito.doCallRealMethod().when(context).queryId();
            context.setQueryId(new TUniqueId(100, 1));
            Mockito.doCallRealMethod().when(context).setThreadLocalInfo();
            statement = new StatementContext(context, new OriginStatement("select 1", 0));
            Mockito.when(context.getStatementContext()).thenReturn(statement);
            context.setThreadLocalInfo();
            for (String value : values) {
                full.put("p=" + value, new ListPartitionItem(List.of(
                        PartitionKey.createListPartitionKeyWithTypes(
                                List.of(new PartitionValue(value)), List.of(column.getType()), true))));
            }
            DatabaseIf<?> db = Mockito.mock(DatabaseIf.class);
            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(db.getCatalog()).thenReturn((CatalogIf) catalog);
            Mockito.when(db.getFullName()).thenReturn("db");
            Mockito.when(catalog.getName()).thenReturn("hive_catalog");
            Mockito.when(catalog.getConnector()).thenReturn(Mockito.mock(Connector.class));
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getType()).thenReturn(TableIf.TableType.PLUGIN_EXTERNAL_TABLE);
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
            List<Column> schemaColumns = type.equals(Type.STRING)
                    ? List.of(column, new Column("id", Type.INT)) : List.of(column);
            Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(schemaColumns);
            PluginDrivenSchemaCacheValue schema = new PluginDrivenSchemaCacheValue(
                    schemaColumns, List.of(column), List.of("p"), Map.of());
            Mockito.when(table.getSchemaCacheValue(Mockito.any())).thenReturn(Optional.of(schema));
            Mockito.when(table.initSchema()).thenAnswer(invocation -> {
                Assertions.assertFalse(locked.get(), "schema revalidation must not hold internal locks");
                return Optional.of(schema);
            });
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
            if (type.equals(Type.STRING)) {
                HiveCatalogProperties properties = HiveCatalogProperties.of(
                        Map.of(HiveCatalogProperties.HIVE_METASTORE_URIS, "thrift://localhost:9083"));
                ConnectorContext connectorContext = Mockito.mock(ConnectorContext.class);
                ConnectorStorageContext storage = Mockito.mock(ConnectorStorageContext.class);
                Mockito.when(connectorContext.getStorageContext()).thenReturn(storage);
                Mockito.when(storage.normalizeStorageUri(Mockito.anyString()))
                        .thenAnswer(invocation -> invocation.getArgument(0));
                HiveConnectorMetadata metadata = new HiveConnectorMetadata(hms, properties, connectorContext);
                HiveTableHandle handle = new HiveTableHandle.Builder("db", "h", HiveTableType.HIVE)
                        .partitionKeyNames(List.of("p")).partitionKeyTypes(Map.of("p", "STRING")).build();
                ConnectorSession session = Mockito.mock(ConnectorSession.class);
                Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
                Mockito.when(hms.listPartitionsByFilter(Mockito.eq("db"), Mockito.eq("h"), Mockito.anyString()))
                        .thenAnswer(invocation -> {
                            Assertions.assertFalse(locked.get(), "HMS RPC must not hold internal locks");
                            String filter = invocation.getArgument(2);
                            List<HmsPartitionInfo> result = new ArrayList<>();
                            for (String value : values) {
                                if (filter.contains(value)) {
                                    result.add(new HmsPartitionInfo(List.of(value), "p=" + value,
                                            null, null, null, Map.of()));
                                }
                            }
                            return result;
                        });
                Mockito.when(table.applyPartitionFilterForScan(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
                    ConnectorExpression predicate = invocation.getArgument(1);
                    FilterApplicationResult<ConnectorTableHandle> result = metadata.applyFilter(
                            session, handle, new ConnectorFilterConstraint(predicate)).orElseThrow();
                    Map<String, PartitionItem> bounded = new LinkedHashMap<>();
                    for (ConnectorPartitionInfo partition : metadata.listPartitions(
                            session, result.getHandle(), Optional.of(predicate))) {
                        bounded.put(partition.getPartitionName(), full.get(partition.getPartitionName()));
                    }
                    ConnectorFilteredPartitionView view = Mockito.mock(ConnectorFilteredPartitionView.class);
                    Mockito.when(view.getItems()).thenReturn(bounded);
                    Mockito.when(view.getFilterResult()).thenReturn(result);
                    return Optional.of(view);
                });
            }
            Mockito.when(internal.getId()).thenReturn(21L);
            DatabaseIf<?> internalDb = Mockito.mock(DatabaseIf.class);
            CatalogIf<?> internalCatalog = Mockito.mock(CatalogIf.class);
            Mockito.when(internalDb.getFullName()).thenReturn("db");
            Mockito.when(internalCatalog.getName()).thenReturn("internal_catalog");
            Mockito.when(internalDb.getCatalog()).thenReturn((CatalogIf) internalCatalog);
            Mockito.when(internal.getDatabase()).thenReturn((DatabaseIf) internalDb);
            Mockito.when(internal.getName()).thenReturn("i");
            Mockito.when(internal.getType()).thenReturn(TableIf.TableType.OLAP);
            Mockito.when(internal.getKeysType()).thenReturn(KeysType.DUP_KEYS);
            Mockito.when(internal.getRowCount()).thenReturn(1L);
            Mockito.when(internal.getPartitionIds()).thenReturn(List.of(1L));
            Mockito.when(internal.getPartitionNames()).thenReturn(Set.of("p"));
            Mockito.when(internal.getPartition(Mockito.anyLong()))
                    .thenReturn(Mockito.mock(org.apache.doris.catalog.Partition.class));
            Mockito.when(internal.getBaseSchema()).thenReturn(List.of(new Column("id", Type.INT)));
            Mockito.when(internal.getSchemaByIndexId(Mockito.anyLong()))
                    .thenReturn(List.of(new Column("id", Type.INT)));
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
            collectSql("select h.p from internal_catalog.db.i i "
                    + "cross join hive_catalog.db.h h where " + predicate);
        }

        private void collectSql(String sql) {
            plan = new NereidsParser().parseSingle(sql);
            List<UnboundRelation> relations = new ArrayList<>(plan.collectToList(UnboundRelation.class::isInstance));
            for (org.apache.doris.nereids.trees.plans.logical.LogicalCTE<?> cte
                    : plan.<org.apache.doris.nereids.trees.plans.logical.LogicalCTE<?>>collectToList(
                            org.apache.doris.nereids.trees.plans.logical.LogicalCTE.class::isInstance)) {
                for (org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias<?> alias : cte.getAliasQueries()) {
                    relations.addAll(alias.collectToList(UnboundRelation.class::isInstance));
                }
            }
            relation = relations.stream()
                    .filter(node -> node.getNameParts().equals(List.of("hive_catalog", "db", "h")))
                    .findFirst().get();
            cascades = CascadesContext.initContext(statement, plan, PhysicalProperties.ANY);
            cascades.newTableCollector(true).collect();
        }

        private ExternalTablePreloadInfo preload() {
            return statement.getExternalTablePreloadInfo(table.getId()).get();
        }

        private LogicalFileScan scan() {
            List<org.apache.doris.nereids.trees.expressions.Slot> slots =
                    table.getFullSchema(Optional.empty()).stream()
                            .<org.apache.doris.nereids.trees.expressions.Slot>map(schemaColumn -> new SlotReference(
                                    schemaColumn.getName(), DataType.fromCatalogType(schemaColumn.getType()),
                                    schemaColumn.isAllowNull(), List.of("h"))).collect(Collectors.toList());
            return new LogicalFileScan(relation.getRelationId(), table, List.of("hive_catalog", "db"), List.of(),
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(slots));
        }

        private void lock() {
            statement.lock();
            Assertions.assertTrue(locked.get());
        }

        private void prune() {
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascades,
                    ImmutableList.of(Rewriter.topDown(new PruneFileScanPartition()))).execute();
        }

        private void bindMixed(String predicate) {
            bindMixedSql("select h.p from internal_catalog.db.i i cross join hive_catalog.db.h h where "
                    + predicate, false);
        }

        private void bindMixedSql(String sql, boolean fullAnalyzer) {
            collectSql(sql);
            statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            lock();
            cascades.setCteContext(new CTEContext());
            if (fullAnalyzer) {
                cascades.newAnalyzer().analyze();
            } else {
                Rewriter.getWholeTreeRewriterWithCustomJobs(cascades,
                        ImmutableList.of(Rewriter.bottomUp(new BindRelation(), new CheckPolicy(), new BindExpression())))
                        .execute();
            }
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascades,
                    ImmutableList.of(Rewriter.bottomUp(new LogicalSubQueryAliasToLogicalProject()),
                            Rewriter.topDown(new PushDownFilterThroughJoin(),
                                    new PushDownFilterThroughProject(), new PruneFileScanPartition()))).execute();
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
