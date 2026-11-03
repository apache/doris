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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable.ConnectorFilteredPartitionView;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.CTEInline;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Ordinary CTE inlining must reuse the pre-lock connector view, not issue a second HMS request. */
public class StatementContextCtePartitionPruningTest {
    @Test
    public void testOneUseCteReusesFilteredViewWithDefaultMvSettings() throws Exception {
        assertCteViewReuse(true, true, false);
    }

    @Test
    public void testOneUseCteReusesFilteredViewForExecutablePlanWithMvDisabled() throws Exception {
        assertCteViewReuse(false, true, false);
    }

    @Test
    public void testOneUseCteReusesFilteredViewForPlanOnlyExplain() throws Exception {
        assertCteViewReuse(false, false, false);
    }

    @Test
    public void testOneUseCteReusesDeclinedFilterFullView() throws Exception {
        assertCteViewReuse(true, true, true);
    }

    @SuppressWarnings("unchecked")
    private static void assertCteViewReuse(boolean mvEnabled, boolean finalizePhysical, boolean declined)
            throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        SessionVariable variables = context.getSessionVariable();
        if (!mvEnabled) {
            variables.setEnableMaterializedViewRewrite(false);
            variables.setEnableDmlMaterializedViewRewrite(false);
        }
        StatementContext statement = new StatementContext(context, null);
        context.setStatementContext(statement);
        context.setThreadLocalInfo();
        AtomicBoolean locked = new AtomicBoolean();
        TableIf internal = Mockito.mock(TableIf.class);
        Mockito.when(internal.getId()).thenReturn(1L);
        Mockito.when(internal.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(internal.tryReadLock(1, TimeUnit.MINUTES)).thenAnswer(invocation -> {
            locked.set(true);
            return true;
        });
        Mockito.doAnswer(invocation -> {
            locked.set(false);
            return null;
        }).when(internal).readUnlock();
        Column column = new Column("p", Type.STRING);
        PluginDrivenExternalTable hive = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(hive.getId()).thenReturn(41L);
        Mockito.when(hive.getName()).thenReturn("h");
        Mockito.when(hive.getFullSchema(Mockito.any())).thenReturn(List.of(column));
        Mockito.when(hive.getPartitionColumns(Mockito.any())).thenReturn(List.of(column));
        Mockito.when(hive.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hive.supportsConnectorPartitionPruning()).thenReturn(true);
        Mockito.when(hive.supportInternalPartitionPruned(Mockito.any())).thenReturn(true);
        Mockito.when(hive.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Map<String, PartitionItem> items = Map.of("p=x", new ListPartitionItem(List.of(
                PartitionKey.createPartitionKey(List.of(new PartitionValue("x")), List.of(column)))));
        ConnectorFilteredPartitionView view = Mockito.mock(ConnectorFilteredPartitionView.class);
        Mockito.when(view.getItems()).thenReturn(items);
        FilterApplicationResult<ConnectorTableHandle> result = new FilterApplicationResult<>(
                Mockito.mock(ConnectorTableHandle.class), Mockito.mock(ConnectorExpression.class), false);
        Mockito.when(view.getFilterResult()).thenReturn(result);
        Mockito.when(hive.applyPartitionFilterForScan(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            Assertions.assertFalse(locked.get(), "selective HMS request must be outside the internal lock");
            return declined ? Optional.empty() : Optional.of(view);
        });
        Mockito.when(hive.getNameToPartitionItemsForScan(Mockito.any())).thenAnswer(invocation -> {
            Assertions.assertFalse(locked.get(), "fallback enumeration must be outside the internal lock");
            return Optional.of(items);
        });
        try {
            // Reduced bound tree of WITH c AS (SELECT p FROM hive h WHERE p='x') SELECT c.p ... JOIN internal i.
            // The statement owns i's real planner lock scope; only its table implementation is mocked.
            LogicalFileScan original = new LogicalFileScan(StatementScopeIdGenerator.newRelationId(), hive,
                    List.of("hive", "db"), Collections.emptyList(), Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty());
            LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(
                    Set.of(new EqualTo(original.getOutput().get(0), new StringLiteral("x"))), original);
            CTEId cteId = new CTEId(1);
            LogicalCTEProducer<?> producer = new LogicalCTEProducer<>(cteId, filter);
            LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                    StatementScopeIdGenerator.newRelationId(), cteId, "c", filter);
            LogicalCTEAnchor<?, ?> anchor = new LogicalCTEAnchor<>(cteId, producer, consumer);
            statement.getTables().put(List.of("internal", "db", "i"), internal);
            statement.registerExternalTableForPreload(hive, Optional.empty(), Optional.empty(), true);
            ExternalTablePreloadInfo info = statement.getExternalTablePreloadInfo(41L).get();
            info.registerScanPredicate(original.getRelationId(),
                    new EqualTo(new UnboundSlot("p"), new StringLiteral("x")));
            statement.preloadDeferredScanPartitionViewsBeforeLock(finalizePhysical);
            Assertions.assertEquals(declined, info.hasScanPartitionView());
            statement.lock();
            Assertions.assertTrue(locked.get());
            CascadesContext cascades = CascadesContext.initContext(statement, anchor, PhysicalProperties.ANY);
            Plan inlined = new CTEInline().rewriteRoot(anchor, cascades.getCurrentJobContext());
            Assertions.assertFalse(inlined.anyMatch(p -> p instanceof LogicalCTEAnchor));
            LogicalFileScan copied = (LogicalFileScan) inlined.collect(p -> p instanceof LogicalFileScan)
                    .iterator().next();
            LogicalFilter<LogicalFileScan> copiedFilter = (LogicalFilter<LogicalFileScan>) inlined
                    .collect(p -> p instanceof LogicalFilter).iterator().next();
            Assertions.assertEquals(original.getRelationId(), copied.getPreloadRelationId());
            LogicalFileScan copyOfCopy = (LogicalFileScan) copied.accept(
                    LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());
            Assertions.assertNotEquals(copied.getRelationId(), copyOfCopy.getRelationId());
            Assertions.assertNotEquals(copied.getOutput().get(0).getExprId(),
                    copyOfCopy.getOutput().get(0).getExprId());
            // Every constructor-based rewrite must preserve the original preparation identity.
            List<LogicalFileScan> rewrites = List.of(copyOfCopy,
                    copyOfCopy.withGroupExpression(Optional.empty()),
                    (LogicalFileScan) copyOfCopy.withGroupExprLogicalPropChildren(
                            Optional.empty(), Optional.of(copyOfCopy.getLogicalProperties()), List.of()),
                    copyOfCopy.withSelectedPartitions(copyOfCopy.getSelectedPartitions()),
                    copyOfCopy.withRelationId(StatementScopeIdGenerator.newRelationId()),
                    copyOfCopy.withTableAlias("copied"),
                    copyOfCopy.withOperativeSlots(copyOfCopy.getOperativeSlots()),
                    copyOfCopy.withCachedOutput(copyOfCopy.getOutput()));
            for (LogicalFileScan rewrite : rewrites) {
                Assertions.assertEquals(original.getRelationId(), rewrite.getPreloadRelationId());
            }
            Assertions.assertNotEquals(original.getRelationId(), copied.getRelationId());
            Assertions.assertNotEquals(original.getOutput().get(0).getExprId(), copied.getOutput().get(0).getExprId());
            SelectedPartitions selected = Deencapsulation.invoke(new PruneFileScanPartition(),
                    "pruneExternalPartitions", hive, copiedFilter, copied, cascades);
            Assertions.assertEquals(Set.of("p=x"), selected.selectedPartitions.keySet());
            if (!declined) {
                Assertions.assertSame(result, selected.getConnectorFilterResult().get());
            }
            LogicalFilter<LogicalFileScan> secondFilter = new LogicalFilter<>(Set.of(
                    new EqualTo(copyOfCopy.getOutput().get(0), new StringLiteral("x"))), copyOfCopy);
            SelectedPartitions second = Deencapsulation.invoke(new PruneFileScanPartition(),
                    "pruneExternalPartitions", hive, secondFilter, copyOfCopy, cascades);
            Assertions.assertEquals(Set.of("p=x"), second.selectedPartitions.keySet());
            Assertions.assertEquals(Set.of(new EqualTo(copied.getOutput().get(0), new StringLiteral("x"))),
                    copiedFilter.getConjuncts());
            Mockito.verify(hive, Mockito.times(1)).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(hive, Mockito.times(declined ? 1 : 0)).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            try {
                statement.close();
            } finally {
                ConnectContext.remove();
                if (previous != null) {
                    previous.setThreadLocalInfo();
                }
            }
        }
    }
}
