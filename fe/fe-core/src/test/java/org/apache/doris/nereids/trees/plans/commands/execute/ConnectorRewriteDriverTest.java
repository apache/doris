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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.RewriteCapableTransaction;
import org.apache.doris.connector.spi.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.spi.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteStatistics;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.scheduler.manager.TransientTaskManager;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards the engine-neutral parts of {@link ConnectorRewriteDriver} that are unit-testable without a live
 * cluster: the empty-plan early return (no transaction opened), what the driver reports to the connector, and
 * the connector-failure mapping. The full distributed write path (N INSERT-SELECTs against BE) is exercised at
 * the flip rehearsal.
 *
 * <p>The RESULT SHAPE is deliberately not asserted here any more: the columns belong to the connector that
 * declares the procedure, and are pinned by {@code IcebergProcedureOpsTest}. What the engine owes is asserted
 * below — it reports the right numbers and returns the connector's result untouched.</p>
 */
public class ConnectorRewriteDriverTest {

    private ConnectorRewriteDriver driverWith(ConnectorProcedureOps procedureOps, ConnectorMetadata metadata) {
        return driverWith(procedureOps, metadata, null);
    }

    private ConnectorRewriteDriver driverWith(ConnectorProcedureOps procedureOps, ConnectorMetadata metadata,
            ConnectorPredicate where) {
        return new ConnectorRewriteDriver(
                Mockito.mock(ConnectContext.class),
                Mockito.mock(ExternalTable.class),
                Mockito.mock(PluginDrivenExternalCatalog.class),
                metadata,
                procedureOps,
                Mockito.mock(ConnectorSession.class),
                Mockito.mock(ConnectorTableHandle.class),
                "rewrite_data_files",
                Collections.emptyMap(),
                Collections.emptyList(),
                where);
    }

    @Test
    public void emptyPlanReportsAllZerosWithoutOpeningTransaction() throws Exception {
        ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(Collections.emptyList());
        ConnectorProcedureResult rendered = new ConnectorProcedureResult(
                Collections.singletonList(new ConnectorColumn("c", ConnectorType.of("INT"), "", false, null)),
                Collections.singletonList(Collections.singletonList("0")));
        Mockito.when(procedureOps.buildRewriteResult(Mockito.any(), Mockito.any())).thenReturn(rendered);

        ConnectorProcedureResult result = driverWith(procedureOps, metadata).run();

        // Nothing to rewrite: the connector is still asked to render, and it is told so with four zeros.
        ArgumentCaptor<ConnectorRewriteStatistics> captor =
                ArgumentCaptor.forClass(ConnectorRewriteStatistics.class);
        Mockito.verify(procedureOps).buildRewriteResult(Mockito.eq("rewrite_data_files"), captor.capture());
        ConnectorRewriteStatistics stats = captor.getValue();
        Assertions.assertEquals(0, stats.getDataFileCount());
        Assertions.assertEquals(0, stats.getAddedDataFileCount());
        Assertions.assertEquals(0L, stats.getTotalSizeBytes());
        Assertions.assertEquals(0, stats.getDeleteFileCount());
        // The engine returns what the connector rendered, unmodified. MUTATION: any engine-side post-processing
        // of the result (re-wrapping, re-typing, substituting a default) is killed here.
        Assertions.assertSame(rendered, result);
        // MUTATION: dropping the empty-groups early return is killed — no transaction may be opened, and no
        // group work scheduled, when there is nothing to rewrite. The driver opens the txn via the per-handle
        // beginTransaction(session, handle) overload, so watch that one (the single-arg matcher would go
        // vacuous once the call site passes the resolved tableHandle).
        Mockito.verify(metadata, Mockito.never()).beginTransaction(Mockito.any(), Mockito.any());
    }

    @Test
    public void whereConditionIsThreadedToPlanRewrite() throws Exception {
        // The lowered WHERE must reach the connector's planRewrite as the 5th argument (the file-scope filter),
        // not be dropped to null. MUTATION: passing null instead of whereCondition is killed here.
        ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        Mockito.when(procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(Collections.emptyList());
        ConnectorPredicate where = new ConnectorPredicate(new ConnectorColumnRef("a", ConnectorType.of("INT")));
        // Stubbed only so run() does not return a null from the unstubbed mock; this test asserts nothing
        // about the result.
        Mockito.when(procedureOps.buildRewriteResult(Mockito.any(), Mockito.any())).thenReturn(
                new ConnectorProcedureResult(Collections.emptyList(), Collections.emptyList()));

        driverWith(procedureOps, Mockito.mock(ConnectorMetadata.class), where).run();

        ArgumentCaptor<ConnectorPredicate> captor = ArgumentCaptor.forClass(ConnectorPredicate.class);
        Mockito.verify(procedureOps).planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                captor.capture(), Mockito.any());
        Assertions.assertSame(where, captor.getValue(), "the driver must pass the lowered WHERE through verbatim");
    }

    @Test
    public void planRewriteFailureSurfacesAsUserException() {
        ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        Mockito.when(procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenThrow(new DorisConnectorException("plan boom"));

        ConnectorRewriteDriver driver = driverWith(procedureOps, Mockito.mock(ConnectorMetadata.class));
        UserException ex = Assertions.assertThrows(UserException.class, driver::run);
        Assertions.assertTrue(ex.getMessage().contains("plan boom"),
                "the connector failure text must be preserved, got: " + ex.getMessage());
    }

    @Test
    public void committedRewriteFencesBeforeResultConstruction() throws Exception {
        ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle tableHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTransaction connectorTx = Mockito.mock(ConnectorTransaction.class,
                Mockito.withSettings().extraInterfaces(RewriteCapableTransaction.class));
        RewriteCapableTransaction rewriteTx = (RewriteCapableTransaction) connectorTx;
        PluginDrivenTransactionManager txnManager = Mockito.mock(PluginDrivenTransactionManager.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        ConnectorRewriteGroup group = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://bucket/table/a.parquet"), 1, 1024L, 0);

        Mockito.when(procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(Collections.singletonList(group));
        Mockito.when(metadata.beginTransaction(session, tableHandle)).thenReturn(connectorTx);
        Mockito.when(catalog.getTransactionManager()).thenReturn(txnManager);
        Mockito.when(txnManager.begin(connectorTx)).thenReturn(7L);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(sessionVariable.getInsertTimeoutS()).thenReturn(1);
        Mockito.when(rewriteTx.getRewriteAddedDataFilesCount()).thenReturn(1);
        Mockito.when(procedureOps.buildRewriteResult(Mockito.eq("rewrite_data_files"), Mockito.any()))
                .thenThrow(new IllegalStateException("invalid committed result"));

        Env env = Mockito.mock(Env.class);
        RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
        TransientTaskManager transientTaskManager = Mockito.mock(TransientTaskManager.class);
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
        Mockito.when(env.getTransientTaskManager()).thenReturn(transientTaskManager);
        AtomicReference<ConnectorRewriteGroupTask.RewriteResultCallback> callback = new AtomicReference<>();

        ConnectorRewriteDriver driver = new ConnectorRewriteDriver(
                context, table, catalog, metadata, procedureOps, session, tableHandle,
                "rewrite_data_files", Collections.emptyMap(), Collections.emptyList(), null);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedConstruction<ConnectorRewriteGroupTask> taskConstruction = Mockito.mockConstruction(
                        ConnectorRewriteGroupTask.class, (task, constructionContext) -> {
                            callback.set((ConnectorRewriteGroupTask.RewriteResultCallback)
                                    constructionContext.arguments().get(5));
                            Mockito.when(task.getId()).thenReturn(11L);
                        })) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(transientTaskManager.addMemoryTask(Mockito.any())).thenAnswer(invocation -> {
                ConnectorRewriteGroupTask task = invocation.getArgument(0);
                callback.get().onTaskCompleted(task.getId());
                return task.getId();
            });

            Assertions.assertThrows(IllegalStateException.class, driver::run);

            Assertions.assertEquals(1, taskConstruction.constructed().size());
            InOrder order = Mockito.inOrder(txnManager, refreshManager, procedureOps);
            order.verify(txnManager).commit(7L);
            order.verify(refreshManager).refreshTableAfterExternalMutation(table);
            order.verify(procedureOps).buildRewriteResult(Mockito.eq("rewrite_data_files"), Mockito.any());
        }
    }

    @Test
    public void unionSourceFilePathsMergesAllGroupsAndDedupsByPath() {
        // STEP 3 registers the UNION of every group's source files in ONE connector call (one planFiles() scan)
        // instead of one call per group. Disjoint groups union straight; a path recurring across groups collapses
        // to a single entry, so the connector never double-registers a file to delete. MUTATION: unioning only
        // the first group (or not deduping) is killed here.
        ConnectorRewriteGroup g1 = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/a.parquet", "s3://b/t/b.parquet"), 2, 2048L, 0);
        ConnectorRewriteGroup g2 = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/c.parquet"), 1, 1024L, 0);
        // Defensive: a path shared with g1 (bin-packing keeps groups disjoint, but the union must still dedup).
        ConnectorRewriteGroup g3 = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/a.parquet", "s3://b/t/d.parquet"), 2, 2048L, 0);

        Set<String> union = ConnectorRewriteDriver.unionSourceFilePaths(Arrays.asList(g1, g2, g3));

        Assertions.assertEquals(
                ImmutableSet.of("s3://b/t/a.parquet", "s3://b/t/b.parquet", "s3://b/t/c.parquet",
                        "s3://b/t/d.parquet"),
                union, "the union must contain each distinct source path exactly once across all groups");
    }

    @Test
    public void unionSourceFilePathsSkipsEmptyGroupsAndEmptyPlan() {
        // An empty group contributes nothing; an all-empty plan unions to the empty set (the connector treats
        // that as a no-op registration — the same net state as the former loop making N early-returning calls).
        ConnectorRewriteGroup withFiles = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/a.parquet"), 1, 1024L, 0);
        ConnectorRewriteGroup empty = new ConnectorRewriteGroup(Collections.emptySet(), 0, 0L, 0);

        Assertions.assertEquals(ImmutableSet.of("s3://b/t/a.parquet"),
                ConnectorRewriteDriver.unionSourceFilePaths(Arrays.asList(withFiles, empty)),
                "an empty group must not affect the union");
        Assertions.assertTrue(
                ConnectorRewriteDriver.unionSourceFilePaths(Collections.emptyList()).isEmpty(),
                "an all-empty plan unions to the empty set");
    }
}
