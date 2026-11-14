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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards the cancellation/publication handoff of {@link ConnectorRewriteGroupTask}. The distributed write
 * path needs a live cluster, but the handoff itself is unit-testable: a queued task must report its
 * cancellation through the collector, and a cancel that lands after the executor is published must reach it.
 */
public class ConnectorRewriteGroupTaskTest {

    @Test
    public void groupAnalysisKeepsTheOperationSnapshotAndSchema() {
        ExternalTable table = Mockito.mock(ExternalTable.class,
                Mockito.withSettings().extraInterfaces(MvccTable.class));
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn("events");
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        ConnectorMvccSnapshot identity = ConnectorMvccSnapshot.builder().snapshotId(100L).schemaId(7L).build();
        SchemaCacheValue schema = new SchemaCacheValue(Collections.emptyList());
        PluginDrivenMvccSnapshot snapshot = new PluginDrivenMvccSnapshot(
                identity, Collections.emptyMap(), Collections.emptyMap(), schema);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle input = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle pinned = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.applySnapshot(session, input, identity)).thenReturn(pinned);
        ConnectorRewriteGroup group = new ConnectorRewriteGroup(ImmutableSet.of("a.parquet"), 1, 100L, 0);
        ConnectContext previous = ConnectContext.get();
        try {
            for (int i = 0; i < 2; i++) {
                ConnectorRewriteGroupTask task = new ConnectorRewriteGroupTask(group, 7L,
                        Mockito.mock(ConnectorTransaction.class), table, new ConnectContext(),
                        Optional.of(snapshot), null);
                ConnectContext context = Deencapsulation.invoke(task, "buildConnectContext");
                // This is what source/sink analysis does before consuming the context pin.
                context.getStatementContext().loadSnapshots(table, Optional.empty(), Optional.empty());
                Optional<MvccSnapshot> read = MvccUtil.getSnapshotFromContext(
                        table, Optional.empty(), Optional.empty());
                Assertions.assertSame(snapshot, read.orElseThrow(AssertionError::new));
                Assertions.assertSame(schema, ((PluginDrivenMvccSnapshot) read.get()).getPinnedSchema());
                Assertions.assertSame(pinned, PluginDrivenScanNode.applyMvccSnapshotPin(
                        metadata, session, input, read));
            }
            Mockito.verify((MvccTable) table, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
            Mockito.verify(metadata, Mockito.times(2)).applySnapshot(session, input, identity);
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    private ConnectorRewriteGroupTask newTask(
            AtomicBoolean completed,
            AtomicReference<Long> failedId,
            AtomicReference<Exception> failure) {
        ConnectorRewriteGroup group = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://bucket/table/a.parquet"), 1, 1024L, 0);
        return new ConnectorRewriteGroupTask(
                group,
                7L,
                Mockito.mock(ConnectorTransaction.class),
                Mockito.mock(ExternalTable.class),
                Mockito.mock(ConnectContext.class),
                Optional.empty(),
                new ConnectorRewriteGroupTask.RewriteResultCallback() {
                    @Override
                    public void onTaskCompleted(Long taskId) {
                        completed.set(true);
                    }

                    @Override
                    public void onTaskFailed(Long taskId, Exception error) {
                        failedId.set(taskId);
                        failure.set(error);
                    }
                });
    }

    @Test
    public void queuedCancellationIsReportedToTheCollector() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);

        // Cancelled while still queued: the scheduler may still invoke execute().
        task.cancel();
        Assertions.assertThrows(JobException.class, task::execute);

        Assertions.assertFalse(completed.get(), "a cancelled task must never report completion");
        Assertions.assertEquals(task.getId(), failedId.get(), "the collector must be told this task failed");
        Assertions.assertNotNull(failure.get());
    }

    @Test
    public void cancelAfterExecutorPublicationCancelsTheExecutor() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);

        // Simulate a running task whose executor has been published.
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Deencapsulation.setField(task, "stmtExecutor", stmtExecutor);

        task.cancel();

        Mockito.verify(stmtExecutor).cancel(Mockito.any(Status.class));
        Assertions.assertFalse(completed.get());
    }

    @Test
    public void cancelAfterFinishIsIgnored() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);
        ((AtomicBoolean) Deencapsulation.getField(task, "isFinished")).set(true);
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Deencapsulation.setField(task, "stmtExecutor", stmtExecutor);

        task.cancel();

        Mockito.verify(stmtExecutor, Mockito.never()).cancel(Mockito.any(Status.class));
    }
}
