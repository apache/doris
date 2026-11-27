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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Status;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards the cancellation/publication handoff of {@link ConnectorRewriteGroupTask}. The distributed write
 * path needs a live cluster, but the handoff itself is unit-testable: a queued task must report its
 * cancellation through the collector, and a cancel that lands after the executor is published must reach it.
 */
public class ConnectorRewriteGroupTaskTest {

    private ConnectorRewriteGroupTask taskWithSettings(ConnectContext parent, Map<String, String> settings) {
        return new ConnectorRewriteGroupTask(new ConnectorRewriteGroup(
                ImmutableSet.of("s3://bucket/table/a.parquet"), 1, 1024L, 0, settings),
                7L, Mockito.mock(ConnectorTransaction.class), Mockito.mock(ExternalTable.class), parent, null);
    }

    private ConnectContext parentWithTarget(long target) {
        ConnectContext parent = Mockito.mock(ConnectContext.class);
        SessionVariable variables = new SessionVariable();
        variables.setIcebergWriteTargetFileSizeBytes(target);
        Mockito.when(parent.getSessionVariable()).thenReturn(variables);
        Mockito.when(parent.getDatabase()).thenReturn("rewrite_db");
        return parent;
    }

    private Env testEnv() {
        Env currentEnv = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(catalog.getDb("rewrite_db")).thenReturn(Optional.empty());
        Mockito.when(catalogs.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenReturn(catalog);
        Mockito.when(currentEnv.getCatalogMgr()).thenReturn(catalogs);
        Mockito.when(currentEnv.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(currentEnv.getSqlCacheManager()).thenReturn(new NereidsSqlCacheManager());
        return currentEnv;
    }

    private void installEnv(MockedStatic<Env> env) {
        Env currentEnv = testEnv();
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getMinPipelineExecutorSize(Mockito.anyString())).thenReturn(2);
        env.when(Env::getCurrentEnv).thenReturn(currentEnv);
        env.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
    }

    @Test
    public void writerTargetReachesQueryOptionsWithoutChangingTheCaller() throws Exception {
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            installEnv(env);
            for (long parentTarget : new long[] {0L, 268435456L}) {
                ConnectContext parent = parentWithTarget(parentTarget);
                for (long target : new long[] {134217728L, 536870912L, 5368709120L}) {
                    ConnectorRewriteGroupTask task = taskWithSettings(parent, Collections.singletonMap(
                            SessionVariable.ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES, Long.toString(target)));
                    ConnectContext child = task.buildConnectContext();
                    TQueryOptions options = child.getSessionVariable().toThrift();

                    Assertions.assertNotSame(parent.getSessionVariable(), child.getSessionVariable());
                    Assertions.assertTrue(options.isSetIcebergWriteTargetFileSizeBytes());
                    Assertions.assertEquals(target, options.getIcebergWriteTargetFileSizeBytes());
                    Assertions.assertEquals(parentTarget,
                            parent.getSessionVariable().getIcebergWriteTargetFileSizeBytes());
                }
            }
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void groupsWithoutOverridesKeepTheInheritedWriterTarget() throws Exception {
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            installEnv(env);
            ConnectContext parent = parentWithTarget(268435456L);
            ConnectContext child = taskWithSettings(parent, Collections.emptyMap()).buildConnectContext();
            Assertions.assertEquals(268435456L,
                    child.getSessionVariable().toThrift().getIcebergWriteTargetFileSizeBytes());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void invalidWriteSettingsFailWithoutChangingTheCaller() {
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            installEnv(env);
            ConnectContext parent = parentWithTarget(268435456L);
            ConnectorRewriteGroupTask task = taskWithSettings(parent, Collections.singletonMap(
                    SessionVariable.ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES, "not-a-number"));

            Assertions.assertThrows(DdlException.class, task::buildConnectContext);
            Assertions.assertEquals(268435456L, parent.getSessionVariable().getIcebergWriteTargetFileSizeBytes());
        }
    }

    @Test
    public void concurrentGroupsKeepIndependentWriterTargets() throws Exception {
        ConnectContext parent = parentWithTarget(536870912L);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch contextsBuilt = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<Long> first = workers.submit(() -> concurrentTarget(parent, 134217728L, contextsBuilt, release));
            Future<Long> second = workers.submit(() -> concurrentTarget(parent, 268435456L, contextsBuilt, release));
            Assertions.assertTrue(contextsBuilt.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(536870912L, parent.getSessionVariable().getIcebergWriteTargetFileSizeBytes());
            release.countDown();
            Assertions.assertEquals(134217728L, first.get(10, TimeUnit.SECONDS).longValue());
            Assertions.assertEquals(268435456L, second.get(10, TimeUnit.SECONDS).longValue());
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    private long concurrentTarget(ConnectContext parent, long target, CountDownLatch contextsBuilt,
            CountDownLatch release) throws Exception {
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            installEnv(env);
            ConnectContext child = taskWithSettings(parent, Collections.singletonMap(
                    SessionVariable.ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES, Long.toString(target)))
                    .buildConnectContext();
            contextsBuilt.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            Assertions.assertSame(child, ConnectContext.get());
            return child.getSessionVariable().toThrift().getIcebergWriteTargetFileSizeBytes();
        } finally {
            ConnectContext.remove();
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
