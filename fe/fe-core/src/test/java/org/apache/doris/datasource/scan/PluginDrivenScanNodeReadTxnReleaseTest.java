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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards {@link PluginDrivenScanNode#buildReadTransactionReleaseCallback}, the query-finish callback that
 * releases a connector's per-query read transaction. Hive full-ACID / insert-only reads open a metastore read
 * transaction + shared read lock during {@code planScan}; the engine registers this callback in
 * {@code getSplits} to commit it (releasing the lock) when the query finishes. Without the release the shared
 * read lock leaks for the metastore's lifetime.
 *
 * <p><b>Why this matters:</b> the callback runs on the StmtExecutor thread at query finish, whose TCCL is the
 * fe-core app loader. {@code releaseReadTransaction -> txn.commit -> hmsClient.commitTxn} resolves
 * metastore/thrift classes by name via the TCCL, so the callback MUST pin the provider's plugin classloader for
 * the duration of the release, else it split-brains against the app loader's duplicate copies (ClassCast /
 * NoClassDef at commit). Each test kills a mutation: (1) dropping the release call / wrong queryId, (2) dropping
 * the TCCL pin, (3) leaking the pin.</p>
 */
public class PluginDrivenScanNodeReadTxnReleaseTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @ResourceLock("global")
    public void outerReplanReplacesConnectorScopeWhileOldCleanupOwnsItsResources(boolean holdOldProducer)
            throws Exception {
        String oldCloudId = Config.cloud_unique_id;
        int oldRetryTime = Config.max_query_retry_time;
        ConnectContext context = new ConnectContext();
        StmtExecutor executor = Mockito.spy(new StmtExecutor(context, "select 1"));
        List<ConnectorStatementScopeImpl> scopes = new ArrayList<>();
        CountDownLatch allMetadataClosed = new CountDownLatch(2);
        CompletableFuture<Void> oldProducerTermination = new CompletableFuture<>();
        Mockito.doAnswer(call -> {
            TUniqueId queryId = call.getArgument(0);
            context.setQueryId(queryId);
            ConnectorStatementScopeImpl scope = (ConnectorStatementScopeImpl)
                    context.getStatementContext().getOrCreateConnectorStatementScope();
            if (!scopes.isEmpty()) {
                Assertions.assertNotSame(scopes.get(0), scope);
                Assertions.assertEquals(holdOldProducer ? 2 : 1, allMetadataClosed.getCount());
            }
            scopes.add(scope);
            CountDownLatch metadataClosed = new CountDownLatch(1);
            CountDownLatch transactionReleased = new CountDownLatch(1);
            scope.computeIfAbsent("metadata", () -> (AutoCloseable) () -> {
                metadataClosed.countDown();
                allMetadataClosed.countDown();
            });
            ConnectorSession session = Mockito.mock(ConnectorSession.class);
            Mockito.when(session.getQueryId()).thenReturn(DebugUtil.printId(queryId));
            Mockito.when(session.getStatementScope()).thenReturn(scope);
            ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
            Mockito.doAnswer(release -> {
                transactionReleased.countDown();
                return null;
            }).when(provider).releaseReadTransaction(DebugUtil.printId(queryId));
            PluginDrivenScanNode.QueryFinishCleanup cleanup = PluginDrivenScanNode.registerQueryFinishCleanup(session);
            cleanup.bind(provider);
            if (scopes.size() == 1 && holdOldProducer) {
                SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
                Mockito.when(assignment.getTerminationFuture()).thenReturn(oldProducerTermination);
                cleanup.waitFor(assignment);
            }
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            if (scopes.size() == 1 && holdOldProducer) {
                Assertions.assertEquals(1, transactionReleased.getCount());
                Assertions.assertEquals(1, metadataClosed.getCount());
            } else {
                Assertions.assertTrue(transactionReleased.await(5, TimeUnit.SECONDS));
                Assertions.assertTrue(metadataClosed.await(5, TimeUnit.SECONDS));
            }
            if (scopes.size() == 1) {
                throw new UserException(SystemInfoService.ERROR_E230);
            }
            return null;
        }).when(executor).execute(Mockito.any(TUniqueId.class));
        try {
            Config.cloud_unique_id = "connector-scope-replan-test";
            Config.max_query_retry_time = 1;
            executor.queryRetry(new TUniqueId(37, 41));
            Assertions.assertEquals(2, scopes.size());
            oldProducerTermination.complete(null);
            Assertions.assertTrue(allMetadataClosed.await(5, TimeUnit.SECONDS));
        } finally {
            oldProducerTermination.complete(null);
            QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
            context.getStatementContext().resetConnectorStatementScope();
            Config.cloud_unique_id = oldCloudId;
            Config.max_query_retry_time = oldRetryTime;
        }
    }

    @Test
    public void callbackReleasesReadTransactionForTheQuery() {
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);

        Runnable callback = PluginDrivenScanNode.buildReadTransactionReleaseCallback(provider, "query-42");
        // The transaction is released only when the query finishes: merely building the callback must not
        // release it yet (a mutation that releases eagerly would leak nothing but breaks the deferred contract).
        Mockito.verifyNoInteractions(provider);

        callback.run();
        // MUTATION: dropping the releaseReadTransaction call, or passing a different queryId than the one the
        // txn was registered under (so deregister would miss it), is killed here.
        Mockito.verify(provider).releaseReadTransaction("query-42");
    }

    @Test
    public void callbackPinsProviderClassLoaderDuringReleaseAndRestoresAfter() throws Exception {
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        ClassLoader providerLoader = provider.getClass().getClassLoader();

        AtomicReference<ClassLoader> tcclDuringRelease = new AtomicReference<>();
        Mockito.doAnswer(inv -> {
            tcclDuringRelease.set(Thread.currentThread().getContextClassLoader());
            return null;
        }).when(provider).releaseReadTransaction(Mockito.anyString());

        Thread current = Thread.currentThread();
        ClassLoader outer = current.getContextClassLoader();
        // A distinct sentinel TCCL (never equal to the provider's mock loader), so a "no pin" mutation — which
        // would leave the sentinel in place during the release — is caught by the assertSame below.
        try (URLClassLoader sentinel = new URLClassLoader(new URL[0], null)) {
            current.setContextClassLoader(sentinel);

            PluginDrivenScanNode.buildReadTransactionReleaseCallback(provider, "q").run();

            // MUTATION: dropping the onPluginClassLoader pin is killed — during the release the TCCL must be the
            // provider's (plugin) classloader, not the caller's app/sentinel loader.
            Assertions.assertSame(providerLoader, tcclDuringRelease.get(),
                    "the release must run with the TCCL pinned to the provider's plugin classloader");
            // MUTATION: leaking the pin (not restoring in a finally) is killed — the caller's TCCL must be back.
            Assertions.assertSame(sentinel, current.getContextClassLoader(),
                    "the TCCL must be restored to the caller's after the release");
        } finally {
            current.setContextClassLoader(outer);
        }
    }

    @Test
    public void registrationReleasesTransactionBeforeClosingStatementScope() {
        TUniqueId queryId = new TUniqueId(17L, 29L);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorStatementScope statementScope = Mockito.mock(ConnectorStatementScope.class);
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(session.getQueryId()).thenReturn(DebugUtil.printId(queryId));
        Mockito.when(session.getStatementScope()).thenReturn(statementScope);

        PluginDrivenScanNode.QueryFinishCleanup cleanup =
                PluginDrivenScanNode.registerQueryFinishCleanup(session);
        cleanup.bind(provider);
        Mockito.verifyNoInteractions(statementScope);
        Mockito.verifyNoInteractions(provider);

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        InOrder cleanupOrder = Mockito.inOrder(provider, statementScope);
        cleanupOrder.verify(provider).releaseReadTransaction(DebugUtil.printId(queryId));
        cleanupOrder.verify(statementScope).closeAll();
    }

    @Test
    public void queryFinishWaitsForProducerClosureBeforeReleasingTransactionsAndMetadata() throws Exception {
        TUniqueId queryId = new TUniqueId(17L, 30L);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(session.getQueryId()).thenReturn(DebugUtil.printId(queryId));
        Mockito.when(session.getStatementScope()).thenReturn(scope);
        AtomicInteger step = new AtomicInteger();
        CountDownLatch metadataClosed = new CountDownLatch(1);
        scope.computeIfAbsent("metadata", () -> (AutoCloseable) () -> {
            Assertions.assertEquals(1, step.getAndIncrement());
            metadataClosed.countDown();
        });
        Mockito.doAnswer(call -> {
            Assertions.assertEquals(0, step.getAndIncrement());
            return null;
        }).when(provider).releaseReadTransaction(DebugUtil.printId(queryId));
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        CompletableFuture<Void> terminated = new CompletableFuture<>();
        Mockito.when(assignment.getTerminationFuture()).thenReturn(terminated);
        PluginDrivenScanNode.QueryFinishCleanup cleanup = PluginDrivenScanNode.registerQueryFinishCleanup(session);
        cleanup.bind(provider);
        cleanup.waitFor(assignment);
        CompletableFuture<Void> otherScan = new CompletableFuture<>();
        scope.registerSplitTask(otherScan);
        Runnable releaseOther = scope.retain();
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        scope.closeAll();
        Assertions.assertEquals(0, step.get());
        Mockito.verifyNoInteractions(provider);
        terminated.complete(null);
        Assertions.assertEquals(0, step.get(), "a shared read transaction still belongs to the other scan");
        releaseOther.run();
        otherScan.complete(null);
        Assertions.assertTrue(metadataClosed.await(5, TimeUnit.SECONDS));
        Assertions.assertEquals(2, step.get());
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        Mockito.verify(provider).releaseReadTransaction(DebugUtil.printId(queryId));
    }

    @Test
    public void earlyBatchSetupFailureStillClosesStatementScopeAtQueryFinish() throws Exception {
        assertEarlyBatchSetupFailureClosesStatementScope(false, new TUniqueId(31L, 41L));
        assertEarlyBatchSetupFailureClosesStatementScope(true, new TUniqueId(31L, 42L));
    }

    private static void assertEarlyBatchSetupFailureClosesStatementScope(
            boolean streamingBatch, TUniqueId queryId) throws Exception {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorStatementScope statementScope = Mockito.mock(ConnectorStatementScope.class);
        Mockito.when(session.getQueryId()).thenReturn(DebugUtil.printId(queryId));
        Mockito.when(session.getStatementScope()).thenReturn(statementScope);

        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        SplitAssignment splitAssignment = Mockito.mock(SplitAssignment.class);
        Deencapsulation.setField(node, "connectorSession", session);
        Deencapsulation.setField(node, "splitAssignment", splitAssignment);
        Deencapsulation.setField(node, "streamingBatch", streamingBatch);
        UserException setupFailure = new UserException("early setup failure");
        Mockito.doThrow(setupFailure).when(node).checkSysTableScanConstraints();

        try {
            node.startSplit(1);
            Mockito.verify(splitAssignment).setException(setupFailure);
            Mockito.verifyNoInteractions(statementScope);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
        Mockito.verify(statementScope).closeAll();
    }
}
