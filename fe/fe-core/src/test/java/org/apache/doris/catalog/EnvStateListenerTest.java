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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.statistics.analysis.AnalysisManager;
import org.apache.doris.statistics.analysis.FollowerColumnSender;
import org.apache.doris.statistics.cache.StatisticsCache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Timeout(30)
public class EnvStateListenerTest {
    private Env env;

    @BeforeEach
    public void setUp() {
        // Cold Env initialization and Mockito instrumentation can exceed the test timeout in CI.
        // Keep fixture creation outside the timeout that guards the state listener transitions.
        env = Mockito.spy(new Env(false));
    }

    @Test
    public void testInterruptedNonMasterTransitionDoesNotCommitFeType() throws Exception {
        setField(env, "replayer", Mockito.mock(Daemon.class));

        CountDownLatch firstTransitionInterrupted = new CountDownLatch(1);
        CountDownLatch repeatedTransitionAttempted = new CountDownLatch(1);
        AtomicInteger transitionAttempts = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            if (transitionAttempts.incrementAndGet() == 1) {
                firstTransitionInterrupted.countDown();
            } else {
                // Let runOneCycle return after the repeated FOLLOWER transition is interrupted. Without this
                // event, the state listener would correctly keep waiting for another state after the assertion.
                env.notifyNewFETypeTransfer(FrontendNodeType.INIT);
                repeatedTransitionAttempted.countDown();
            }
            return false;
        }).when(env).postProcessAfterMetadataReplayed(true);

        env.startStateListener();
        Daemon stateListener = (Daemon) getField(env, "listener");
        try {
            env.notifyNewFETypeTransfer(FrontendNodeType.FOLLOWER);
            Assertions.assertTrue(firstTransitionInterrupted.await(5, TimeUnit.SECONDS));

            // The first transition was interrupted before non-master initialization completed. A repeated
            // FOLLOWER event must retry the transition instead of being discarded as an already completed state.
            env.notifyNewFETypeTransfer(FrontendNodeType.FOLLOWER);
            Assertions.assertTrue(repeatedTransitionAttempted.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(FrontendNodeType.INIT, env.getFeType());
        } finally {
            stateListener.exit();
            env.notifyNewFETypeTransfer(env.getFeType());
            stateListener.join(5000);
            Assertions.assertFalse(stateListener.isAlive());
        }
    }

    @ParameterizedTest
    @CsvSource({"INIT, FOLLOWER", "INIT, OBSERVER", "UNKNOWN, FOLLOWER", "UNKNOWN, OBSERVER"})
    public void testUnknownInterruptionKeepsStartupGateClosedUntilRetryCompletes(
            FrontendNodeType initialType, FrontendNodeType targetType) throws Exception {
        setField(env, "feType", initialType);
        Mockito.doReturn(false).when(env).replayJournal(-1);
        env.createReplayer();
        Daemon replayer = (Daemon) getField(env, "replayer");
        Daemon stateListener = env.createStateListener();

        Auth auth = Mockito.mock(Auth.class);
        setField(env, "auth", auth);
        CatalogMgr catalogMgr = Mockito.spy(env.getCatalogMgr());
        setField(env, "catalogMgr", catalogMgr);
        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        StatisticsCache statisticsCache = Mockito.mock(StatisticsCache.class);
        Mockito.when(analysisManager.getStatisticsCache()).thenReturn(statisticsCache);
        setField(env, "analysisManager", analysisManager);

        List<Boolean> servingDuringInitialization = new ArrayList<>();
        Answer<Void> recordServingState = invocation -> {
            servingDuringInitialization.add(env.isReady() || env.canRead());
            return null;
        };
        Mockito.doAnswer(recordServingState).when(env).startNonMasterDaemonThreads();
        Mockito.doAnswer(recordServingState).when(statisticsCache).preHeat();

        AtomicInteger transitionAttempts = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            if (transitionAttempts.incrementAndGet() == 1) {
                boolean completed = (boolean) invocation.callRealMethod();
                // Run the real replayer readiness update after UNKNOWN interrupts the wait, before
                // the listener handles UNKNOWN. This deterministically reproduces the unsafe interleaving.
                replayFreshMetadata(env, replayer);
                return completed;
            }
            // transferToNonMaster resets metadata readiness on every attempt. Let the replayer
            // catch up again before the retry executes the real metadata post-processing.
            replayFreshMetadata(env, replayer);
            return invocation.callRealMethod();
        }).when(env).postProcessAfterMetadataReplayed(true);

        try (MockedStatic<MetricRepo> metrics = Mockito.mockStatic(MetricRepo.class);
                MockedConstruction<FollowerColumnSender> senders = Mockito.mockConstruction(
                        FollowerColumnSender.class,
                        (sender, context) -> Mockito.doAnswer(recordServingState).when(sender).start())) {
            metrics.when(MetricRepo::init).thenAnswer(recordServingState);

            env.notifyNewFETypeTransfer(targetType);
            env.notifyNewFETypeTransfer(FrontendNodeType.UNKNOWN);
            // An equal event ends runOneCycle. For INIT, first commit UNKNOWN; for an initial
            // UNKNOWN, the interruption event itself is already equal to the retained FE type.
            if (initialType == FrontendNodeType.INIT) {
                env.notifyNewFETypeTransfer(FrontendNodeType.UNKNOWN);
            }
            runOneCycle(stateListener);

            Assertions.assertEquals(FrontendNodeType.UNKNOWN, env.getFeType());
            Assertions.assertEquals(1, transitionAttempts.get());
            Assertions.assertTrue(((AtomicBoolean) getField(env, "isReady")).get());
            Assertions.assertTrue(((AtomicBoolean) getField(env, "canRead")).get());
            Assertions.assertFalse(env.isReady());
            Assertions.assertFalse(env.canRead());
            Mockito.verify(auth, Mockito.never()).rectifyPrivs();
            Mockito.verify(catalogMgr, Mockito.never()).registerCatalogRefreshListener(env);
            Mockito.verify(env, Mockito.never()).startNonMasterDaemonThreads();
            Mockito.verify(statisticsCache, Mockito.never()).preHeat();
            metrics.verify(MetricRepo::init, Mockito.never());
            Assertions.assertTrue(senders.constructed().isEmpty());

            replayFreshMetadata(env, replayer);
            Assertions.assertFalse(env.isReady());
            Assertions.assertFalse(env.canRead());

            env.notifyNewFETypeTransfer(targetType);
            env.notifyNewFETypeTransfer(targetType);
            runOneCycle(stateListener);

            Assertions.assertEquals(targetType, env.getFeType());
            Assertions.assertEquals(2, transitionAttempts.get());
            Mockito.verify(auth).rectifyPrivs();
            Mockito.verify(catalogMgr).registerCatalogRefreshListener(env);
            Mockito.verify(env).startNonMasterDaemonThreads();
            metrics.verify(MetricRepo::init);
            Mockito.verify(statisticsCache).preHeat();
            Assertions.assertEquals(1, senders.constructed().size());
            Mockito.verify(senders.constructed().get(0)).start();
            Assertions.assertEquals(List.of(false, false, false, false), servingDuringInitialization);
            Assertions.assertTrue(env.isReady());
            Assertions.assertTrue(env.canRead());
            env.waitForReady();

            // Once startup has completed, UNKNOWN retains the existing read policy until metadata expires.
            env.notifyNewFETypeTransfer(FrontendNodeType.UNKNOWN);
            env.notifyNewFETypeTransfer(FrontendNodeType.UNKNOWN);
            runOneCycle(stateListener);
            Assertions.assertFalse(env.isReady());
            Assertions.assertTrue(env.canRead());
            replayFreshMetadata(env, replayer);
            Assertions.assertTrue(env.isReady());
            Assertions.assertTrue(env.canRead());
            env.setSynchronizedTime(0);
            runOneCycle(replayer);
            Assertions.assertFalse(env.isReady());
            Assertions.assertFalse(env.canRead());
        }
    }

    @Test
    public void testIgnoreMetaCheckDoesNotBypassStartupGate() throws Exception {
        Mockito.doReturn(false).when(env).replayJournal(-1);
        env.createReplayer();
        boolean originalIgnoreMetaCheck = Config.ignore_meta_check;
        try {
            Config.ignore_meta_check = true;
            runOneCycle((Daemon) getField(env, "replayer"));
            Assertions.assertTrue(((AtomicBoolean) getField(env, "canRead")).get());
            Assertions.assertFalse(env.isReady());
            Assertions.assertFalse(env.canRead());
        } finally {
            Config.ignore_meta_check = originalIgnoreMetaCheck;
        }
    }

    private static void replayFreshMetadata(Env env, Daemon replayer) throws ReflectiveOperationException {
        env.setSynchronizedTime(System.currentTimeMillis());
        runOneCycle(replayer);
    }

    private static void runOneCycle(Daemon daemon) throws ReflectiveOperationException {
        Method method = daemon.getClass().getDeclaredMethod("runOneCycle");
        method.setAccessible(true);
        method.invoke(daemon);
    }

    private static Object getField(Env env, String fieldName) throws ReflectiveOperationException {
        Field field = Env.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(env);
    }

    private static void setField(Env env, String fieldName, Object value) throws ReflectiveOperationException {
        Field field = Env.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(env, value);
    }
}
