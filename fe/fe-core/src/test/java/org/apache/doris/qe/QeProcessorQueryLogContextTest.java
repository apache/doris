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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class QeProcessorQueryLogContextTest {
    private static final TUniqueId QUERY_ID = new TUniqueId(68694, 401);
    private static final TNetworkAddress ADDRESS = new TNetworkAddress("127.0.0.1", 9050);
    private final QeProcessorImpl processor = (QeProcessorImpl) QeProcessorImpl.INSTANCE;
    private boolean savedEnabled;
    private String savedQueryId;
    private ExecutorService savedExecutor;
    private boolean registered;

    @BeforeEach
    void setUp() {
        savedEnabled = Config.sys_log_enable_query_id;
        savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        savedExecutor = Deencapsulation.getField(processor, "writeProfileExecutor");
        Config.sys_log_enable_query_id = true;
        ThreadContext.put(QueryLogContext.QUERY_ID, "report-caller");
    }

    @AfterEach
    void tearDown() {
        Deencapsulation.setField(processor, "writeProfileExecutor", savedExecutor);
        if (registered) {
            processor.unregisterQuery(QUERY_ID);
        }
        Config.sys_log_enable_query_id = savedEnabled;
        if (savedQueryId == null) {
            ThreadContext.remove(QueryLogContext.QUERY_ID);
        } else {
            ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
        }
    }

    @Test
    void statusReportRestoresCallerAfterCoordinatorFailure() throws Exception {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        processor.registerQuery(QUERY_ID, new QeProcessorImpl.QueryInfo(coordinator));
        registered = true;
        Mockito.when(coordinator.updateFragmentExecStatus(Mockito.any())).thenAnswer(invocation -> {
            Assertions.assertEquals(DebugUtil.printId(QUERY_ID), ThreadContext.get(QueryLogContext.QUERY_ID));
            throw new IllegalStateException("injected report failure");
        });
        TReportExecStatusParams params = new TReportExecStatusParams().setQueryId(QUERY_ID)
                .setFragmentInstanceId(new TUniqueId(68694, 402));
        try (TestLogAppender appender = TestLogAppender.attach(QeProcessorImpl.class)) {
            Assertions.assertEquals(TStatusCode.OK, processor.reportExecStatus(params, ADDRESS).getStatus().status_code);
            LogEvent event = events(appender).stream().filter(e -> e.getLevel() == Level.WARN).findFirst().orElseThrow();
            Assertions.assertEquals(DebugUtil.printId(QUERY_ID), event.getContextData().getValue(QueryLogContext.QUERY_ID));
            Assertions.assertFalse(event.getMessage().getFormattedMessage().contains(DebugUtil.printId(QUERY_ID)));
            Assertions.assertEquals("injected report failure", event.getThrown().getMessage());
        }
        Assertions.assertEquals("report-caller", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @Test
    void profileOnlyReportCapturesIdentityForDelayedWorkerAndRestoresOnFailure() throws Exception {
        ExecutionProfile executionProfile = Mockito.mock(ExecutionProfile.class);
        AtomicReference<Runnable> pending = new AtomicReference<>();
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        Mockito.when(executor.submit(Mockito.any(Runnable.class))).thenAnswer(invocation -> {
            pending.set(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });
        Deencapsulation.setField(processor, "writeProfileExecutor", executor);
        Mockito.when(executionProfile.updateProfile(Mockito.any(), Mockito.eq(ADDRESS), Mockito.eq(true)))
                .thenAnswer(invocation -> {
                    Assertions.assertEquals(DebugUtil.printId(QUERY_ID), ThreadContext.get(QueryLogContext.QUERY_ID));
                    throw new IllegalStateException("injected profile failure");
                });
        TQueryProfile profile = profile();
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<ProfileManager> profiles = Mockito.mockStatic(ProfileManager.class);
                TestLogAppender appender = TestLogAppender.attach(QeProcessorImpl.class)) {
            mockProfileServices(env, profiles, executionProfile);
            Assertions.assertEquals(TStatusCode.OK, processor.reportExecStatus(profileParams(profile), ADDRESS)
                    .getStatus().status_code);
            Assertions.assertEquals(DebugUtil.printId(QUERY_ID), events(appender).get(0).getContextData()
                    .getValue(QueryLogContext.QUERY_ID));
        }
        Assertions.assertEquals("report-caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        Assertions.assertNotNull(pending.get());
        // Mutating the original Thrift object after submission must not change the captured identity.
        profile.getQueryId().setLo(999);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            worker.submit(() -> {
                ThreadContext.put(QueryLogContext.QUERY_ID, "profile-worker");
                Assertions.assertThrows(IllegalStateException.class, pending.get()::run);
                Assertions.assertEquals("profile-worker", ThreadContext.get(QueryLogContext.QUERY_ID));
                ThreadContext.remove(QueryLogContext.QUERY_ID);
            }).get(10, TimeUnit.SECONDS);
        } finally {
            worker.shutdownNow();
        }
        Mockito.verify(executionProfile).updateProfile(profile, ADDRESS, true);
    }

    @Test
    void rejectedProfileTaskKeepsReportIdentityAndProtocolSuccess() {
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        Mockito.when(executor.submit(Mockito.any(Runnable.class)))
                .thenThrow(new RejectedExecutionException("injected rejection"));
        Deencapsulation.setField(processor, "writeProfileExecutor", executor);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<ProfileManager> profiles = Mockito.mockStatic(ProfileManager.class);
                TestLogAppender appender = TestLogAppender.attach(QeProcessorImpl.class)) {
            mockProfileServices(env, profiles, Mockito.mock(ExecutionProfile.class));
            Assertions.assertEquals(TStatusCode.OK, processor.reportExecStatus(profileParams(profile()), ADDRESS)
                    .getStatus().status_code);
            LogEvent event = events(appender).stream().filter(e -> e.getLevel() == Level.WARN).findFirst().orElseThrow();
            Assertions.assertTrue(event.getMessage().getFormattedMessage().contains("Failed to submit profile"));
            Assertions.assertEquals(DebugUtil.printId(QUERY_ID), event.getContextData().getValue(QueryLogContext.QUERY_ID));
            Assertions.assertFalse(event.getMessage().getFormattedMessage().contains(DebugUtil.printId(QUERY_ID)));
        }
        Assertions.assertEquals("report-caller", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @Test
    void missingProfileRetainsIdInReturnedStatusWithoutChangingCaller() {
        try (MockedStatic<ProfileManager> profiles = Mockito.mockStatic(ProfileManager.class);
                TestLogAppender appender = TestLogAppender.attach(QeProcessorImpl.class)) {
            profiles.when(ProfileManager::getInstance).thenReturn(Mockito.mock(ProfileManager.class));
            Status status = Deencapsulation.invoke(processor, "processQueryProfile", profile(), ADDRESS, true);
            Assertions.assertEquals(TStatusCode.NOT_FOUND, status.getErrorCode());
            Assertions.assertTrue(status.getErrorMsg().contains(DebugUtil.printId(QUERY_ID)));
            Assertions.assertTrue(appender.contains(Level.DEBUG, DebugUtil.printId(QUERY_ID)));
            Assertions.assertEquals("report-caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        }
    }

    @Test
    void invalidProfileReportUsesProfileIdentityInsteadOfZeroTopLevelId() {
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                TestLogAppender appender = TestLogAppender.attach(QeProcessorImpl.class)) {
            env.when(Env::getCurrentSystemInfo).thenReturn(Mockito.mock(SystemInfoService.class));
            TReportExecStatusParams params = profileParams(profile());
            Assertions.assertEquals(TStatusCode.OK, processor.reportExecStatus(params, ADDRESS).getStatus().status_code);
            params.unsetBackendId();
            Assertions.assertEquals(TStatusCode.OK, processor.reportExecStatus(params, ADDRESS).getStatus().status_code);
            List<LogEvent> warnings = events(appender).stream().filter(e -> e.getLevel() == Level.WARN)
                    .collect(java.util.stream.Collectors.toList());
            Assertions.assertEquals(2, warnings.size());
            Assertions.assertTrue(warnings.get(0).getMessage().getFormattedMessage().contains("backend 7 not found"));
            Assertions.assertTrue(warnings.get(1).getMessage().getFormattedMessage().contains("must set backendId"));
            for (LogEvent event : warnings) {
                Assertions.assertEquals(DebugUtil.printId(QUERY_ID),
                        event.getContextData().getValue(QueryLogContext.QUERY_ID));
            }
        }
        Assertions.assertEquals("report-caller", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    private void mockProfileServices(MockedStatic<Env> env, MockedStatic<ProfileManager> profiles,
            ExecutionProfile executionProfile) {
        SystemInfoService system = Mockito.mock(SystemInfoService.class);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(system.getBackend(7L)).thenReturn(backend);
        Mockito.when(backend.getHeartbeatAddress()).thenReturn(ADDRESS);
        env.when(Env::getCurrentSystemInfo).thenReturn(system);
        ProfileManager manager = Mockito.mock(ProfileManager.class);
        Mockito.when(manager.getExecutionProfile(QUERY_ID)).thenReturn(executionProfile);
        profiles.when(ProfileManager::getInstance).thenReturn(manager);
    }

    private static TQueryProfile profile() {
        return new TQueryProfile().setQueryId(QUERY_ID.deepCopy()).setFragmentIdToProfile(Collections.emptyMap());
    }

    private static TReportExecStatusParams profileParams(TQueryProfile profile) {
        return new TReportExecStatusParams().setQueryId(new TUniqueId(0, 0))
                .setQueryProfile(profile).setBackendId(7).setDone(true);
    }

    private static List<LogEvent> events(TestLogAppender appender) {
        return Deencapsulation.getField(appender, "events");
    }
}
