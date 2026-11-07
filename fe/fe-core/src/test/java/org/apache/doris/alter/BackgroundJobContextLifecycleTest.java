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

package org.apache.doris.alter;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.GlobalTransactionMgr;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataOutput;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class BackgroundJobContextLifecycleTest {
    private enum Exit {
        SUCCESS, FAILURE, EARLY_RETURN
    }

    private boolean savedUnitTest;
    private MockedStatic<Env> environment;
    private ConnectContext parent;

    @BeforeEach
    public void setUp() {
        savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        environment = Mockito.mockStatic(Env.class);
        environment.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
        environment.when(Env::getCurrentGlobalTransactionMgr).thenReturn(Mockito.mock(GlobalTransactionMgr.class));
        parent = new ConnectContext();
        parent.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        parent.cleanup();
        ConnectContext.remove();
        environment.close();
        FeConstants.runningUnitTest = savedUnitTest;
    }

    @ParameterizedTest
    @EnumSource(Exit.class)
    public void alterJobClosesCloudContextAndRestoresCaller(Exit exit) {
        TestAlterJob job = new TestAlterJob(exit);
        AtomicReference<ConnectContext> execution = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        try (MockedStatic<DebugPointUtil> debugPoints = Mockito.mockStatic(DebugPointUtil.class)) {
            debugPoints.when(() -> DebugPointUtil.isEnable("FE.STOP_ALTER_JOB_RUN")).thenAnswer(invocation -> {
                execution.set(ConnectContext.get());
                execution.get().setStatementContext(statement);
                return exit == Exit.EARLY_RETURN;
            });
            job.run();
        }
        Assertions.assertEquals(exit == Exit.FAILURE, job.cancelled);
        assertContextReleased(execution.get(), statement);
    }

    @ParameterizedTest
    @EnumSource(Exit.class)
    public void indexJobClosesCloudContextAndRestoresCaller(Exit exit) throws Exception {
        IndexChangeJob job = Mockito.spy(new IndexChangeJob(1, 2, 3, "table", Long.MAX_VALUE, 0,
                Collections.emptyList(), Collections.emptyList()));
        job.setCloudClusterName("cluster");
        AtomicReference<ConnectContext> execution = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        Mockito.doAnswer(invocation -> {
            execution.set(ConnectContext.get());
            execution.get().setStatementContext(statement);
            return exit == Exit.EARLY_RETURN;
        }).when(job).isTimeout();
        Mockito.doReturn(true).when(job).cancelImpl(Mockito.anyString());
        if (exit == Exit.FAILURE) {
            Mockito.doThrow(new AlterCancelException("job failed")).when(job).runWaitingTxnJob();
        } else {
            Mockito.doNothing().when(job).runWaitingTxnJob();
        }
        job.run();
        Mockito.verify(job, Mockito.times(exit == Exit.SUCCESS ? 0 : 1)).cancelImpl(Mockito.anyString());
        assertContextReleased(execution.get(), statement);
    }

    private void assertContextReleased(ConnectContext execution, StatementContext statement) {
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertFalse(parent.isClosingOrClosed());
        Assertions.assertNotSame(parent, execution);
        Assertions.assertTrue(execution.isClosingOrClosed());
        Assertions.assertNull(execution.getStatementContext());
        Mockito.verify(statement).close();
    }

    private static class TestAlterJob extends AlterJobV2 {
        private final Exit exit;
        private boolean cancelled;

        private TestAlterJob(Exit exit) {
            super(JobType.SCHEMA_CHANGE);
            this.exit = exit;
            jobState = JobState.PENDING;
            timeoutMs = Long.MAX_VALUE;
            createTimeMs = System.currentTimeMillis();
            cloudClusterName = "cluster";
            userIdentity = UserIdentity.ROOT;
        }

        @Override
        protected void stateWait(String name) {
        }

        @Override
        protected void runPendingJob() {
            if (exit == Exit.FAILURE) {
                throw new IllegalStateException("job failed");
            }
        }

        @Override
        protected void runWaitingTxnJob() {
        }

        @Override
        protected void runRunningJob() {
        }

        @Override
        protected boolean cancelImpl(String errMsg) {
            cancelled = true;
            return true;
        }

        @Override
        protected void getInfo(List<List<Comparable>> infos) {
        }

        @Override
        public void replay(AlterJobV2 replayedJob) {
        }

        @Override
        public void write(DataOutput output) {
        }
    }
}
