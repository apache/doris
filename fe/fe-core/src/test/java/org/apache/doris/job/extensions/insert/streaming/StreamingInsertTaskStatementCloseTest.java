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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * The task runs on the streaming scheduler, not under TaskProcessor, so nobody else ends the
 * statement of an attempt: the task thread does, in endStatement() from execute()'s finally, for
 * every outcome. That stops the scan nodes of the plan before() built only to rewrite the TVF and
 * that no coordinator ever took - a remote Doris scan joined with the TVF keeps a Flight SQL
 * session on the other frontend until then.
 */
public class StreamingInsertTaskStatementCloseTest {

    private static SourceOffsetProvider s3Provider() {
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Mockito.when(provider.getSourceType()).thenReturn("s3");
        return provider;
    }

    // A statement with what its plan registered: the scan nodes to stop when it ends.
    private static StatementContext statementWith(ScanNode... registered) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext =
                new StatementContext(ctx, new OriginStatement("insert into t select 1", 0));
        ctx.setStatementContext(statementContext);
        for (ScanNode scanNode : registered) {
            statementContext.stopScanNodeAtClose(scanNode);
        }
        return statementContext;
    }

    // The task once before() has planned: its context and statement in place, as before() leaves them.
    private static void planned(StreamingInsertTask task, StatementContext statementContext) {
        Deencapsulation.setField(task, "ctx", statementContext.getConnectContext());
        Deencapsulation.setField(task, "attemptStatement", statementContext);
    }

    @Test
    public void testEndingAnAttemptStopsWhatItsPlanRegistered() {
        StreamingInsertTask task = new StreamingInsertTask(1L, 2L, "insert into t select 1", s3Provider(), "test_db",
                Mockito.mock(StreamingJobProperties.class), Collections.emptyMap(), UserIdentity.ROOT, null);
        ScanNode scanNode = Mockito.mock(ScanNode.class);
        planned(task, statementWith(scanNode));

        task.endStatement();

        Mockito.verify(scanNode).stop();
        Assertions.assertNull(Deencapsulation.getField(task, "attemptStatement"));
        // The attempt's fields are closeOrReleaseResources()'s to drop, not the statement's.
        Assertions.assertNotNull(Deencapsulation.getField(task, "ctx"));
        task.endStatement();
        Mockito.verify(scanNode, Mockito.times(1)).stop();
    }

    /**
     * STOP JOB: AbstractJob.updateJobStatus(STOPPED) only runs cancelAllTasks(false), which marks the
     * task CANCELED; StreamingInsertJob.clearRunningStreamTask runs for PAUSED alone, and execute()'s
     * finally skips closeOrReleaseResources() for a canceled attempt. The task thread still ends the
     * attempt's statement.
     */
    @Test
    public void testACanceledAttemptStillEndsItsStatement() throws Exception {
        ScanNode scanNode = Mockito.mock(ScanNode.class);
        StatementContext statementContext = statementWith(scanNode);
        StreamingInsertTask task = new StreamingInsertTask(1L, 2L, "insert into t select 1", s3Provider(), "test_db",
                Mockito.mock(StreamingJobProperties.class), Collections.emptyMap(), UserIdentity.ROOT, null) {
            @Override
            public void before() {
                planned(this, statementContext);
                cancel(false);
            }

            @Override
            public void run() {
                Assertions.assertTrue(getIsCanceled().get());
            }

            @Override
            public boolean onSuccess() {
                return false;
            }
        };

        task.execute();

        Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
        Mockito.verify(scanNode).stop();
        // Kept for the control thread, which drops them itself (see execute()'s finally).
        Assertions.assertSame(statementContext.getConnectContext(), Deencapsulation.getField(task, "ctx"));
    }

    /**
     * PAUSE JOB while before() is still planning: stmtExecutor does not exist yet, so cancel(true)
     * has nothing to wait for and StreamingInsertJob.clearRunningStreamTask runs
     * closeOrReleaseResources() on the control thread at once. It must not end the statement - the
     * plan is still running on the task thread, holding the planner's table locks - and the remote
     * Doris scan that registers after it is stopped by the task thread's final close, along with
     * the one registered before. (The control thread's calls are made from the task thread here:
     * their order against the registration is what matters, not the thread.)
     */
    @Test
    public void testAPauseDuringPlanningLeavesTheFinalCloseToTheTaskThread() throws Exception {
        ScanNode registeredBeforeThePause = Mockito.mock(ScanNode.class);
        ScanNode registeredAfterThePause = Mockito.mock(ScanNode.class);
        StatementContext statementContext = statementWith(registeredBeforeThePause);
        StreamingInsertTask task = new StreamingInsertTask(1L, 2L, "insert into t select 1", s3Provider(), "test_db",
                Mockito.mock(StreamingJobProperties.class), Collections.emptyMap(), UserIdentity.ROOT, null) {
            @Override
            public void before() {
                planned(this, statementContext);
                // The control thread pauses the job.
                cancel(true);
                closeOrReleaseResources();
                Mockito.verify(registeredBeforeThePause, Mockito.never()).stop();
                Assertions.assertNull(Deencapsulation.getField(this, "ctx"));
                // Planning goes on and registers the remote Doris scan; before() then fails on the
                // context the control thread dropped.
                statementContext.stopScanNodeAtClose(registeredAfterThePause);
                throw new NullPointerException("ctx");
            }
        };

        task.execute();

        Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
        Mockito.verify(registeredBeforeThePause).stop();
        Mockito.verify(registeredAfterThePause).stop();
        Assertions.assertNull(Deencapsulation.getField(task, "attemptStatement"));
    }
}
