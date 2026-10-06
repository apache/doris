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
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

/**
 * The streaming scheduler runs the task itself, not through TaskProcessor, which ends the statements of the tasks it
 * runs, and its attempts recur: what one leaves running adds up. So before() stops the plan it builds only to rewrite
 * the TVF as soon as it has it, and the task thread ends every attempt (endAttempt, from execute()'s finally): it stops
 * the split generation the attempt's planning started for a plan no coordinator dispatched.
 */
public class StreamingInsertTaskAttemptEndTest {

    private static final String SQL = "insert into t select s.k, d.v from s3(\"uri\" = \"s3://b/*.csv\") s"
            + " join ice.db.big d on s.k = d.k";

    @Test
    public void testBeforeStopsThePlanItBuildsOnlyToRewriteTheTvf() throws Exception {
        StreamingJobProperties properties = Mockito.mock(StreamingJobProperties.class);
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Mockito.when(provider.getSourceType()).thenReturn("cdc_stream");
        Offset offset = Mockito.mock(Offset.class);
        Mockito.when(provider.getNextOffset(Mockito.eq(properties), Mockito.anyMap())).thenReturn(offset);
        InsertIntoTableCommand baseCommand = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(baseCommand.getParsedPlan()).thenReturn(Optional.of(Mockito.mock(LogicalPlan.class)));
        Mockito.when(provider.rewriteTvfParams(Mockito.eq(baseCommand), Mockito.eq(offset), Mockito.anyLong()))
                .thenReturn(Mockito.mock(InsertIntoTableCommand.class));
        // The scan of the batch-mode table the job's statement joins with the TVF: it started generating its splits
        // while the plan was translated.
        ScanNode batchScan = Mockito.mock(ScanNode.class);
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.singletonList(batchScan));

        try (MockedStatic<InsertTask> insertTask = Mockito.mockStatic(InsertTask.class);
                MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                        (executor, construction) -> Mockito.when(executor.planner()).thenReturn(planner));
                MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                        (parser, construction) -> Mockito.when(parser.parseSingle(SQL)).thenReturn(baseCommand))) {
            insertTask.when(() -> InsertTask.makeConnectContext(UserIdentity.ROOT, "test_db"))
                    .thenReturn(Mockito.mock(ConnectContext.class));
            StreamingInsertTask task = new StreamingInsertTask(1L, 2L, SQL, provider, "test_db", properties,
                    Collections.emptyMap(), UserIdentity.ROOT, null);

            task.before();
        }

        // Stopped as soon as before() has the plan, rather than left generating splits nobody fetches while run()
        // plans and reads the same table again.
        InOrder inOrder = Mockito.inOrder(baseCommand, batchScan, provider);
        inOrder.verify(baseCommand).initPlan(Mockito.any(), Mockito.any(), Mockito.eq(false));
        inOrder.verify(batchScan).stopUndispatched();
        inOrder.verify(provider).rewriteTvfParams(Mockito.eq(baseCommand), Mockito.eq(offset), Mockito.anyLong());
    }

    @Test
    public void testEndingAnAttemptStopsWhatItsPlanningLeftUndispatched() {
        StreamingInsertTask task = newTask();
        SplitAssignment undispatched = Mockito.mock(SplitAssignment.class);
        planned(task, statementWith(undispatched));

        task.endAttempt();

        Mockito.verify(undispatched).stopIfNotDispatched();
        // The attempt's fields are closeOrReleaseResources()'s to drop, not the attempt's end.
        Assertions.assertNotNull(Deencapsulation.getField(task, "ctx"));
        task.endAttempt();
        Mockito.verify(undispatched, Mockito.times(1)).stopIfNotDispatched();
    }

    /**
     * STOP JOB: AbstractJob.updateJobStatus(STOPPED) only runs cancelAllTasks(false), which marks the task CANCELED;
     * StreamingInsertJob.clearRunningStreamTask runs for PAUSED alone, and execute()'s finally skips
     * closeOrReleaseResources() for a canceled attempt. The task thread still ends the attempt.
     */
    @Test
    public void testACanceledAttemptIsStillEnded() throws Exception {
        SplitAssignment undispatched = Mockito.mock(SplitAssignment.class);
        StatementContext statementContext = statementWith(undispatched);
        StreamingInsertTask task = new StreamingInsertTask(1L, 2L, SQL, s3Provider(), "test_db",
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
        Mockito.verify(undispatched).stopIfNotDispatched();
        // Kept for the control thread, which drops them itself (see execute()'s finally).
        Assertions.assertSame(statementContext.getConnectContext(), Deencapsulation.getField(task, "ctx"));
    }

    /**
     * PAUSE JOB while before() is still planning: stmtExecutor does not exist yet, so cancel(true) has nothing to wait
     * for and StreamingInsertJob.clearRunningStreamTask runs closeOrReleaseResources() on the control thread at once,
     * dropping ctx while the plan is still being translated on the task thread. The scan that starts generating its
     * splits after that is stopped all the same, by the task thread's end of the attempt. (The control thread's calls
     * are made from the task thread here: their order against the registration is what matters, not the thread.)
     */
    @Test
    public void testAPauseDuringPlanningLeavesTheEndOfTheAttemptToTheTaskThread() throws Exception {
        SplitAssignment startedBeforeThePause = Mockito.mock(SplitAssignment.class);
        SplitAssignment startedAfterThePause = Mockito.mock(SplitAssignment.class);
        StatementContext statementContext = statementWith(startedBeforeThePause);
        StreamingInsertTask task = new StreamingInsertTask(1L, 2L, SQL, s3Provider(), "test_db",
                Mockito.mock(StreamingJobProperties.class), Collections.emptyMap(), UserIdentity.ROOT, null) {
            @Override
            public void before() {
                planned(this, statementContext);
                // The control thread pauses the job.
                cancel(true);
                closeOrReleaseResources();
                Assertions.assertNull(Deencapsulation.getField(this, "ctx"));
                // Translating the plan goes on and starts another scan; before() then fails on the context the
                // control thread dropped.
                statementContext.addSplitAssignmentStartedWhilePlanning(startedAfterThePause);
                throw new NullPointerException("ctx");
            }
        };

        task.execute();

        Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
        Mockito.verify(startedBeforeThePause).stopIfNotDispatched();
        Mockito.verify(startedAfterThePause).stopIfNotDispatched();
    }

    private static StreamingInsertTask newTask() {
        return new StreamingInsertTask(1L, 2L, SQL, s3Provider(), "test_db",
                Mockito.mock(StreamingJobProperties.class), Collections.emptyMap(), UserIdentity.ROOT, null);
    }

    private static SourceOffsetProvider s3Provider() {
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Mockito.when(provider.getSourceType()).thenReturn("s3");
        return provider;
    }

    // An attempt's statement, with the split assignments its planning started that no coordinator took over.
    private static StatementContext statementWith(SplitAssignment... startedWhilePlanning) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement(SQL, 0));
        ctx.setStatementContext(statementContext);
        for (SplitAssignment splitAssignment : startedWhilePlanning) {
            statementContext.addSplitAssignmentStartedWhilePlanning(splitAssignment);
        }
        return statementContext;
    }

    // The task once before() has planned: its context and the attempt's statement in place, as before() leaves them.
    private static void planned(StreamingInsertTask task, StatementContext statementContext) {
        Deencapsulation.setField(task, "ctx", statementContext.getConnectContext());
        Deencapsulation.setField(task, "attemptStatementContext", statementContext);
    }
}
