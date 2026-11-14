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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RoutineLoadTaskSchedulerTest {

    private RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
    private Env env = Mockito.mock(Env.class);
    private AgentTaskExecutor agentTaskExecutor = Mockito.mock(AgentTaskExecutor.class);
    private MockedStatic<Env> envStatic;

    @BeforeEach
    public void setUp() {
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    public void tearDown() {
        envStatic.close();
    }

    @Test
    public void testRunOneCycle() throws LoadException,
            MetaNotFoundException, AnalysisException, LabelAlreadyUsedException, BeginTransactionException {
        KafkaRoutineLoadJob kafkaRoutineLoadJob1 = Mockito.mock(KafkaRoutineLoadJob.class);
        KafkaRoutineLoadJob routineLoadJob = Mockito.mock(KafkaRoutineLoadJob.class);
        Mockito.mock(RoutineLoadDesc.class);
        Mockito.mock(GlobalTransactionMgr.class);
        Mockito.mock(BackendService.Client.class);

        try (MockedStatic<ClientPool> clientPoolStatic = Mockito.mockStatic(ClientPool.class)) {
            long beId = 100L;

            ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
            partitionIdToOffset.put(1, 100L);
            partitionIdToOffset.put(2, 200L);
            KafkaProgress kafkaProgress = new KafkaProgress();
            Deencapsulation.setField(kafkaProgress, "partitionIdToOffset", partitionIdToOffset);

            LinkedBlockingDeque<RoutineLoadTaskInfo> routineLoadTaskInfoQueue = new LinkedBlockingDeque<>();
            KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                    partitionIdToOffset, false, -1, false);
            routineLoadTaskInfoQueue.addFirst(routineLoadTaskInfo1);

            Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
            idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

            Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
            idToRoutineLoadJob.put("1", routineLoadJob);

            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(routineLoadManager.getClusterIdleSlotNum()).thenReturn(1);
            Mockito.when(routineLoadManager.checkTaskInJob(Mockito.any(RoutineLoadTaskInfo.class))).thenReturn(true);
            Mockito.when(kafkaRoutineLoadJob1.getDbId()).thenReturn(1L);
            Mockito.when(kafkaRoutineLoadJob1.getTableId()).thenReturn(1L);
            Mockito.when(kafkaRoutineLoadJob1.getName()).thenReturn("");
            Mockito.when(routineLoadManager.getMinTaskBeId(Mockito.anyString())).thenReturn(beId);
            Mockito.when(routineLoadManager.getJob(Mockito.anyLong())).thenReturn(kafkaRoutineLoadJob1);

            RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
            Deencapsulation.setField(routineLoadTaskScheduler, "needScheduleTasksQueue", routineLoadTaskInfoQueue);
            routineLoadTaskScheduler.runAfterCatalogReady();
        }
    }

    @Test
    public void testSubmitTaskFailureRenewsTaskWithJobWriteLock() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        LockCheckingKafkaRoutineLoadJob routineLoadJob = new LockCheckingKafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList",
                Lists.newArrayList(routineLoadTaskInfo));
        Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assertions.assertTrue(routineLoadJob.isRenewCalledWithWriteLock());
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Assertions.assertEquals(1, routineLoadTaskInfoList.size());
        Assertions.assertNotSame(routineLoadTaskInfo, routineLoadTaskInfoList.get(0));

        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assertions.assertSame(routineLoadTaskInfoList.get(0), needScheduleTasksQueue.peek());
    }

    @Test
    public void testSubmitTaskFailureSkipsRenewWhenTaskRemoved() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        LockCheckingKafkaRoutineLoadJob routineLoadJob = new LockCheckingKafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", Lists.newArrayList());
        Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assertions.assertFalse(routineLoadJob.isRenewCalled());
        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assertions.assertTrue(needScheduleTasksQueue.isEmpty());
    }

    @Test
    public void testSubmitTaskFailureSkipsRenewWhenJobPaused() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        LockCheckingKafkaRoutineLoadJob routineLoadJob = new LockCheckingKafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList",
                Lists.newArrayList(routineLoadTaskInfo));
        Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assertions.assertFalse(routineLoadJob.isRenewCalled());
        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assertions.assertTrue(needScheduleTasksQueue.isEmpty());
    }

    private static class LockCheckingKafkaRoutineLoadJob extends KafkaRoutineLoadJob {
        private boolean renewCalled;
        private boolean renewCalledWithWriteLock;

        @Override
        protected RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo,
                boolean delaySchedule) {
            renewCalled = true;
            renewCalledWithWriteLock = lock.isWriteLockedByCurrentThread();
            return super.unprotectRenewTask(routineLoadTaskInfo, delaySchedule);
        }

        private boolean isRenewCalled() {
            return renewCalled;
        }

        private boolean isRenewCalledWithWriteLock() {
            return renewCalledWithWriteLock;
        }
    }

    private static final long JOB_ID = 1L;
    private static final long MAX_BATCH_INTERVAL_S = 60L;

    /** A scheduler whose queue holds this one task, of a job with max_batch_interval = 60 s. No BE is free. */
    private RoutineLoadTaskScheduler schedulerOver(ScriptedKafkaTaskInfo task) throws Exception {
        KafkaRoutineLoadJob routineLoadJob = Mockito.mock(KafkaRoutineLoadJob.class);
        Mockito.when(routineLoadJob.getMaxBatchIntervalS()).thenReturn(MAX_BATCH_INTERVAL_S);
        Mockito.when(routineLoadManager.getJob(JOB_ID)).thenReturn(routineLoadJob);
        Mockito.when(routineLoadManager.checkTaskInJob(Mockito.any(RoutineLoadTaskInfo.class))).thenReturn(true);
        Mockito.when(routineLoadManager.getClusterIdleSlotNum()).thenReturn(1);
        Mockito.when(routineLoadManager.getAvailableBeForTask(Mockito.anyLong(), Mockito.anyLong())).thenReturn(-1L);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        queueOf(routineLoadTaskScheduler).add(task);
        return routineLoadTaskScheduler;
    }

    private LinkedBlockingDeque<RoutineLoadTaskInfo> queueOf(RoutineLoadTaskScheduler routineLoadTaskScheduler) {
        return Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
    }

    /** Scheduler passes over a queue that must hold a task: an empty queue would block the pass for good. */
    private void runPasses(RoutineLoadTaskScheduler routineLoadTaskScheduler, int passes) {
        for (int pass = 0; pass < passes; pass++) {
            Assertions.assertFalse(queueOf(routineLoadTaskScheduler).isEmpty(), "the task was not put back");
            routineLoadTaskScheduler.runAfterCatalogReady();
        }
    }

    private void moveBeyondMaxBatchInterval(RoutineLoadTaskInfo task) {
        task.setLastScheduledTime(System.currentTimeMillis() - (MAX_BATCH_INTERVAL_S + 1) * 1000);
    }

    @Test
    public void testNewTaskWithNothingToConsumeWaitsForMaxBatchInterval() throws Exception {
        // the task of a job that was just created or resumed: never scheduled, not at EOF, not delayed
        ScriptedKafkaTaskInfo task = new ScriptedKafkaTaskInfo(JOB_ID);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = schedulerOver(task);

        runPasses(routineLoadTaskScheduler, 5);
        // It asked the data source once and now waits. Without the mark it asks on every pass.
        Assertions.assertEquals(1, task.lookups);
        Assertions.assertTrue(task.isWaitingForData());
        // the two flags that a commit decides are not touched
        Assertions.assertFalse(task.isDelaySchedule());
        Assertions.assertFalse(task.getIsEof());
        Assertions.assertSame(task, queueOf(routineLoadTaskScheduler).peek());
        // a pass that holds the task does not move its time, or the wait would never end
        long stamp = task.getLastScheduledTime();
        runPasses(routineLoadTaskScheduler, 5);
        Assertions.assertEquals(stamp, task.getLastScheduledTime());
        Assertions.assertEquals(1, task.lookups);

        // max_batch_interval later it asks again, once, and waits again
        moveBeyondMaxBatchInterval(task);
        runPasses(routineLoadTaskScheduler, 5);
        Assertions.assertEquals(2, task.lookups);
        Assertions.assertTrue(task.isWaitingForData());
        // nothing was asked of a BE for a task with nothing to consume
        Mockito.verify(routineLoadManager, Mockito.never()).getAvailableBeForTask(Mockito.anyLong(), Mockito.anyLong());
    }

    @Test
    public void testWaitingTaskGoesOnWhenDataArrives() throws Exception {
        ScriptedKafkaTaskInfo task = new ScriptedKafkaTaskInfo(JOB_ID);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = schedulerOver(task);
        runPasses(routineLoadTaskScheduler, 1);
        Assertions.assertTrue(task.isWaitingForData());

        // Data arrives while the task waits: it is seen at the next look, not before.
        task.hasData = true;
        runPasses(routineLoadTaskScheduler, 5);
        Assertions.assertEquals(1, task.lookups);

        moveBeyondMaxBatchInterval(task);
        runPasses(routineLoadTaskScheduler, 1);
        Assertions.assertEquals(2, task.lookups);
        Assertions.assertFalse(task.isWaitingForData());
        Mockito.verify(routineLoadManager, Mockito.times(1)).getAvailableBeForTask(JOB_ID, -1L);

        // No BE was free, so the task went back to the queue. It has data and no mark: every pass tries again.
        runPasses(routineLoadTaskScheduler, 3);
        Assertions.assertEquals(5, task.lookups);
        Mockito.verify(routineLoadManager, Mockito.times(4)).getAvailableBeForTask(JOB_ID, -1L);
    }

    @Test
    public void testNewTaskWithDataIsNotMarked() throws Exception {
        ScriptedKafkaTaskInfo task = new ScriptedKafkaTaskInfo(JOB_ID);
        task.hasData = true;
        RoutineLoadTaskScheduler routineLoadTaskScheduler = schedulerOver(task);

        runPasses(routineLoadTaskScheduler, 3);
        Assertions.assertEquals(3, task.lookups);
        Assertions.assertFalse(task.isWaitingForData());
        Mockito.verify(routineLoadManager, Mockito.times(3)).getAvailableBeForTask(JOB_ID, -1L);
    }

    @Test
    public void testTaskRenewedAfterCommitNotAtEofIsNotHeld() throws Exception {
        // As a renewal makes a task after a commit that was not at EOF: it carries the old task's time, and
        // neither flag is set. Such a task is scheduled again as soon as data arrives (regression suite
        // test_routine_load_eof), so an empty look must not hold it.
        ScriptedKafkaTaskInfo task = new ScriptedKafkaTaskInfo(JOB_ID, System.currentTimeMillis(), false);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = schedulerOver(task);

        runPasses(routineLoadTaskScheduler, 5);
        Assertions.assertEquals(5, task.lookups);
        Assertions.assertFalse(task.isWaitingForData());
        Assertions.assertFalse(task.needDedalySchedule());
    }

    @Test
    public void testTaskRenewingWaitingTaskStartsWithoutMark() throws Exception {
        ScriptedKafkaTaskInfo task = new ScriptedKafkaTaskInfo(JOB_ID);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = schedulerOver(task);
        runPasses(routineLoadTaskScheduler, 1);
        Assertions.assertTrue(task.isWaitingForData());

        KafkaTaskInfo renewed = new KafkaTaskInfo(task, Maps.newHashMap(), false);
        Assertions.assertFalse(renewed.isWaitingForData());
        Assertions.assertEquals(task.getLastScheduledTime(), renewed.getLastScheduledTime());
    }

    @Test
    public void testWaitingTaskOfRemovedJobIsAbandoned() throws Exception {
        ScriptedKafkaTaskInfo task = new ScriptedKafkaTaskInfo(JOB_ID);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = schedulerOver(task);
        runPasses(routineLoadTaskScheduler, 1);
        Assertions.assertTrue(task.isWaitingForData());
        Mockito.verify(routineLoadManager, Mockito.times(1)).checkTaskInJob(task);

        // The job is removed while its task waits. The next pass hands the task to the check that abandons
        // it, instead of failing on the missing job where the wait is measured.
        Mockito.when(routineLoadManager.getJob(JOB_ID)).thenReturn(null);
        Mockito.when(routineLoadManager.checkTaskInJob(Mockito.any(RoutineLoadTaskInfo.class))).thenReturn(false);
        runPasses(routineLoadTaskScheduler, 1);
        Mockito.verify(routineLoadManager, Mockito.times(2)).checkTaskInJob(task);
        Assertions.assertTrue(queueOf(routineLoadTaskScheduler).isEmpty());
        Assertions.assertEquals(1, task.lookups);
    }

    /** A Kafka task whose data source says what the test tells it to. It counts how often the scheduler asks. */
    private static class ScriptedKafkaTaskInfo extends KafkaTaskInfo {
        private boolean hasData = false;
        private int lookups = 0;

        // as divideRoutineLoadJob makes a task: never scheduled, not at EOF
        private ScriptedKafkaTaskInfo(long jobId) {
            this(jobId, -1, false);
        }

        // as a renewal makes a task: it carries the time and the EOF verdict of the task it renews
        private ScriptedKafkaTaskInfo(long jobId, long lastScheduledTime, boolean isEof) {
            super(UUID.randomUUID(), jobId, 20000, Maps.newHashMap(), false, lastScheduledTime, isEof);
        }

        @Override
        protected boolean hasMoreDataToConsume() {
            lookups++;
            return hasData;
        }
    }
}
