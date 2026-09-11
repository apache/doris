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

package org.apache.doris.cloud;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.CloudWarmUpJob.JobState;
import org.apache.doris.cloud.CloudWarmUpJob.JobType;
import org.apache.doris.cloud.CloudWarmUpJob.SyncEvent;
import org.apache.doris.cloud.CloudWarmUpJob.SyncMode;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWarmUpTabletsRequest;
import org.apache.doris.thrift.TWarmUpTabletsRequestType;
import org.apache.doris.thrift.TWarmUpTabletsResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CacheHotspotManagerRecoveryTest {
    private CacheHotspotManager manager;
    private CloudEnv env;
    private EditLog editLog;
    private MockedStatic<Env> envMock;
    private GenericPool<BackendService.Client> originalBackendPool;
    private GenericPool<BackendService.Client> backendPool;
    private BackendService.Client client;
    private boolean originalRunningUnitTest;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() throws Exception {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        originalBackendPool = ClientPool.backendPool;
        backendPool = Mockito.mock(GenericPool.class);
        ClientPool.backendPool = backendPool;
        client = Mockito.mock(BackendService.Client.class);
        Mockito.when(backendPool.borrowObject(Mockito.any(TNetworkAddress.class))).thenReturn(client);
        Mockito.when(client.warmUpTablets(Mockito.any())).thenAnswer(invocation -> response(0));

        CloudSystemInfoService systemInfo = Mockito.mock(CloudSystemInfoService.class);
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBePort(9060);
        Mockito.when(systemInfo.getBackendsByClusterName(Mockito.anyString()))
                .thenReturn(Collections.singletonList(backend));
        manager = new CacheHotspotManager(systemInfo, Mockito.mock(ThreadPoolExecutor.class));
        env = Mockito.mock(CloudEnv.class);
        editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getCacheHotspotMgr()).thenReturn(manager);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.loadCloudWarmUpJob(Mockito.any(DataInputStream.class), Mockito.anyLong()))
                .thenCallRealMethod();
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
    }

    @AfterEach
    public void tearDown() {
        envMock.close();
        ClientPool.backendPool = originalBackendPool;
        FeConstants.runningUnitTest = originalRunningUnitTest;
    }

    @ParameterizedTest
    @EnumSource(value = SyncMode.class, names = {"ONCE", "PERIODIC"})
    public void testRunningJobRecoveredFromJournalBlocksNewJob(SyncMode mode) throws Exception {
        CloudWarmUpJob job = newJob(204L, mode, JobState.RUNNING);
        job.setLastBatchId(3L);
        CloudWarmUpJob restored = roundTrip(job);
        manager.replayCloudWarmUpJob(restored);
        manager.recoverRunningJobsBeforeStart();

        Assertions.assertFalse(manager.tryRegisterRunningJob(newJob(205L, SyncMode.ONCE, JobState.PENDING)),
                "The recovered running job must keep the destination reserved");
        Assertions.assertEquals(JobState.RUNNING, restored.getJobState());
        Assertions.assertEquals(job.startTimeMs, restored.startTimeMs);
        Assertions.assertEquals(3L, restored.getLastBatchId());
        Assertions.assertEquals(job.beToTabletIdBatches, restored.beToTabletIdBatches);
        Mockito.verifyNoInteractions(editLog, backendPool, client);
    }

    @ParameterizedTest
    @EnumSource(value = SyncMode.class, names = {"ONCE", "PERIODIC"})
    public void testRunningJobRecoveredOnlyFromImageBlocksNewJob(SyncMode mode) throws Exception {
        loadImage(newJob(204L, mode, JobState.RUNNING));
        manager.recoverRunningJobsBeforeStart();

        Assertions.assertFalse(manager.tryRegisterRunningJob(newJob(205L, SyncMode.ONCE, JobState.PENDING)));
        Mockito.verifyNoInteractions(editLog, backendPool, client);
    }

    @ParameterizedTest
    @EnumSource(value = JobState.class, names = {"PENDING", "FINISHED", "CANCELLED", "DELETED"})
    public void testLatestJournalStateOverridesRunningImage(JobState finalState) throws Exception {
        CloudWarmUpJob job = newJob(204L, SyncMode.PERIODIC, JobState.RUNNING);
        loadImage(job);
        job.setJobState(finalState);
        manager.replayCloudWarmUpJob(roundTrip(job));
        manager.recoverRunningJobsBeforeStart();

        Assertions.assertTrue(manager.tryRegisterRunningJob(newJob(205L, SyncMode.ONCE, JobState.PENDING)));
        Mockito.verifyNoInteractions(editLog, backendPool, client);
    }

    @Test
    public void testRepeatedReplayAndOldCompletionDoNotReleaseNewOwner() throws Exception {
        CloudWarmUpJob first = newJob(204L, SyncMode.PERIODIC, JobState.RUNNING);
        manager.replayCloudWarmUpJob(roundTrip(first));
        manager.replayCloudWarmUpJob(roundTrip(first));
        first.setJobState(JobState.PENDING);
        manager.replayCloudWarmUpJob(roundTrip(first));
        CloudWarmUpJob second = newJob(205L, SyncMode.ONCE, JobState.RUNNING);
        manager.replayCloudWarmUpJob(roundTrip(second));
        manager.recoverRunningJobsBeforeStart();
        manager.recoverRunningJobsBeforeStart();
        manager.notifyJobStop(first);

        Assertions.assertTrue(manager.tryRegisterRunningJob(second));
        Assertions.assertFalse(manager.tryRegisterRunningJob(first));
        manager.cancelRecoveredConflictingJobs();
        Mockito.verifyNoInteractions(editLog, backendPool, client);
    }

    @Test
    public void testLegacyOnceJobWithoutSyncModeReservesDestination() throws Exception {
        CloudWarmUpJob job = newJob(204L, SyncMode.ONCE, JobState.RUNNING);
        job.syncMode = null;
        loadImage(job);
        manager.recoverRunningJobsBeforeStart();

        Assertions.assertTrue(manager.getCloudWarmUpJob(204L).isOnce());
        Assertions.assertFalse(manager.tryRegisterRunningJob(newJob(205L, SyncMode.ONCE, JobState.PENDING)));
    }

    @Test
    public void testEventDrivenAndDifferentDestinationsRemainIndependent() throws Exception {
        CloudWarmUpJob event = newJob(204L, SyncMode.EVENT_DRIVEN, JobState.RUNNING);
        loadImage(event);
        CloudWarmUpJob running = newJob(205L, SyncMode.ONCE, JobState.RUNNING);
        running.setCloudClusterName("another_target");
        manager.replayCloudWarmUpJob(roundTrip(running));
        manager.recoverRunningJobsBeforeStart();

        Assertions.assertTrue(manager.tryRegisterRunningJob(newJob(206L, SyncMode.ONCE, JobState.PENDING)));
        Assertions.assertTrue(manager.tryRegisterRunningJob(event));
        CloudWarmUpJob blocked = newJob(207L, SyncMode.ONCE, JobState.PENDING);
        blocked.setCloudClusterName("another_target");
        Assertions.assertFalse(manager.tryRegisterRunningJob(blocked));
        manager.cancelRecoveredConflictingJobs();
        Mockito.verifyNoInteractions(editLog, backendPool, client);
    }

    @ParameterizedTest
    @EnumSource(value = SyncMode.class, names = {"ONCE", "PERIODIC"})
    public void testRecoveredJobFinishesBeforeQueuedJobStarts(SyncMode mode) throws Exception {
        CloudWarmUpJob running = newJob(204L, mode, JobState.RUNNING);
        running.setLastBatchId(0L);
        running.setBeToTabletIdBatches(Collections.singletonMap(1L,
                Arrays.asList(Collections.singletonList(11L), Collections.singletonList(12L))));
        manager.replayCloudWarmUpJob(roundTrip(running));
        CloudWarmUpJob restored = manager.getCloudWarmUpJob(204L);
        CloudWarmUpJob pending = newJob(205L, SyncMode.ONCE, JobState.PENDING);
        pending.setJobType(JobType.TABLE);
        manager.addCloudWarmUpJob(pending);
        manager.recoverRunningJobsBeforeStart();
        AtomicBoolean oldBatchPending = new AtomicBoolean(true);
        Mockito.when(client.warmUpTablets(Mockito.any())).thenAnswer(invocation -> {
            TWarmUpTabletsRequest request = invocation.getArgument(0);
            return response(request.getType() == TWarmUpTabletsRequestType.GET_CURRENT_JOB_STATE_AND_LEASE
                    && oldBatchPending.get() ? 1 : 0);
        });

        restored.run();
        pending.run();
        Assertions.assertEquals(JobState.RUNNING, restored.getJobState());
        Assertions.assertEquals(JobState.PENDING, pending.getJobState());
        Mockito.verify(client, Mockito.never()).warmUpTablets(Mockito.argThat(request -> request.getJobId() == 205L));
        Mockito.verifyNoInteractions(editLog);

        oldBatchPending.set(false);
        restored.run();
        Assertions.assertEquals(mode == SyncMode.ONCE ? JobState.FINISHED : JobState.PENDING,
                restored.getJobState());
        pending.run();
        Assertions.assertEquals(JobState.RUNNING, pending.getJobState());
        pending.run();
        pending.run();
        Assertions.assertEquals(JobState.FINISHED, pending.getJobState());
        Assertions.assertEquals("", pending.errMsg);
        Mockito.verify(client).warmUpTablets(Mockito.argThat(request -> request.getJobId() == 205L
                && request.getType() == TWarmUpTabletsRequestType.SET_JOB));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConflictingJobsAreCleanedDeterministically(boolean reverseOrder) throws Exception {
        CloudWarmUpJob owner = newJob(204L, SyncMode.ONCE, JobState.RUNNING);
        CloudWarmUpJob periodic = newJob(205L, SyncMode.PERIODIC, JobState.RUNNING);
        CloudWarmUpJob once = newJob(206L, SyncMode.ONCE, JobState.RUNNING);
        List<CloudWarmUpJob> jobs = Arrays.asList(owner, periodic, once);
        if (reverseOrder) {
            Collections.reverse(jobs);
        }
        for (CloudWarmUpJob job : jobs) {
            manager.replayCloudWarmUpJob(roundTrip(job));
        }
        manager.recoverRunningJobsBeforeStart();
        manager.recoverRunningJobsBeforeStart();
        Mockito.verifyNoInteractions(editLog, backendPool, client);

        manager.cancelRecoveredConflictingJobs();
        Assertions.assertEquals(JobState.RUNNING, manager.getCloudWarmUpJob(204L).getJobState());
        Assertions.assertEquals(JobState.PENDING, manager.getCloudWarmUpJob(205L).getJobState());
        Assertions.assertEquals(JobState.CANCELLED, manager.getCloudWarmUpJob(206L).getJobState());
        Assertions.assertTrue(manager.getCloudWarmUpJob(205L).errMsg.contains("204"));
        Assertions.assertTrue(manager.tryRegisterRunningJob(owner));
        Assertions.assertFalse(manager.tryRegisterRunningJob(periodic));
        ArgumentCaptor<TWarmUpTabletsRequest> requests = ArgumentCaptor.forClass(TWarmUpTabletsRequest.class);
        Mockito.verify(client, Mockito.times(2)).warmUpTablets(requests.capture());
        Assertions.assertEquals(Arrays.asList(205L, 206L), requests.getAllValues().stream()
                .map(TWarmUpTabletsRequest::getJobId).collect(Collectors.toList()));
        Assertions.assertTrue(requests.getAllValues().stream()
                .allMatch(request -> request.getType() == TWarmUpTabletsRequestType.CLEAR_JOB));
        Mockito.verify(editLog).logModifyCloudWarmUpJob(manager.getCloudWarmUpJob(205L));
        Mockito.verify(editLog).logModifyCloudWarmUpJob(manager.getCloudWarmUpJob(206L));

        Mockito.clearInvocations(editLog, backendPool, client);
        manager.cancelRecoveredConflictingJobs();
        Mockito.verifyNoInteractions(editLog, backendPool, client);
    }

    @Test
    public void testUserCancelledConflictDoesNotClearTheOwner() throws Exception {
        manager.replayCloudWarmUpJob(roundTrip(newJob(204L, SyncMode.ONCE, JobState.RUNNING)));
        manager.replayCloudWarmUpJob(roundTrip(newJob(205L, SyncMode.ONCE, JobState.RUNNING)));
        manager.recoverRunningJobsBeforeStart();
        Assertions.assertTrue(manager.getCloudWarmUpJob(205L).cancel("user cancel", true));
        Mockito.clearInvocations(editLog, backendPool, client);

        manager.cancelRecoveredConflictingJobs();

        Assertions.assertFalse(manager.tryRegisterRunningJob(newJob(206L, SyncMode.ONCE, JobState.PENDING)));
        Mockito.verifyNoInteractions(editLog, backendPool, client);
    }

    private void loadImage(CloudWarmUpJob job) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(0); // Legacy runnable jobs.
        output.writeInt(0); // Legacy finished jobs.
        output.writeInt(1);
        job.write(output);
        Assertions.assertEquals(1L, env.loadCloudWarmUpJob(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())), 0L));
    }

    private CloudWarmUpJob roundTrip(CloudWarmUpJob job) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        job.write(new DataOutputStream(bytes));
        return CloudWarmUpJob.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    private CloudWarmUpJob newJob(long jobId, SyncMode syncMode, JobState state) {
        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(jobId)
                .setSrcClusterName("source_" + jobId)
                .setDstClusterName("target_cluster")
                .setSyncMode(syncMode)
                .setSyncEvent(SyncEvent.LOAD)
                .setSyncInterval(60L)
                .build();
        job.setJobState(state);
        job.startTimeMs = System.currentTimeMillis();
        job.setBeToThriftAddress(Collections.singletonMap(1L, "127.0.0.1:9060"));
        job.setBeToTabletIdBatches(Collections.singletonMap(1L,
                Collections.singletonList(Collections.singletonList(11L))));
        return job;
    }

    private TWarmUpTabletsResponse response(int pendingJobs) {
        TWarmUpTabletsResponse response = new TWarmUpTabletsResponse();
        response.setStatus(new TStatus(TStatusCode.OK));
        response.setPendingJobSize(pendingJobs);
        return response;
    }
}
