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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.load.CloudRoutineLoadManager;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * The transitional {@code compute_group} declaration on routine load jobs.
 *
 * <p>The declaration lives in {@code jobProperties} under the key {@code compute_group}. It is only
 * written when the user actually declared one, so an absent key keeps the old behaviour of using the
 * cluster snapshotted from the creating session, and later versions can tell a pinned group apart
 * from an inherited one just by looking at whether the key is present.
 */
public class RoutineLoadComputeGroupTest {

    private static final String SESSION_SNAPSHOT_CG = "cg_from_session";
    private static final String DECLARED_CG = "cg_declared";

    private String originalDeployMode;
    private String originalCloudUniqueId;
    private SystemInfoService originalSystemInfo;
    private AccessControllerManager originalAccessManager;

    @Before
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalSystemInfo = Env.getCurrentSystemInfo();
        originalAccessManager = Env.getCurrentEnv().getAccessManager();
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfo);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", originalAccessManager);
        ConnectContext.remove();
    }

    /**
     * Puts the FE into cloud mode with a known set of existing compute groups, and a session that
     * holds (or does not hold) USAGE on whatever it asks for.
     */
    private void enterCloudMode(boolean hasPriv, List<String> existingClusters) {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";

        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(accessManager.checkCloudPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.any(PrivPredicate.class), Mockito.any(ResourceTypeEnum.class))).thenReturn(hasPriv);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", accessManager);

        Map<String, List<Backend>> clusterToBackends = Maps.newHashMap();
        for (String cluster : existingClusters) {
            clusterToBackends.put(cluster, Lists.newArrayList(createBackend(10000L + clusterToBackends.size())));
        }
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo",
                new ClusterAwareCloudSystemInfoService(clusterToBackends));

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.setThreadLocalInfo();
    }

    private KafkaRoutineLoadJob newJob(String snapshotCluster, String declaredComputeGroup) {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        // serialization walks this field, give it something non-null
        job.setOrigStmt(new OriginStatement("CREATE ROUTINE LOAD test ON tbl", 0));
        Deencapsulation.setField(job, "cloudCluster", snapshotCluster);
        if (declaredComputeGroup != null) {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(RoutineLoadJob.COMPUTE_GROUP, declaredComputeGroup);
            Deencapsulation.setField(job, "jobProperties", jobProperties);
        }
        return job;
    }

    @Test
    public void testDeclaredComputeGroupWinsOverSessionSnapshot() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, DECLARED_CG);
        Assert.assertEquals(DECLARED_CG, job.getDeclaredComputeGroup());
        Assert.assertEquals(DECLARED_CG, job.getCloudCluster());
    }

    // No declaration must leave the existing behaviour completely untouched.
    @Test
    public void testFallsBackToSessionSnapshotWhenNotDeclared() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, null);
        Assert.assertNull(job.getDeclaredComputeGroup());
        Assert.assertEquals(SESSION_SNAPSHOT_CG, job.getCloudCluster());
    }

    // An empty declared value is treated as "not declared", same as the workload group property.
    @Test
    public void testEmptyDeclarationFallsBackToSnapshot() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, "");
        Assert.assertEquals(SESSION_SNAPSHOT_CG, job.getCloudCluster());
    }

    // SHOW ROUTINE LOAD must report the cluster the job actually runs in.
    @Test
    public void testClusterInfoShowsEffectiveComputeGroup() {
        Assert.assertEquals(DECLARED_CG, newJob(SESSION_SNAPSHOT_CG, DECLARED_CG).getClusterInfo());
        Assert.assertEquals(SESSION_SNAPSHOT_CG, newJob(SESSION_SNAPSHOT_CG, null).getClusterInfo());
        Assert.assertEquals("", newJob(null, null).getClusterInfo());
    }

    // Metadata compatibility: the declaration is carried in the already existing jobProperties map,
    // so it survives a serialization round trip without any new persisted field.
    @Test
    public void testMetadataRoundTripKeepsDeclaration() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, DECLARED_CG);
        String json = GsonUtils.GSON.toJson(job);
        Assert.assertTrue(json, json.contains(RoutineLoadJob.COMPUTE_GROUP));

        KafkaRoutineLoadJob restored = GsonUtils.GSON.fromJson(json, KafkaRoutineLoadJob.class);
        Assert.assertEquals(DECLARED_CG, restored.getDeclaredComputeGroup());
        Assert.assertEquals(DECLARED_CG, restored.getCloudCluster());
    }

    // Downgrade safety: metadata written without the key must still load and behave as before.
    @Test
    public void testMetadataRoundTripWithoutDeclaration() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, null);
        String json = GsonUtils.GSON.toJson(job);
        Assert.assertFalse(json, json.contains(RoutineLoadJob.COMPUTE_GROUP));

        KafkaRoutineLoadJob restored = GsonUtils.GSON.fromJson(json, KafkaRoutineLoadJob.class);
        Assert.assertNull(restored.getDeclaredComputeGroup());
        Assert.assertEquals(SESSION_SNAPSHOT_CG, restored.getCloudCluster());
    }

    /**
     * Isolation: two jobs pinned to different compute groups must be scheduled onto disjoint sets of
     * backends, so neither can consume the other's resources. The backend set is resolved per job
     * and on every task allocation, not once at create time.
     */
    @Test
    public void testJobsWithDifferentComputeGroupsGetDisjointBackends() throws LoadException {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";

        Backend beInA1 = createBackend(10001L);
        Backend beInA2 = createBackend(10002L);
        Backend beInB = createBackend(10003L);
        Map<String, List<Backend>> clusterToBackends = Maps.newHashMap();
        clusterToBackends.put("cg_a", Lists.newArrayList(beInA1, beInA2));
        clusterToBackends.put("cg_b", Lists.newArrayList(beInB));
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo",
                new ClusterAwareCloudSystemInfoService(clusterToBackends));

        KafkaRoutineLoadJob jobA = newJob(SESSION_SNAPSHOT_CG, "cg_a");
        KafkaRoutineLoadJob jobB = newJob(SESSION_SNAPSHOT_CG, "cg_b");
        Map<Long, RoutineLoadJob> jobs = Maps.newHashMap();
        jobs.put(1L, jobA);
        jobs.put(2L, jobB);
        TestCloudRoutineLoadManager manager = new TestCloudRoutineLoadManager(jobs);

        List<Long> backendsForA = manager.getAvailableBackendIdsForTest(1L);
        List<Long> backendsForB = manager.getAvailableBackendIdsForTest(2L);

        Assert.assertEquals(Lists.newArrayList(beInA1.getId(), beInA2.getId()), backendsForA);
        Assert.assertEquals(Lists.newArrayList(beInB.getId()), backendsForB);
        Assert.assertTrue("jobs pinned to different compute groups must not share backends",
                backendsForA.stream().noneMatch(backendsForB::contains));
    }

    /**
     * Isolation is driven by the declaration, not by the creating session: both jobs were created in
     * the same session (same snapshot cluster) yet land on different backends.
     */
    @Test
    public void testDeclarationOverridesIdenticalCreatingSession() throws LoadException {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";

        Backend beInA = createBackend(10001L);
        Backend beInSession = createBackend(10009L);
        Map<String, List<Backend>> clusterToBackends = Maps.newHashMap();
        clusterToBackends.put("cg_a", Lists.newArrayList(beInA));
        clusterToBackends.put(SESSION_SNAPSHOT_CG, Lists.newArrayList(beInSession));
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo",
                new ClusterAwareCloudSystemInfoService(clusterToBackends));

        Map<Long, RoutineLoadJob> jobs = Maps.newHashMap();
        jobs.put(1L, newJob(SESSION_SNAPSHOT_CG, "cg_a"));
        jobs.put(2L, newJob(SESSION_SNAPSHOT_CG, null));
        TestCloudRoutineLoadManager manager = new TestCloudRoutineLoadManager(jobs);

        Assert.assertEquals(Lists.newArrayList(beInA.getId()), manager.getAvailableBackendIdsForTest(1L));
        Assert.assertEquals(Lists.newArrayList(beInSession.getId()), manager.getAvailableBackendIdsForTest(2L));
    }

    // Changing the declaration takes effect on the next task allocation, no restart required.
    @Test
    public void testAlteringDeclarationMovesJobToAnotherComputeGroup() throws LoadException {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";

        Backend beInA = createBackend(10001L);
        Backend beInB = createBackend(10002L);
        Map<String, List<Backend>> clusterToBackends = Maps.newHashMap();
        clusterToBackends.put("cg_a", Lists.newArrayList(beInA));
        clusterToBackends.put("cg_b", Lists.newArrayList(beInB));
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo",
                new ClusterAwareCloudSystemInfoService(clusterToBackends));

        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, "cg_a");
        Map<Long, RoutineLoadJob> jobs = Maps.newHashMap();
        jobs.put(1L, job);
        TestCloudRoutineLoadManager manager = new TestCloudRoutineLoadManager(jobs);
        Assert.assertEquals(Lists.newArrayList(beInA.getId()), manager.getAvailableBackendIdsForTest(1L));

        // what ALTER ROUTINE LOAD ... PROPERTIES("compute_group" = "cg_b") ends up doing
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put(RoutineLoadJob.COMPUTE_GROUP, "cg_b");

        Assert.assertEquals(Lists.newArrayList(beInB.getId()), manager.getAvailableBackendIdsForTest(1L));
    }

    /**
     * Metadata load must not re-check the declared compute group.
     *
     * <p>{@link RoutineLoadJob#gsonPostProcess()} re-parses the stored CREATE statement on every FE
     * metadata load - a restart, and every checkpoint - only to rebuild the RoutineLoadDesc, and it
     * turns any failure into {@code JobState.CANCELLED}, which is final and cannot be undone by
     * RESUME. A compute group can be dropped, renamed, or scaled to zero backends (which
     * CloudSystemInfoService treats as dropped) while a job exists, so validating it there would
     * kill the job permanently instead of letting the per task check pause it recoverably.
     */
    @Test
    public void testMetadataLoadDoesNotValidateDeclaredComputeGroup() throws UserException {
        enterCloudMode(true, Lists.newArrayList("cg_live"));

        CreateRoutineLoadInfo info = newCreateInfo("cg_dropped");
        info.setReplay(true);

        info.checkJobProperties();

        // Only the resource check is skipped: the declaration itself is still adopted, so the job
        // keeps running in the group it was pinned to once the group comes back.
        Assert.assertEquals("cg_dropped", info.getComputeGroupName());
    }

    // Negative control for the test above: on the real CREATE path the same value must still be
    // rejected, otherwise the skip would have disabled validation everywhere.
    @Test
    public void testCreateStillRejectsComputeGroupThatDoesNotExist() {
        enterCloudMode(true, Lists.newArrayList("cg_live"));

        CreateRoutineLoadInfo info = newCreateInfo("cg_dropped");

        UserException e = Assert.assertThrows(UserException.class, info::checkJobProperties);
        Assert.assertTrue(e.getMessage(),
                e.getMessage().contains("Compute group 'cg_dropped' not found."));
    }

    /**
     * The per task re-check has to run before the task takes any resource.
     *
     * <p>Backend allocation resolves the backends through the very compute group being checked, so
     * a missing group makes it return an empty list and pause the job with a generic
     * "no available BE found for job ... please check the BE status and user's cluster or tags".
     * If the re-check ran after allocation it could never fire for a dropped group, and the
     * operator would be sent to look at BE health for what is really a compute group that is gone.
     * Running it first also means no transaction has been begun yet when it fails.
     */
    @Test
    public void testDroppedComputeGroupFailsTaskBeforeBackendAllocation() {
        enterCloudMode(true, Lists.newArrayList("cg_live"));

        RecordingKafkaRoutineLoadJob job = new RecordingKafkaRoutineLoadJob();
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(job, "userIdentity", UserIdentity.ADMIN);
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(RoutineLoadJob.COMPUTE_GROUP, "cg_dropped");
        Deencapsulation.setField(job, "jobProperties", jobProperties);

        Map<Long, RoutineLoadJob> jobs = Maps.newHashMap();
        jobs.put(1L, job);
        RoutineLoadTaskScheduler scheduler =
                new RoutineLoadTaskScheduler(new TestCloudRoutineLoadManager(jobs));

        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo taskInfo = new AlwaysReadyKafkaTaskInfo(new UUID(1, 1), 1L, partitionIdToOffset);

        try {
            Deencapsulation.invoke(scheduler, "scheduleOneTask", taskInfo);
            // Without the check in front, scheduling reaches allocateTaskToBe, which finds no
            // backend for the dropped group, pauses the job with "no available BE found" and
            // returns normally - so reaching this line is itself the regression.
            Assert.fail("scheduling a task of a job pinned to a dropped compute group must fail,"
                    + " last pause reason: " + job.pauseMsg);
        } catch (Exception expected) {
            // scheduleOneTask pauses the job and rethrows
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, job.newState);
        Assert.assertNotNull("the job must be paused with a reason", job.pauseMsg);
        Assert.assertTrue(job.pauseMsg, job.pauseMsg.contains("Compute group 'cg_dropped' not found."));
        Assert.assertFalse("the operator must not be sent to look at BE status for a dropped group",
                job.pauseMsg.contains("no available BE found"));

        // A group can come back by itself - one that is only scaled to zero backends leaves the
        // cluster map and re-enters it when it scales up - so this pause has to stay retryable.
        Assert.assertNotEquals(InternalErrorCode.CANNOT_RESUME_ERR, job.pauseCode);
        Assert.assertTrue("a dropped group must still auto resume once it is back",
                ScheduleRule.isNeedAutoSchedule(job));
    }

    /**
     * A revoked privilege is not transient, so the job must stop instead of flapping.
     *
     * <p>Pausing with the default INTERNAL_ERR would let {@link ScheduleRule} auto resume the job
     * within at most MAX_BACK_OFF_TIME_SEC, whereupon the next task fails the same check and pauses
     * it again - for as long as the grant is missing, at two edit log entries per cycle.
     */
    @Test
    public void testRevokedUsagePausesTaskWithoutAutoResume() {
        // the group exists, so the check gets past existence and fails on the privilege
        enterCloudMode(false, Lists.newArrayList("cg_pinned"));

        RecordingKafkaRoutineLoadJob job = new RecordingKafkaRoutineLoadJob();
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(job, "userIdentity", UserIdentity.ADMIN);
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(RoutineLoadJob.COMPUTE_GROUP, "cg_pinned");
        Deencapsulation.setField(job, "jobProperties", jobProperties);

        Map<Long, RoutineLoadJob> jobs = Maps.newHashMap();
        jobs.put(1L, job);
        RoutineLoadTaskScheduler scheduler =
                new RoutineLoadTaskScheduler(new TestCloudRoutineLoadManager(jobs));

        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo taskInfo = new AlwaysReadyKafkaTaskInfo(new UUID(1, 1), 1L, partitionIdToOffset);

        try {
            Deencapsulation.invoke(scheduler, "scheduleOneTask", taskInfo);
            Assert.fail("a task whose owner lost USAGE on the pinned group must fail");
        } catch (Exception expected) {
            // scheduleOneTask pauses the job and rethrows
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, job.newState);
        Assert.assertNotNull("the job must be paused with a reason", job.pauseMsg);
        Assert.assertTrue(job.pauseMsg, job.pauseMsg.contains("USAGE denied"));
        Assert.assertEquals(InternalErrorCode.CANNOT_RESUME_ERR, job.pauseCode);
        Assert.assertFalse("a revoked privilege must not be auto resumed",
                ScheduleRule.isNeedAutoSchedule(job));
    }

    private CreateRoutineLoadInfo newCreateInfo(String declaredComputeGroup) {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.COMPUTE_GROUP, declaredComputeGroup);
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("kafka_broker_list", "127.0.0.1:9092");
        dataSourceProperties.put("kafka_topic", "test_topic");
        Map<String, LoadProperty> loadPropertyMap = Maps.newHashMap();
        return new CreateRoutineLoadInfo(new LabelNameInfo("test_db", "test_job"), "test_tbl",
                loadPropertyMap, jobProperties, "kafka", dataSourceProperties,
                LoadTask.MergeType.APPEND, "");
    }

    private Backend createBackend(long id) {
        Backend backend = new Backend(id, "127.0.0." + id, 9050);
        backend.setAlive(true);
        return backend;
    }

    /**
     * KafkaTaskInfo asks the real RoutineLoadManager, and then Kafka, whether there is more data to
     * consume. Neither exists here, so answer yes and let scheduling proceed to the part under
     * test: without the compute group check in front, it must reach backend allocation.
     */
    private static class AlwaysReadyKafkaTaskInfo extends KafkaTaskInfo {
        private AlwaysReadyKafkaTaskInfo(UUID id, long jobId, Map<Integer, Long> partitionIdToOffset) {
            super(id, jobId, 20000, partitionIdToOffset, false, -1, false);
        }

        @Override
        boolean hasMoreDataToConsume() {
            return true;
        }
    }

    /**
     * Captures the state transition instead of writing an edit log, and mirrors it onto the real
     * fields so that ScheduleRule can be asked what it would do with the resulting pause.
     */
    private static class RecordingKafkaRoutineLoadJob extends KafkaRoutineLoadJob {
        private JobState newState;
        private String pauseMsg;
        private InternalErrorCode pauseCode;

        @Override
        public void updateState(JobState jobState, ErrorReason reason, boolean isReplay) {
            this.newState = jobState;
            this.pauseMsg = reason == null ? null : reason.getMsg();
            this.pauseCode = reason == null ? null : reason.getCode();
            this.state = jobState;
            this.pauseReason = reason;
        }
    }

    private static class ClusterAwareCloudSystemInfoService extends CloudSystemInfoService {
        private final Map<String, List<Backend>> clusterToBackends;

        private ClusterAwareCloudSystemInfoService(Map<String, List<Backend>> clusterToBackends) {
            this.clusterToBackends = clusterToBackends;
        }

        @Override
        public List<Backend> getBackendsByClusterName(final String clusterName) {
            return clusterToBackends.getOrDefault(clusterName, Lists.newArrayList());
        }

        @Override
        public List<String> getCloudClusterNames() {
            return clusterToBackends.keySet().stream().collect(Collectors.toList());
        }
    }

    private static class TestCloudRoutineLoadManager extends CloudRoutineLoadManager {
        private final Map<Long, RoutineLoadJob> jobs;

        private TestCloudRoutineLoadManager(Map<Long, RoutineLoadJob> jobs) {
            this.jobs = jobs;
        }

        @Override
        public RoutineLoadJob getJob(long jobId) {
            return jobs.get(jobId);
        }

        // the real one looks the task up in idToRoutineLoadJob, which this stub never fills
        @Override
        public boolean checkTaskInJob(RoutineLoadTaskInfo task) {
            return jobs.containsKey(task.getJobId());
        }

        private List<Long> getAvailableBackendIdsForTest(long jobId) throws LoadException {
            return super.getAvailableBackendIds(jobId);
        }
    }
}
