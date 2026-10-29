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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.datasource.kinesis.KinesisUtil;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.meta.PersistMetaModules;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
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
    private WorkloadGroupMgr originalWorkloadGroupMgr;
    private ComputeGroupMgr originalComputeGroupMgr;

    @BeforeEach
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalSystemInfo = Env.getCurrentSystemInfo();
        originalAccessManager = Env.getCurrentEnv().getAccessManager();
        originalWorkloadGroupMgr = Env.getCurrentEnv().getWorkloadGroupMgr();
        originalComputeGroupMgr = Env.getCurrentEnv().getComputeGroupMgr();
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfo);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", originalAccessManager);
        Deencapsulation.setField(Env.getCurrentEnv(), "workloadGroupMgr", originalWorkloadGroupMgr);
        Deencapsulation.setField(Env.getCurrentEnv(), "computeGroupMgr", originalComputeGroupMgr);
        ConnectContext.remove();
    }

    /**
     * Puts the FE into cloud mode with a known set of existing compute groups, and a session that
     * holds (or does not hold) USAGE on whatever it asks for.
     */
    private void enterCloudMode(boolean hasPriv, List<String> existingClusters) {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";

        mockAccessManager(hasPriv);

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

    // USAGE on every compute group and workload group the session asks for, or on none.
    private void mockAccessManager(boolean hasPriv) {
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(accessManager.checkCloudPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.any(PrivPredicate.class), Mockito.any(ResourceTypeEnum.class))).thenReturn(hasPriv);
        Mockito.when(accessManager.checkWorkloadGroupPriv(Mockito.any(ConnectContext.class), Mockito.anyString(),
                Mockito.any(PrivPredicate.class))).thenReturn(hasPriv);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", accessManager);
    }

    private ClusterAwareCloudSystemInfoService cloudSystemInfo() {
        return (ClusterAwareCloudSystemInfoService) Env.getCurrentSystemInfo();
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
        Assertions.assertEquals(DECLARED_CG, job.getDeclaredComputeGroup());
        Assertions.assertEquals(DECLARED_CG, job.getCloudCluster());
    }

    // No declaration must leave the existing behaviour completely untouched.
    @Test
    public void testFallsBackToSessionSnapshotWhenNotDeclared() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, null);
        Assertions.assertNull(job.getDeclaredComputeGroup());
        Assertions.assertEquals(SESSION_SNAPSHOT_CG, job.getCloudCluster());
    }

    // An empty declared value is treated as "not declared", same as the workload group property.
    @Test
    public void testEmptyDeclarationFallsBackToSnapshot() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, "");
        Assertions.assertEquals(SESSION_SNAPSHOT_CG, job.getCloudCluster());
    }

    // SHOW ROUTINE LOAD must report the cluster the job actually runs in.
    @Test
    public void testClusterInfoShowsEffectiveComputeGroup() {
        Assertions.assertEquals(DECLARED_CG, newJob(SESSION_SNAPSHOT_CG, DECLARED_CG).getClusterInfo());
        Assertions.assertEquals(SESSION_SNAPSHOT_CG, newJob(SESSION_SNAPSHOT_CG, null).getClusterInfo());
        Assertions.assertEquals("", newJob(null, null).getClusterInfo());
    }

    // Metadata compatibility: the declaration is carried in the already existing jobProperties map,
    // so it survives a serialization round trip without any new persisted field.
    @Test
    public void testMetadataRoundTripKeepsDeclaration() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, DECLARED_CG);
        String json = GsonUtils.GSON.toJson(job);
        Assertions.assertTrue(json.contains(RoutineLoadJob.COMPUTE_GROUP), json);

        KafkaRoutineLoadJob restored = GsonUtils.GSON.fromJson(json, KafkaRoutineLoadJob.class);
        Assertions.assertEquals(DECLARED_CG, restored.getDeclaredComputeGroup());
        Assertions.assertEquals(DECLARED_CG, restored.getCloudCluster());
    }

    // Downgrade safety: metadata written without the key must still load and behave as before.
    @Test
    public void testMetadataRoundTripWithoutDeclaration() {
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, null);
        String json = GsonUtils.GSON.toJson(job);
        Assertions.assertFalse(json.contains(RoutineLoadJob.COMPUTE_GROUP), json);

        KafkaRoutineLoadJob restored = GsonUtils.GSON.fromJson(json, KafkaRoutineLoadJob.class);
        Assertions.assertNull(restored.getDeclaredComputeGroup());
        Assertions.assertEquals(SESSION_SNAPSHOT_CG, restored.getCloudCluster());
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

        Assertions.assertEquals(Lists.newArrayList(beInA1.getId(), beInA2.getId()), backendsForA);
        Assertions.assertEquals(Lists.newArrayList(beInB.getId()), backendsForB);
        Assertions.assertTrue(backendsForA.stream().noneMatch(backendsForB::contains),
                "jobs pinned to different compute groups must not share backends");
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

        Assertions.assertEquals(Lists.newArrayList(beInA.getId()), manager.getAvailableBackendIdsForTest(1L));
        Assertions.assertEquals(Lists.newArrayList(beInSession.getId()), manager.getAvailableBackendIdsForTest(2L));
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
        Assertions.assertEquals(Lists.newArrayList(beInA.getId()), manager.getAvailableBackendIdsForTest(1L));

        // what ALTER ROUTINE LOAD ... PROPERTIES("compute_group" = "cg_b") ends up doing
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put(RoutineLoadJob.COMPUTE_GROUP, "cg_b");

        Assertions.assertEquals(Lists.newArrayList(beInB.getId()), manager.getAvailableBackendIdsForTest(1L));
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
        Assertions.assertEquals("cg_dropped", info.getComputeGroupName());
        // Nor does metadata load wait for the group to start: nothing is sent to it from here.
        Assertions.assertTrue(cloudSystemInfo().autoStarted.isEmpty(), cloudSystemInfo().autoStarted.toString());
    }

    // Negative control for the test above: on the real CREATE path the same value must still be
    // rejected, otherwise the skip would have disabled validation everywhere.
    @Test
    public void testCreateStillRejectsComputeGroupThatDoesNotExist() {
        enterCloudMode(true, Lists.newArrayList("cg_live"));

        CreateRoutineLoadInfo info = newCreateInfo("cg_dropped");

        UserException e = Assertions.assertThrows(UserException.class, info::checkJobProperties);
        Assertions.assertTrue(e.getMessage().contains("Compute group 'cg_dropped' not found."),
                e.getMessage());
    }

    /**
     * Metadata load must not resolve the workload group either.
     *
     * <p>The workload group lookup resolves in the declared compute group
     * ({@code ConnectContext#getComputeGroup} → {@code ComputeGroupMgr#getComputeGroupByName}),
     * which throws once that group is gone, and {@link RoutineLoadJob#gsonPostProcess()} turns the
     * throw into the final CANCELLED. The stored statement is the CREATE one: a job moved from
     * cg_dropped to another group by ALTER still declares cg_dropped there, so dropping its old
     * group used to cancel it on the next restart.
     */
    @Test
    public void testMetadataLoadDoesNotResolveWorkloadGroupInDeclaredComputeGroup() throws UserException {
        enterCloudMode(true, Lists.newArrayList("cg_live"));
        // resolve compute groups through the stub, where cg_dropped has no cluster id
        Deencapsulation.setField(Env.getCurrentEnv(), "computeGroupMgr", new ComputeGroupMgr(cloudSystemInfo()));

        CreateRoutineLoadInfo info = newCreateInfo("cg_dropped", "wg_a");
        info.setReplay(true);

        info.checkJobProperties();

        Assertions.assertEquals("cg_dropped", info.getComputeGroupName());
        Assertions.assertEquals("wg_a", info.getWorkloadGroupName());
    }

    /**
     * Image load replays routine load jobs before any workload group exists.
     *
     * <p>{@code PersistMetaModules} reads {@code routineLoadJobs} before {@code workloadGroups}, and
     * until {@code Env#loadWorkloadGroups} runs, the Env holds the empty WorkloadGroupMgr its
     * constructor created. So the lookup above cannot find the group on image load even when no
     * compute group is declared: a job created with a workload_group was cancelled by the first
     * restart that loaded it from an image. This runs in non-cloud mode, where the replay context
     * (ADMIN) looks the group up under the resource tag of every backend.
     */
    @Test
    public void testImageLoadAdoptsWorkloadGroupBeforeWorkloadGroupsAreLoaded() throws UserException {
        List<String> modules = PersistMetaModules.MODULE_NAMES;
        Assertions.assertTrue(modules.indexOf("routineLoadJobs") < modules.indexOf("workloadGroups"),
                modules.toString());
        mockAccessManager(true);
        // the backends module is loaded before routineLoadJobs, the workload groups are not
        SystemInfoService systemInfo = new SystemInfoService();
        systemInfo.addBackend(createBackend(10001L));
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfo);
        Deencapsulation.setField(Env.getCurrentEnv(), "computeGroupMgr", new ComputeGroupMgr(systemInfo));
        Deencapsulation.setField(Env.getCurrentEnv(), "workloadGroupMgr", new WorkloadGroupMgr());
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.setThreadLocalInfo();

        CreateRoutineLoadInfo info = newCreateInfo(null, "wg_a");
        info.setReplay(true);
        info.checkJobProperties();
        Assertions.assertEquals("wg_a", info.getWorkloadGroupName());

        // negative control: a CREATE naming a workload group that does not exist is still rejected
        UserException e = Assertions.assertThrows(UserException.class,
                newCreateInfo(null, "wg_a")::checkJobProperties);
        Assertions.assertTrue(e.getMessage().contains("Can not find workload group wg_a"), e.getMessage());
    }

    /**
     * CREATE wakes the declared group, as {@code USE @<compute group>} does after the same checks.
     *
     * <p>With custom partitions, CREATE sends its Kafka metadata request through the declared
     * group's backends while the statement runs. When that group is suspended and the session runs
     * on another one, nothing else would start it, and the CREATE failed for lack of a backend.
     */
    @Test
    public void testCreateWakesDeclaredComputeGroup() throws UserException {
        enterCloudMode(true, Lists.newArrayList("cg_live"));

        newCreateInfo("cg_live").checkJobProperties();

        Assertions.assertEquals(Lists.newArrayList("cg_live"), cloudSystemInfo().autoStarted);
    }

    // ALTER can move a job to a suspended group, and converts the new offsets in it right away.
    @Test
    public void testAlterWakesDeclaredComputeGroup() {
        enterCloudMode(true, Lists.newArrayList("cg_live"));
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.COMPUTE_GROUP, "cg_live");
        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(new LabelNameInfo("test_db", "test_job"),
                jobProperties, Maps.newHashMap());

        Deencapsulation.invoke(command, "checkJobProperties", ConnectContext.get());

        Assertions.assertEquals("cg_live", command.getAnalyzedJobProperties().get(CreateRoutineLoadInfo.COMPUTE_GROUP));
        Assertions.assertEquals(Lists.newArrayList("cg_live"), cloudSystemInfo().autoStarted);
    }

    /**
     * {@code ALTER ROUTINE LOAD ... PROPERTIES("compute_group" = "cg_live")
     * FROM KAFKA("kafka_partitions" = "0", "kafka_offsets" = "OFFSET_END")} converts the offsets
     * before the new properties are installed, so the conversion has to go to cg_live already: the
     * job's current group may be gone, which is why it is being moved.
     */
    @Test
    public void testCombinedAlterConvertsOffsetsInTheGroupItDeclares() {
        enterCloudMode(true, Lists.newArrayList("cg_live"));
        KafkaRoutineLoadJob job = newJob(SESSION_SNAPSHOT_CG, "cg_dropped");
        // left RUNNING, so modifyProperties stops at its PAUSED check right after the conversion
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.RUNNING);

        List<String> conversionGroups = Lists.newArrayList();
        try (MockedStatic<KafkaUtil> kafkaUtil = Mockito.mockStatic(KafkaUtil.class)) {
            kafkaUtil.when(() -> KafkaUtil.getRealOffsets(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
                        conversionGroups.add(invocation.getArgument(4));
                        return invocation.getArgument(3);
                    });

            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> job.modifyProperties(newOffsetAlter("cg_live")));
            Assertions.assertTrue(e.getMessage().contains("Only supports modification of PAUSED jobs"),
                    e.getMessage());
            // an ALTER that declares no group keeps converting in the job's current one
            Assertions.assertThrows(DdlException.class, () -> job.modifyProperties(newOffsetAlter(null)));
        }

        Assertions.assertEquals(Lists.newArrayList("cg_live", "cg_dropped"), conversionGroups);
    }

    // What AlterRoutineLoadCommand#validate leaves behind for "kafka_partitions" = "0",
    // "kafka_offsets" = "OFFSET_END", optionally with a compute_group.
    private AlterRoutineLoadCommand newOffsetAlter(String declaredComputeGroup) {
        Map<String, String> analyzedJobProperties = Maps.newHashMap();
        if (declaredComputeGroup != null) {
            analyzedJobProperties.put(CreateRoutineLoadInfo.COMPUTE_GROUP, declaredComputeGroup);
        }
        KafkaDataSourceProperties dataSourceProperties = new KafkaDataSourceProperties(Maps.newHashMap());
        dataSourceProperties.setKafkaPartitionOffsets(Lists.newArrayList(Pair.of(0, KafkaProgress.OFFSET_END_VAL)));
        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(new LabelNameInfo("test_db", "test_job"),
                Maps.newHashMap(), Maps.newHashMap());
        Deencapsulation.setField(command, "analyzedJobProperties", analyzedJobProperties);
        Deencapsulation.setField(command, "dataSourceProperties", dataSourceProperties);
        return command;
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
            Assertions.fail("scheduling a task of a job pinned to a dropped compute group must fail,"
                    + " last pause reason: " + job.pauseMsg);
        } catch (Exception expected) {
            // scheduleOneTask pauses the job and rethrows
        }

        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.newState);
        Assertions.assertNotNull(job.pauseMsg, "the job must be paused with a reason");
        Assertions.assertTrue(job.pauseMsg.contains("Compute group 'cg_dropped' not found."), job.pauseMsg);
        Assertions.assertFalse(job.pauseMsg.contains("no available BE found"),
                "the operator must not be sent to look at BE status for a dropped group");

        // A group can come back by itself - one that is only scaled to zero backends leaves the
        // cluster map and re-enters it when it scales up - so this pause has to stay retryable.
        Assertions.assertNotEquals(InternalErrorCode.CANNOT_RESUME_ERR, job.pauseCode);
        Assertions.assertTrue(ScheduleRule.isNeedAutoSchedule(job),
                "a dropped group must still auto resume once it is back");
    }

    /**
     * RESUME leaves the job in NEED_SCHEDULE, and the scheduler refreshes the topic's partitions before it creates
     * any task. That metadata request goes to the backends of the declared group, so it runs the same check first.
     */
    @Test
    public void testDroppedComputeGroupPausesJobBeforeKafkaMetadataRequest() throws UserException {
        enterCloudMode(true, Lists.newArrayList("cg_live"));

        RecordingKafkaRoutineLoadJob job = new RecordingKafkaRoutineLoadJob();
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        Deencapsulation.setField(job, "userIdentity", UserIdentity.ADMIN);
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(RoutineLoadJob.COMPUTE_GROUP, "cg_dropped");
        Deencapsulation.setField(job, "jobProperties", jobProperties);

        try (MockedStatic<KafkaUtil> kafkaUtil = Mockito.mockStatic(KafkaUtil.class)) {
            // what the request reports for a group without backends
            kafkaUtil.when(() -> KafkaUtil.getAllKafkaPartitions(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any())).thenThrow(new LoadException("failed to get info: no alive backends"));

            RoutineLoadJob routineLoadJob = job;
            Assertions.assertFalse(routineLoadJob.refreshKafkaPartitions(false));

            kafkaUtil.verifyNoInteractions();
        }
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.newState);
        Assertions.assertTrue(job.pauseMsg.contains("Compute group 'cg_dropped' not found."), job.pauseMsg);
        Assertions.assertFalse(job.pauseMsg.contains("Kafka properties"),
                "the operator must not be sent to check the Kafka properties for a dropped group");
        Assertions.assertTrue(ScheduleRule.isNeedAutoSchedule(job),
                "a dropped group must still auto resume once it is back");
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
            Assertions.fail("a task whose owner lost USAGE on the pinned group must fail");
        } catch (Exception expected) {
            // scheduleOneTask pauses the job and rethrows
        }

        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.newState);
        Assertions.assertNotNull(job.pauseMsg, "the job must be paused with a reason");
        Assertions.assertTrue(job.pauseMsg.contains("USAGE denied"), job.pauseMsg);
        Assertions.assertEquals(InternalErrorCode.CANNOT_RESUME_ERR, job.pauseCode);
        Assertions.assertFalse(ScheduleRule.isNeedAutoSchedule(job),
                "a revoked privilege must not be auto resumed");
    }

    /**
     * update() refreshes the partitions before it takes the job lock, which a SQL STOP holds while it
     * runs. A pause found by the refresh has to wait for that lock and look at the state again: a STOP
     * that completed in between must stay STOPPED, as replay rejects STOPPED -> PAUSED.
     */
    @Test
    public void testRevokedUsagePauseInPartitionRefreshKeepsCompletedStop() throws Exception {
        // the group exists and its metadata request would succeed; only the owner's USAGE is gone
        enterCloudMode(false, Lists.newArrayList("cg_pinned"));

        RecordingKafkaRoutineLoadJob job = new RecordingKafkaRoutineLoadJob();
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        Deencapsulation.setField(job, "userIdentity", UserIdentity.ADMIN);
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(RoutineLoadJob.COMPUTE_GROUP, "cg_pinned");
        Deencapsulation.setField(job, "jobProperties", jobProperties);

        Assertions.assertFalse(refreshWhileStopHoldsJobLock(job));

        Assertions.assertNull(job.newState, "a completed STOP must not be turned back into PAUSED");
        Assertions.assertEquals(RoutineLoadJob.JobState.STOPPED, job.getState());
    }

    @Test
    public void testKafkaMetadataPauseInPartitionRefreshKeepsCompletedStop() throws Exception {
        RecordingKafkaRoutineLoadJob job = new RecordingKafkaRoutineLoadJob();
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);

        try (MockedStatic<KafkaUtil> kafkaUtil = Mockito.mockStatic(KafkaUtil.class)) {
            kafkaUtil.when(() -> KafkaUtil.getAllKafkaPartitions(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any())).thenThrow(new LoadException("failed to get info"));

            Assertions.assertFalse(refreshWhileStopHoldsJobLock(job));
        }

        Assertions.assertNull(job.newState, "a completed STOP must not be turned back into PAUSED");
        Assertions.assertEquals(RoutineLoadJob.JobState.STOPPED, job.getState());
    }

    @Test
    public void testKinesisShardPauseInRefreshKeepsCompletedStop() throws Exception {
        RecordingKinesisRoutineLoadJob job = new RecordingKinesisRoutineLoadJob();
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);

        try (MockedStatic<KinesisUtil> kinesisUtil = Mockito.mockStatic(KinesisUtil.class)) {
            kinesisUtil.when(() -> KinesisUtil.getAllKinesisShards(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any())).thenThrow(new LoadException("failed to list shards"));

            Assertions.assertFalse(refreshWhileStopHoldsJobLock(job));
        }

        Assertions.assertNull(job.newState, "a completed STOP must not be turned back into PAUSED");
        Assertions.assertEquals(RoutineLoadJob.JobState.STOPPED, job.getState());
    }

    /**
     * Runs the partition refresh on this thread while another thread holds the job lock, as a SQL STOP
     * does, and completes that STOP once the refresh waits for the lock. A refresh that changes the
     * state without the lock never waits for it, and the STOP gives up after ten seconds.
     */
    private static boolean refreshWhileStopHoldsJobLock(RoutineLoadJob job) throws Exception {
        Thread refresher = Thread.currentThread();
        CountDownLatch stopHoldsLock = new CountDownLatch(1);
        AtomicBoolean refreshWaitedForLock = new AtomicBoolean();
        Thread stop = new Thread(() -> {
            job.writeLock();
            try {
                stopHoldsLock.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!job.lock.hasQueuedThread(refresher) && System.nanoTime() < deadline) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                refreshWaitedForLock.set(job.lock.hasQueuedThread(refresher));
                // what executeStop() leaves behind
                job.state = RoutineLoadJob.JobState.STOPPED;
            } finally {
                job.writeUnlock();
            }
        });
        stop.start();
        stopHoldsLock.await();
        boolean refreshed = job.refreshKafkaPartitions(false);
        stop.join();
        Assertions.assertTrue(refreshWaitedForLock.get(), "the pause must wait for the job lock");
        return refreshed;
    }

    private CreateRoutineLoadInfo newCreateInfo(String declaredComputeGroup) {
        return newCreateInfo(declaredComputeGroup, null);
    }

    private CreateRoutineLoadInfo newCreateInfo(String declaredComputeGroup, String workloadGroup) {
        Map<String, String> jobProperties = Maps.newHashMap();
        if (declaredComputeGroup != null) {
            jobProperties.put(CreateRoutineLoadInfo.COMPUTE_GROUP, declaredComputeGroup);
        }
        if (workloadGroup != null) {
            jobProperties.put(CreateRoutineLoadInfo.WORKLOAD_GROUP, workloadGroup);
        }
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
        protected boolean hasMoreDataToConsume() {
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

        // the partition refresh changes the state under a lock it takes itself
        @Override
        protected void unprotectUpdateState(JobState jobState, ErrorReason reason, boolean isReplay) {
            updateState(jobState, reason, isReplay);
        }
    }

    private static class RecordingKinesisRoutineLoadJob extends KinesisRoutineLoadJob {
        private JobState newState;

        private RecordingKinesisRoutineLoadJob() {
            super(1L, "kinesis_routine_load_job", 1L, 1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        }

        @Override
        protected void unprotectUpdateState(JobState jobState, ErrorReason reason, boolean isReplay) {
            this.newState = jobState;
            this.state = jobState;
        }
    }

    private static class ClusterAwareCloudSystemInfoService extends CloudSystemInfoService {
        private final Map<String, List<Backend>> clusterToBackends;
        // every group asked to start, in call order; the real call resumes a suspended group
        private final List<String> autoStarted = Lists.newArrayList();

        private ClusterAwareCloudSystemInfoService(Map<String, List<Backend>> clusterToBackends) {
            this.clusterToBackends = clusterToBackends;
        }

        @Override
        public String waitForAutoStart(String clusterName) {
            autoStarted.add(clusterName);
            return clusterName;
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
