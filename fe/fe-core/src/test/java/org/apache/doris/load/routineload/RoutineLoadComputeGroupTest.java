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
import org.apache.doris.cloud.load.CloudRoutineLoadManager;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
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

    @Before
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalSystemInfo = Env.getCurrentSystemInfo();
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfo);
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

    private Backend createBackend(long id) {
        Backend backend = new Backend(id, "127.0.0." + id, 9050);
        backend.setAlive(true);
        return backend;
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

        private List<Long> getAvailableBackendIdsForTest(long jobId) throws LoadException {
            return super.getAvailableBackendIds(jobId);
        }
    }
}
