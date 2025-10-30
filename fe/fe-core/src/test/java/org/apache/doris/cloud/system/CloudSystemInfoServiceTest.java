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

package org.apache.doris.cloud.system;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.ComputeGroup;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CloudSystemInfoServiceTest {
    private CloudSystemInfoService infoService;

    @Before
    public void setUp() {
        // Enable cloud mode for testing
        Config.cloud_unique_id = "test_cloud_unique_id";
    }

    @Test
    public void testGetPhysicalClusterNotExist() {
        infoService = new CloudSystemInfoService();
        // not exist cluster
        String c1 = "not_exist_cluster_1";
        String res = infoService.getPhysicalCluster(c1);
        Assert.assertEquals(c1, res);
    }

    @Test
    public void testGetPhysicalClusterPhysicalCluster() {
        infoService = new CloudSystemInfoService();
        String c1 = "physical_cluster_1";
        String res = infoService.getPhysicalCluster(c1);
        Assert.assertEquals(c1, res);
    }

    // virtual cluster does not contain physical cluster
    //@Test
    //public void testGetPhysicalClusterEmptyVirtualCluster() {
    //    infoService = new CloudSystemInfoService();
    //    String vcgName = "v_cluster_1";
    //    ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
    //    infoService.addComputeGroup(vcgName, vcg);

    //    String res = infoService.getPhysicalCluster(vcgName);
    //    Assert.assertEquals(vcgName, res);
    //}

    // active and standby are both empty cluster
    @Test
    public void testGetPhysicalClusterEmptyCluster() {
        infoService = new CloudSystemInfoService();
        String vcgName = "v_cluster_1";
        String pcgName1 = "p_cluster_1";
        String pcgName2 = "p_cluster_2";

        ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(pcgName1);
        policy.setStandbyComputeGroup(pcgName2);
        vcg.setPolicy(policy);

        ComputeGroup pcg1 = new ComputeGroup("id2", pcgName1, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg2 = new ComputeGroup("id3", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(vcgName, vcg);
        infoService.addComputeGroup(pcgName1, pcg1);
        infoService.addComputeGroup(pcgName2, pcg2);

        String res = infoService.getPhysicalCluster(vcgName);
        Assert.assertEquals(pcgName1, res);
    }

    // active is empty cluster and standby has 3 alive be
    @Test
    public void testGetPhysicalClusterStandbyAvailable() {
        infoService = new CloudSystemInfoService();

        String vcgName = "v_cluster_1";
        String pcgName1 = "p_cluster_1";
        String pcgName2 = "p_cluster_2";

        ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(pcgName1);
        policy.setStandbyComputeGroup(pcgName2);
        vcg.setPolicy(policy);

        ComputeGroup pcg1 = new ComputeGroup("id2", pcgName1, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg2 = new ComputeGroup("id3", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(vcgName, vcg);
        infoService.addComputeGroup(pcgName1, pcg1);
        infoService.addComputeGroup(pcgName2, pcg2);

        List<Backend> toAdd = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName2);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id3");
            b.setTagMap(newTagMap);
            b.setAlive(true);
            toAdd.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        String res = infoService.getPhysicalCluster(vcgName);
        Assert.assertEquals(pcgName2, res);
    }

    // active has 3 alive be and standby is empty cluster
    @Test
    public void testGetPhysicalClusterActiveAvailable() {
        infoService = new CloudSystemInfoService();

        String vcgName = "v_cluster_1";
        String pcgName1 = "p_cluster_1";
        String pcgName2 = "p_cluster_2";

        ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(pcgName1);
        policy.setStandbyComputeGroup(pcgName2);
        vcg.setPolicy(policy);

        ComputeGroup pcg1 = new ComputeGroup("id2", pcgName1, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg2 = new ComputeGroup("id3", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(vcgName, vcg);
        infoService.addComputeGroup(pcgName1, pcg1);
        infoService.addComputeGroup(pcgName2, pcg2);

        List<Backend> toAdd = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName1);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id2");
            b.setTagMap(newTagMap);
            b.setAlive(true);
            toAdd.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        String res = infoService.getPhysicalCluster(vcgName);
        Assert.assertEquals(pcgName1, res);
    }

    // active has 3 alive be and standby has 3 dead be
    @Test
    public void testGetPhysicalClusterActive3AliveBe() {
        infoService = new CloudSystemInfoService();

        String vcgName = "v_cluster_1";
        String pcgName1 = "p_cluster_1";
        String pcgName2 = "p_cluster_2";

        ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(pcgName1);
        policy.setStandbyComputeGroup(pcgName2);
        vcg.setPolicy(policy);

        ComputeGroup pcg1 = new ComputeGroup("id2", pcgName1, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg2 = new ComputeGroup("id3", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(vcgName, vcg);
        infoService.addComputeGroup(pcgName1, pcg1);
        infoService.addComputeGroup(pcgName2, pcg2);

        List<Backend> toAdd1 = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName1);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id2");
            b.setTagMap(newTagMap);
            b.setAlive(true);
            toAdd1.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd1, new ArrayList<>());

        List<Backend> toAdd2 = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName2);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id3");
            b.setTagMap(newTagMap);
            b.setAlive(false);
            toAdd2.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd2, new ArrayList<>());

        String res = infoService.getPhysicalCluster(vcgName);
        Assert.assertEquals(pcgName1, res);
    }

    // active has 3 dead be and standby has 3 alive be
    @Test
    public void testGetPhysicalClusterStandby3AliveBe() {
        infoService = new CloudSystemInfoService();

        String vcgName = "v_cluster_1";
        String pcgName1 = "p_cluster_1";
        String pcgName2 = "p_cluster_2";

        ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(pcgName1);
        policy.setStandbyComputeGroup(pcgName2);
        vcg.setPolicy(policy);

        ComputeGroup pcg1 = new ComputeGroup("id2", pcgName1, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg2 = new ComputeGroup("id3", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(vcgName, vcg);
        infoService.addComputeGroup(pcgName1, pcg1);
        infoService.addComputeGroup(pcgName2, pcg2);

        List<Backend> toAdd1 = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName1);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id2");
            b.setTagMap(newTagMap);
            b.setAlive(false);
            toAdd1.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd1, new ArrayList<>());

        List<Backend> toAdd2 = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName2);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id3");
            b.setTagMap(newTagMap);
            b.setAlive(true);
            toAdd2.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd2, new ArrayList<>());

        String res = infoService.getPhysicalCluster(vcgName);
        Assert.assertEquals(pcgName2, res);
    }

    // active has 1 alive be and 2 dead be, standby has 3 alive be
    @Test
    public void testGetPhysicalClusterActive1AliveBe2DeadBe() {
        infoService = new CloudSystemInfoService();

        String vcgName = "v_cluster_1";
        String pcgName1 = "p_cluster_1";
        String pcgName2 = "p_cluster_2";

        ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(pcgName1);
        policy.setStandbyComputeGroup(pcgName2);
        vcg.setPolicy(policy);

        ComputeGroup pcg1 = new ComputeGroup("id2", pcgName1, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg2 = new ComputeGroup("id3", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(vcgName, vcg);
        infoService.addComputeGroup(pcgName1, pcg1);
        infoService.addComputeGroup(pcgName2, pcg2);

        List<Backend> toAdd1 = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName1);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id2");
            b.setTagMap(newTagMap);
            if (i == 2) {
                b.setAlive(true);
            } else {
                b.setAlive(false);
            }
            toAdd1.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd1, new ArrayList<>());

        List<Backend> toAdd2 = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), "", i);
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, pcgName2);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, "id3");
            b.setTagMap(newTagMap);
            b.setAlive(true);
            toAdd2.add(b);
        }
        infoService.updateCloudClusterMapNoLock(toAdd2, new ArrayList<>());

        String res = infoService.getPhysicalCluster(vcgName);
        Assert.assertEquals(pcgName1, res);
    }

    @Test
    public void testIsStandByComputeGroup() {
        infoService = new CloudSystemInfoService();

        String vcgName = "v_cluster_1";
        String pcgName1 = "p_cluster_1";
        String pcgName2 = "p_cluster_2";
        String pcgName3 = "p_cluster_3";

        ComputeGroup vcg = new ComputeGroup("id1", vcgName, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(pcgName1);
        policy.setStandbyComputeGroup(pcgName2);
        vcg.setPolicy(policy);

        ComputeGroup pcg1 = new ComputeGroup("id2", pcgName1, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg2 = new ComputeGroup("id3", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        ComputeGroup pcg3 = new ComputeGroup("id4", pcgName2, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(vcgName, vcg);
        infoService.addComputeGroup(pcgName1, pcg1);
        infoService.addComputeGroup(pcgName2, pcg2);
        infoService.addComputeGroup(pcgName3, pcg3);

        boolean res = infoService.isStandByComputeGroup(vcgName);
        Assert.assertFalse(res);
        res = infoService.isStandByComputeGroup(pcgName1);
        Assert.assertFalse(res);
        res = infoService.isStandByComputeGroup(pcgName2);
        Assert.assertTrue(res);
        res = infoService.isStandByComputeGroup(pcgName3);
        Assert.assertFalse(res);
    }

    // Test for getMinPipelineExecutorSize method
    @Test
    public void testGetMinPipelineExecutorSizeWithEmptyCluster() {
        infoService = new CloudSystemInfoService();
        String clusterName = "test_cluster";
        String clusterId = "test_cluster_id";

        // Mock an empty cluster (no backends)
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Since there are no backends in the cluster, should return 1
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(1, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithSingleBackend() {
        infoService = new CloudSystemInfoService();
        String clusterName = "test_cluster";
        String clusterId = "test_cluster_id";

        // Setup cluster
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Add a backend with pipeline executor size = 8
        List<Backend> toAdd = new ArrayList<>();
        Backend backend = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.1", 9050);
        Map<String, String> tagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend.setTagMap(tagMap);
        backend.setPipelineExecutorSize(8);
        toAdd.add(backend);

        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Should return the pipeline executor size of the single backend
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(8, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithMultipleBackends() {
        infoService = new CloudSystemInfoService();
        String clusterName = "test_cluster";
        String clusterId = "test_cluster_id";

        // Setup cluster
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Add multiple backends with different pipeline executor sizes
        List<Backend> toAdd = new ArrayList<>();

        Backend backend1 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend1.setTagMap(tagMap1);
        backend1.setPipelineExecutorSize(12);
        toAdd.add(backend1);

        Backend backend2 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend2.setTagMap(tagMap2);
        backend2.setPipelineExecutorSize(6); // This should be the minimum
        toAdd.add(backend2);

        Backend backend3 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.3", 9050);
        Map<String, String> tagMap3 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap3.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap3.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend3.setTagMap(tagMap3);
        backend3.setPipelineExecutorSize(10);
        toAdd.add(backend3);

        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Should return the minimum pipeline executor size (6)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(6, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithZeroSizeBackends() {
        infoService = new CloudSystemInfoService();
        String clusterName = "test_cluster";
        String clusterId = "test_cluster_id";

        // Setup cluster
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Add backends with zero and positive pipeline executor sizes
        List<Backend> toAdd = new ArrayList<>();

        Backend backend1 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend1.setTagMap(tagMap1);
        backend1.setPipelineExecutorSize(0); // Should be ignored
        toAdd.add(backend1);

        Backend backend2 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend2.setTagMap(tagMap2);
        backend2.setPipelineExecutorSize(4); // This should be the minimum
        toAdd.add(backend2);

        Backend backend3 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.3", 9050);
        Map<String, String> tagMap3 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap3.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap3.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend3.setTagMap(tagMap3);
        backend3.setPipelineExecutorSize(-1); // Should be ignored
        toAdd.add(backend3);

        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Should return the minimum positive pipeline executor size (4)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(4, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithAllZeroSizeBackends() {
        infoService = new CloudSystemInfoService();
        String clusterName = "test_cluster";
        String clusterId = "test_cluster_id";

        // Setup cluster
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Add backends with only zero or negative pipeline executor sizes
        List<Backend> toAdd = new ArrayList<>();

        Backend backend1 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend1.setTagMap(tagMap1);
        backend1.setPipelineExecutorSize(0);
        toAdd.add(backend1);

        Backend backend2 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend2.setTagMap(tagMap2);
        backend2.setPipelineExecutorSize(-1);
        toAdd.add(backend2);

        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Should return 1 when no valid pipeline executor sizes are
            // found
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(1, result);
        } finally {
            ConnectContext.remove();
        }
    }

    // Test for error handling when ConnectContext has no cluster set
    @Test
    public void testGetMinPipelineExecutorSizeWithNoClusterInContext() {
        infoService = new CloudSystemInfoService();

        // Create ConnectContext but don't set any cluster (empty cluster name)
        createTestConnectContext(null);
        try {
            // Should return 1 when no cluster is set in ConnectContext
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(1, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithMixedValidInvalidBackends() {
        infoService = new CloudSystemInfoService();
        String clusterName = "mixed_cluster";
        String clusterId = "mixed_cluster_id";

        // Setup cluster
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Add backends with mixed valid and invalid pipeline executor sizes
        List<Backend> toAdd = new ArrayList<>();

        // Backend with valid size
        Backend backend1 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend1.setTagMap(tagMap1);
        backend1.setPipelineExecutorSize(16);
        toAdd.add(backend1);

        // Backend with zero size (should be ignored)
        Backend backend2 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend2.setTagMap(tagMap2);
        backend2.setPipelineExecutorSize(0);
        toAdd.add(backend2);

        // Backend with valid size (smaller than first)
        Backend backend3 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.3", 9050);
        Map<String, String> tagMap3 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap3.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap3.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend3.setTagMap(tagMap3);
        backend3.setPipelineExecutorSize(8); // This should be the minimum
        toAdd.add(backend3);

        // Backend with negative size (should be ignored)
        Backend backend4 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.4", 9050);
        Map<String, String> tagMap4 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap4.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap4.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend4.setTagMap(tagMap4);
        backend4.setPipelineExecutorSize(-5);
        toAdd.add(backend4);

        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Should return 8 (minimum valid size)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(8, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithLargeValues() {
        infoService = new CloudSystemInfoService();
        String clusterName = "large_cluster";
        String clusterId = "large_cluster_id";

        // Setup cluster
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Add backends with large pipeline executor sizes
        List<Backend> toAdd = new ArrayList<>();

        Backend backend1 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend1.setTagMap(tagMap1);
        backend1.setPipelineExecutorSize(1024);
        toAdd.add(backend1);

        Backend backend2 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend2.setTagMap(tagMap2);
        backend2.setPipelineExecutorSize(2048);
        toAdd.add(backend2);

        Backend backend3 = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0.3", 9050);
        Map<String, String> tagMap3 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap3.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
        tagMap3.put(Tag.CLOUD_CLUSTER_ID, clusterId);
        backend3.setTagMap(tagMap3);
        backend3.setPipelineExecutorSize(512); // This should be the minimum
        toAdd.add(backend3);

        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Should return 512 (minimum among large values)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(512, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeConsistency() {
        infoService = new CloudSystemInfoService();
        String clusterName = "consistency_cluster";
        String clusterId = "consistency_cluster_id";

        // Setup cluster
        ComputeGroup cg = new ComputeGroup(clusterId, clusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(clusterId, cg);

        // Add backends with same pipeline executor sizes
        List<Backend> toAdd = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Backend backend = new Backend(Env.getCurrentEnv().getNextId(), "127.0.0." + (i + 1), 9050);
            Map<String, String> tagMap = Tag.DEFAULT_BACKEND_TAG.toMap();
            tagMap.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
            tagMap.put(Tag.CLOUD_CLUSTER_ID, clusterId);
            backend.setTagMap(tagMap);
            backend.setPipelineExecutorSize(32); // All backends have same size
            toAdd.add(backend);
        }

        infoService.updateCloudClusterMapNoLock(toAdd, new ArrayList<>());

        // Set ConnectContext to select the cluster
        createTestConnectContext(clusterName);

        try {
            // Should return 32 (consistent across all backends)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(32, result);
        } finally {
            ConnectContext.remove();
        }
    }

    // Test for multiple compute groups - should only use current cluster
    @Test
    public void testGetMinPipelineExecutorSizeWithMultipleComputeGroups() {
        infoService = new CloudSystemInfoService();

        // Setup multiple clusters with different pipeline executor sizes
        String cluster1Name = "cluster1";
        String cluster1Id = "cluster1_id";
        String cluster2Name = "cluster2";
        String cluster2Id = "cluster2_id";

        // Setup cluster1
        ComputeGroup cg1 = new ComputeGroup(cluster1Id, cluster1Name, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(cluster1Id, cg1);

        // Setup cluster2
        ComputeGroup cg2 = new ComputeGroup(cluster2Id, cluster2Name, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(cluster2Id, cg2);

        // Add backends to cluster1 with smaller pipeline executor sizes
        List<Backend> cluster1Backends = new ArrayList<>();
        Backend cluster1Backend1 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.1.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, cluster1Name);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, cluster1Id);
        cluster1Backend1.setTagMap(tagMap1);
        cluster1Backend1.setPipelineExecutorSize(4); // Smaller than current cluster
        cluster1Backends.add(cluster1Backend1);

        Backend cluster1Backend2 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.1.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, cluster1Name);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, cluster1Id);
        cluster1Backend2.setTagMap(tagMap2);
        cluster1Backend2.setPipelineExecutorSize(2); // Smallest overall
        cluster1Backends.add(cluster1Backend2);

        infoService.updateCloudClusterMapNoLock(cluster1Backends, new ArrayList<>());

        // Add backends to cluster2
        List<Backend> cluster2Backends = new ArrayList<>();
        Backend cluster2Backend1 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.2.1", 9050);
        Map<String, String> tagMap3 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap3.put(Tag.CLOUD_CLUSTER_NAME, cluster2Name);
        tagMap3.put(Tag.CLOUD_CLUSTER_ID, cluster2Id);
        cluster2Backend1.setTagMap(tagMap3);
        cluster2Backend1.setPipelineExecutorSize(8);
        cluster2Backends.add(cluster2Backend1);

        Backend cluster2Backend2 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.2.2", 9050);
        Map<String, String> tagMap4 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap4.put(Tag.CLOUD_CLUSTER_NAME, cluster2Name);
        tagMap4.put(Tag.CLOUD_CLUSTER_ID, cluster2Id);
        cluster2Backend2.setTagMap(tagMap4);
        cluster2Backend2.setPipelineExecutorSize(12);
        cluster2Backends.add(cluster2Backend2);

        infoService.updateCloudClusterMapNoLock(cluster2Backends, new ArrayList<>());

        // Set ConnectContext to cluster2 to test that only cluster2 backends are used
        createTestConnectContext(cluster2Name);

        try {
            // Should return 8 (minimum from current cluster2), not 2 (global minimum from cluster1)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(8, result);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithVirtualComputeGroup() {
        infoService = new CloudSystemInfoService();

        // Setup virtual and physical clusters
        String virtualClusterName = "virtual_cluster";
        String virtualClusterId = "virtual_cluster_id";
        String physicalClusterName = "physical_cluster";
        String physicalClusterId = "physical_cluster_id";
        String otherClusterName = "other_cluster";
        String otherClusterId = "other_cluster_id";

        // Setup virtual cluster
        ComputeGroup virtualCg = new ComputeGroup(virtualClusterId, virtualClusterName,
                ComputeGroup.ComputeTypeEnum.VIRTUAL);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(physicalClusterName);
        virtualCg.setPolicy(policy);
        infoService.addComputeGroup(virtualClusterId, virtualCg);

        // Setup physical cluster
        ComputeGroup physicalCg = new ComputeGroup(physicalClusterId, physicalClusterName,
                ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(physicalClusterId, physicalCg);

        // Setup other cluster
        ComputeGroup otherCg = new ComputeGroup(otherClusterId, otherClusterName, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(otherClusterId, otherCg);

        // Add backends to physical cluster
        List<Backend> physicalBackends = new ArrayList<>();
        Backend physicalBackend1 = new Backend(Env.getCurrentEnv().getNextId(), "172.16.1.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, physicalClusterName);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, physicalClusterId);
        physicalBackend1.setTagMap(tagMap1);
        physicalBackend1.setPipelineExecutorSize(32); // Min in physical cluster
        physicalBackends.add(physicalBackend1);

        Backend physicalBackend2 = new Backend(Env.getCurrentEnv().getNextId(), "172.16.1.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, physicalClusterName);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, physicalClusterId);
        physicalBackend2.setTagMap(tagMap2);
        physicalBackend2.setPipelineExecutorSize(48);
        physicalBackends.add(physicalBackend2);

        infoService.updateCloudClusterMapNoLock(physicalBackends, new ArrayList<>());

        // Add backends to other cluster with smaller values
        List<Backend> otherBackends = new ArrayList<>();
        Backend otherBackend = new Backend(Env.getCurrentEnv().getNextId(), "10.0.3.1", 9050);
        Map<String, String> tagMap3 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap3.put(Tag.CLOUD_CLUSTER_NAME, otherClusterName);
        tagMap3.put(Tag.CLOUD_CLUSTER_ID, otherClusterId);
        otherBackend.setTagMap(tagMap3);
        otherBackend.setPipelineExecutorSize(8); // Smaller than virtual cluster's physical cluster
        otherBackends.add(otherBackend);

        infoService.updateCloudClusterMapNoLock(otherBackends, new ArrayList<>());

        // Create ConnectContext and set it to select virtual cluster
        ConnectContext ctx = createTestConnectContext(virtualClusterName);

        try {
            // Should return 32 (minimum from virtual cluster's physical cluster), not 8
            // (from other cluster)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(32, result);

            // Switch to other cluster
            ctx.setCloudCluster(otherClusterName);

            // Should return 8 (from other cluster)
            result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(8, result);

        } finally {
            // Clean up ConnectContext
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetMinPipelineExecutorSizeWithConnectContextNoCluster() {
        infoService = new CloudSystemInfoService();

        // Create ConnectContext but don't set any cluster
        createTestConnectContext(null); // null to test no cluster scenario

        try {
            // Should return 1 because no cluster is set (will catch AnalysisException)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(1, result);

        } finally {
            // Clean up ConnectContext
            ConnectContext.remove();
        }
    }

    // Test using real ConnectContext to select compute group
    @Test
    public void testGetMinPipelineExecutorSizeWithConnectContext() {
        infoService = new CloudSystemInfoService();

        // Setup multiple clusters with different pipeline executor sizes
        String cluster1Name = "ctx_cluster1";
        String cluster1Id = "ctx_cluster1_id";
        String cluster2Name = "ctx_cluster2";
        String cluster2Id = "ctx_cluster2_id";

        // Setup cluster1
        ComputeGroup cg1 = new ComputeGroup(cluster1Id, cluster1Name, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(cluster1Id, cg1);

        // Setup cluster2
        ComputeGroup cg2 = new ComputeGroup(cluster2Id, cluster2Name, ComputeGroup.ComputeTypeEnum.COMPUTE);
        infoService.addComputeGroup(cluster2Id, cg2);

        // Add backends to cluster1 with smaller pipeline executor sizes
        List<Backend> cluster1Backends = new ArrayList<>();
        Backend cluster1Backend1 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.1.1", 9050);
        Map<String, String> tagMap1 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap1.put(Tag.CLOUD_CLUSTER_NAME, cluster1Name);
        tagMap1.put(Tag.CLOUD_CLUSTER_ID, cluster1Id);
        cluster1Backend1.setTagMap(tagMap1);
        cluster1Backend1.setPipelineExecutorSize(4);
        cluster1Backends.add(cluster1Backend1);

        Backend cluster1Backend2 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.1.2", 9050);
        Map<String, String> tagMap2 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap2.put(Tag.CLOUD_CLUSTER_NAME, cluster1Name);
        tagMap2.put(Tag.CLOUD_CLUSTER_ID, cluster1Id);
        cluster1Backend2.setTagMap(tagMap2);
        cluster1Backend2.setPipelineExecutorSize(2); // Smallest in cluster1
        cluster1Backends.add(cluster1Backend2);

        infoService.updateCloudClusterMapNoLock(cluster1Backends, new ArrayList<>());

        // Add backends to cluster2 with larger pipeline executor sizes
        List<Backend> cluster2Backends = new ArrayList<>();
        Backend cluster2Backend1 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.2.1", 9050);
        Map<String, String> tagMap3 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap3.put(Tag.CLOUD_CLUSTER_NAME, cluster2Name);
        tagMap3.put(Tag.CLOUD_CLUSTER_ID, cluster2Id);
        cluster2Backend1.setTagMap(tagMap3);
        cluster2Backend1.setPipelineExecutorSize(16); // Smallest in cluster2
        cluster2Backends.add(cluster2Backend1);

        Backend cluster2Backend2 = new Backend(Env.getCurrentEnv().getNextId(), "10.0.2.2", 9050);
        Map<String, String> tagMap4 = Tag.DEFAULT_BACKEND_TAG.toMap();
        tagMap4.put(Tag.CLOUD_CLUSTER_NAME, cluster2Name);
        tagMap4.put(Tag.CLOUD_CLUSTER_ID, cluster2Id);
        cluster2Backend2.setTagMap(tagMap4);
        cluster2Backend2.setPipelineExecutorSize(24);
        cluster2Backends.add(cluster2Backend2);

        infoService.updateCloudClusterMapNoLock(cluster2Backends, new ArrayList<>());

        // Create ConnectContext and set it to select cluster1
        ConnectContext ctx = createTestConnectContext(cluster1Name);

        try {
            // Should return 2 (minimum from cluster1), not 16 (minimum from cluster2)
            int result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(2, result);

            // Now switch to cluster2
            ctx.setCloudCluster(cluster2Name);

            // Should return 16 (minimum from cluster2), not 2 (minimum from cluster1)
            result = infoService.getMinPipelineExecutorSize();
            Assert.assertEquals(16, result);
        } finally {
            // Clean up ConnectContext
            ConnectContext.remove();
        }
    }

    /**
     * Helper method to create a test ConnectContext with specific cluster name
     */
    private ConnectContext createTestConnectContext(String clusterName) {
        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(UserIdentity.ROOT);
            ctx.setRemoteIP("127.0.0.1");
            ctx.setEnv(Env.getCurrentEnv());
            if (clusterName != null) {
                ctx.setCloudCluster(clusterName);
            }
            ctx.setThreadLocalInfo();
            return ctx;
        } catch (Throwable t) {
            throw new IllegalStateException("Cannot create test connect context", t);
        }
    }
}
