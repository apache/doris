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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.ComputeGroup;
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
        //infoService = new CloudSystemInfoService();
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
}
