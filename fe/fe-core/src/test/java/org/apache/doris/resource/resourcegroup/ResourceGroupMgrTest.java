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

package org.apache.doris.resource.resourcegroup;

import org.apache.doris.analysis.CreateResourceGroupStmt;
import org.apache.doris.analysis.DropResourceGroupStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TPipelineResourceGroup;

import com.google.common.collect.Maps;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ResourceGroupMgrTest {

    @Injectable
    private EditLog editLog;

    @Mocked
    private Env env;

    private AtomicLong id = new AtomicLong(10);

    @Before
    public void setUp() throws DdlException {
        new Expectations() {
            {
                env.getEditLog();
                minTimes = 0;
                result = editLog;

                env.getNextId();
                minTimes = 0;
                result = new Delegate() {
                    long delegate() {
                        return id.addAndGet(1);
                    }
                };

                editLog.logCreateResourceGroup((ResourceGroup) any);
                minTimes = 0;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };
    }

    @Test
    public void testCreateResourceGroup() throws DdlException {
        Config.enable_resource_group = true;
        ResourceGroupMgr resourceGroupMgr = new ResourceGroupMgr();
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(ResourceGroup.CPU_SHARE, "10");
        String name1 = "g1";
        CreateResourceGroupStmt stmt1 = new CreateResourceGroupStmt(false, name1, properties1);
        resourceGroupMgr.createResourceGroup(stmt1);

        Map<String, ResourceGroup> nameToRG = resourceGroupMgr.getNameToResourceGroup();
        Assert.assertEquals(1, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(name1));
        ResourceGroup group1 = nameToRG.get(name1);
        Assert.assertEquals(name1, group1.getName());

        Map<Long, ResourceGroup> idToRG = resourceGroupMgr.getIdToResourceGroup();
        Assert.assertEquals(1, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group1.getId()));

        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(ResourceGroup.CPU_SHARE, "20");
        String name2 = "g2";
        CreateResourceGroupStmt stmt2 = new CreateResourceGroupStmt(false, name2, properties2);
        resourceGroupMgr.createResourceGroup(stmt2);

        nameToRG = resourceGroupMgr.getNameToResourceGroup();
        Assert.assertEquals(2, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(name2));
        ResourceGroup group2 = nameToRG.get(name2);
        idToRG = resourceGroupMgr.getIdToResourceGroup();
        Assert.assertEquals(2, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group2.getId()));

        try {
            resourceGroupMgr.createResourceGroup(stmt2);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("already exist"));
        }

        CreateResourceGroupStmt stmt3 = new CreateResourceGroupStmt(true, name2, properties2);
        resourceGroupMgr.createResourceGroup(stmt3);
        Assert.assertEquals(2, resourceGroupMgr.getIdToResourceGroup().size());
        Assert.assertEquals(2, resourceGroupMgr.getNameToResourceGroup().size());
    }

    @Test
    public void testGetResourceGroup() throws UserException {
        Config.enable_resource_group = true;
        ResourceGroupMgr resourceGroupMgr = new ResourceGroupMgr();
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(ResourceGroup.CPU_SHARE, "10");
        String name1 = "g1";
        CreateResourceGroupStmt stmt1 = new CreateResourceGroupStmt(false, name1, properties1);
        resourceGroupMgr.createResourceGroup(stmt1);
        List<TPipelineResourceGroup> tResourceGroups1 = resourceGroupMgr.getResourceGroup(name1);
        Assert.assertEquals(1, tResourceGroups1.size());
        TPipelineResourceGroup tResourceGroup1 = tResourceGroups1.get(0);
        Assert.assertEquals(name1, tResourceGroup1.getName());
        Assert.assertTrue(tResourceGroup1.getProperties().containsKey(ResourceGroup.CPU_SHARE));

        try {
            resourceGroupMgr.getResourceGroup("g2");
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testDropResourceGroup() throws UserException {
        Config.enable_resource_group = true;
        ResourceGroupMgr resourceGroupMgr = new ResourceGroupMgr();
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(ResourceGroup.CPU_SHARE, "10");
        String name1 = "g1";
        CreateResourceGroupStmt createStmt = new CreateResourceGroupStmt(false, name1, properties1);
        resourceGroupMgr.createResourceGroup(createStmt);
        List<TPipelineResourceGroup> tResourceGroups = resourceGroupMgr.getResourceGroup(name1);
        Assert.assertEquals(1, tResourceGroups.size());

        DropResourceGroupStmt dropStmt = new DropResourceGroupStmt(false, name1);
        resourceGroupMgr.dropResourceGroup(dropStmt);
        Assert.assertEquals(0, tResourceGroups.size());

        try {
            resourceGroupMgr.dropResourceGroup(dropStmt);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }
}
