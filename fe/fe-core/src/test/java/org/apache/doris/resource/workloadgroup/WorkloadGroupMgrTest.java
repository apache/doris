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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.analysis.AlterWorkloadGroupStmt;
import org.apache.doris.analysis.CreateWorkloadGroupStmt;
import org.apache.doris.analysis.DropWorkloadGroupStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPipelineWorkloadGroup;

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

public class WorkloadGroupMgrTest {

    @Injectable
    private EditLog editLog;

    @Mocked
    private Env env;

    @Mocked
    AccessControllerManager accessControllerManager;

    @Mocked
    private Auth auth;


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

                editLog.logCreateWorkloadGroup((WorkloadGroup) any);
                minTimes = 0;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkWorkloadGroupPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                env.getAuth();
                minTimes = 0;
                result = auth;

                auth.isWorkloadGroupInUse(anyString);
                minTimes = 0;
                result = new Delegate() {
                    Pair<Boolean, String> list() {
                        return Pair.of(false, "");
                    }
                };
            }
        };
    }

    @Test
    public void testCreateWorkloadGroup() throws DdlException {
        Config.enable_workload_group = true;
        WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "10");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name1 = "g1";
        CreateWorkloadGroupStmt stmt1 = new CreateWorkloadGroupStmt(false, name1, properties1);
        workloadGroupMgr.createWorkloadGroup(stmt1);

        Map<String, WorkloadGroup> nameToRG = workloadGroupMgr.getNameToWorkloadGroup();
        Assert.assertEquals(1, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(name1));
        WorkloadGroup group1 = nameToRG.get(name1);
        Assert.assertEquals(name1, group1.getName());

        Map<Long, WorkloadGroup> idToRG = workloadGroupMgr.getIdToWorkloadGroup();
        Assert.assertEquals(1, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group1.getId()));

        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(WorkloadGroup.CPU_SHARE, "20");
        properties2.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name2 = "g2";
        CreateWorkloadGroupStmt stmt2 = new CreateWorkloadGroupStmt(false, name2, properties2);
        workloadGroupMgr.createWorkloadGroup(stmt2);

        nameToRG = workloadGroupMgr.getNameToWorkloadGroup();
        Assert.assertEquals(2, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(name2));
        WorkloadGroup group2 = nameToRG.get(name2);
        idToRG = workloadGroupMgr.getIdToWorkloadGroup();
        Assert.assertEquals(2, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group2.getId()));

        try {
            workloadGroupMgr.createWorkloadGroup(stmt2);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("already exist"));
        }

        CreateWorkloadGroupStmt stmt3 = new CreateWorkloadGroupStmt(true, name2, properties2);
        workloadGroupMgr.createWorkloadGroup(stmt3);
        Assert.assertEquals(2, workloadGroupMgr.getIdToWorkloadGroup().size());
        Assert.assertEquals(2, workloadGroupMgr.getNameToWorkloadGroup().size());
    }

    @Test
    public void testGetWorkloadGroup() throws UserException {
        Config.enable_workload_group = true;
        ConnectContext context = new ConnectContext();
        WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "10");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name1 = "g1";
        CreateWorkloadGroupStmt stmt1 = new CreateWorkloadGroupStmt(false, name1, properties1);
        workloadGroupMgr.createWorkloadGroup(stmt1);
        context.getSessionVariable().setWorkloadGroup(name1);
        List<TPipelineWorkloadGroup> tWorkloadGroups1 = workloadGroupMgr.getWorkloadGroup(context);
        Assert.assertEquals(1, tWorkloadGroups1.size());
        TPipelineWorkloadGroup tWorkloadGroup1 = tWorkloadGroups1.get(0);
        Assert.assertEquals(name1, tWorkloadGroup1.getName());
        Assert.assertTrue(tWorkloadGroup1.getProperties().containsKey(WorkloadGroup.CPU_SHARE));

        try {
            context.getSessionVariable().setWorkloadGroup("g2");
            workloadGroupMgr.getWorkloadGroup(context);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testDropWorkloadGroup() throws UserException {
        Config.enable_workload_group = true;
        ConnectContext context = new ConnectContext();
        WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties = Maps.newHashMap();
        properties.put(WorkloadGroup.CPU_SHARE, "10");
        properties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name = "g1";
        CreateWorkloadGroupStmt createStmt = new CreateWorkloadGroupStmt(false, name, properties);
        workloadGroupMgr.createWorkloadGroup(createStmt);
        context.getSessionVariable().setWorkloadGroup(name);
        Assert.assertEquals(1, workloadGroupMgr.getWorkloadGroup(context).size());

        DropWorkloadGroupStmt dropStmt = new DropWorkloadGroupStmt(false, name);
        workloadGroupMgr.dropWorkloadGroup(dropStmt);
        try {
            context.getSessionVariable().setWorkloadGroup(name);
            workloadGroupMgr.getWorkloadGroup(context);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }

        DropWorkloadGroupStmt dropDefaultStmt = new DropWorkloadGroupStmt(false, WorkloadGroupMgr.DEFAULT_GROUP_NAME);
        try {
            workloadGroupMgr.dropWorkloadGroup(dropDefaultStmt);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("is not allowed"));
        }
    }

    @Test
    public void testAlterWorkloadGroup() throws UserException {
        Config.enable_workload_group = true;
        ConnectContext context = new ConnectContext();
        WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties = Maps.newHashMap();
        String name = "g1";
        try {
            AlterWorkloadGroupStmt stmt1 = new AlterWorkloadGroupStmt(name, properties);
            workloadGroupMgr.alterWorkloadGroup(stmt1);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }

        properties.put(WorkloadGroup.CPU_SHARE, "10");
        properties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        CreateWorkloadGroupStmt createStmt = new CreateWorkloadGroupStmt(false, name, properties);
        workloadGroupMgr.createWorkloadGroup(createStmt);

        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(WorkloadGroup.CPU_SHARE, "5");
        newProperties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        AlterWorkloadGroupStmt stmt2 = new AlterWorkloadGroupStmt(name, newProperties);
        workloadGroupMgr.alterWorkloadGroup(stmt2);

        context.getSessionVariable().setWorkloadGroup(name);
        List<TPipelineWorkloadGroup> tWorkloadGroups = workloadGroupMgr.getWorkloadGroup(context);
        Assert.assertEquals(1, tWorkloadGroups.size());
        TPipelineWorkloadGroup tWorkloadGroup1 = tWorkloadGroups.get(0);
        Assert.assertEquals(tWorkloadGroup1.getProperties().get(WorkloadGroup.CPU_SHARE), "5");
    }
}
