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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.DropWorkloadGroupOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.MergedComputeGroup;
import org.apache.doris.thrift.TPipelineWorkloadGroup;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
        workloadGroupMgr.getIdToWorkloadGroup().clear();
        workloadGroupMgr.getNameToWorkloadGroup().clear();

        // 1 create workload group 1
        Map<String, String> properties1 = Maps.newHashMap();
        String cg1 = "cg1";
        long wgId1 = 1;
        String wgName1 = "wg1";
        properties1.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        properties1.put(WorkloadGroup.MIN_MEMORY_PERCENT, "40%");
        properties1.put(WorkloadGroup.MAX_MEMORY_PERCENT, "50%");
        properties1.put(WorkloadGroup.COMPUTE_GROUP, cg1);
        WorkloadGroup wg1 = new WorkloadGroup(wgId1, wgName1, properties1);
        workloadGroupMgr.createWorkloadGroup(cg1, wg1, false);

        WorkloadGroupKey key1 = WorkloadGroupKey.get(cg1, wgName1);
        Map<WorkloadGroupKey, WorkloadGroup> nameToRG = workloadGroupMgr.getNameToWorkloadGroup();
        Assert.assertEquals(1, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(key1));
        WorkloadGroup group1 = nameToRG.get(key1);
        Assert.assertEquals(key1.getWorkloadGroupName(), group1.getName());
        Assert.assertEquals(key1.getComputeGroup(), group1.getComputeGroup());

        Map<Long, WorkloadGroup> idToRG = workloadGroupMgr.getIdToWorkloadGroup();
        Assert.assertEquals(1, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group1.getId()));

        // 2 create workload group 2
        long wgId2 = 2;
        String cg2 = "cg2";
        String wgName2 = "wg2";
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(WorkloadGroup.MIN_CPU_PERCENT, "20");
        properties2.put(WorkloadGroup.MAX_MEMORY_PERCENT, "30%");
        properties2.put(WorkloadGroup.COMPUTE_GROUP, cg2);
        WorkloadGroup wg2 = new WorkloadGroup(wgId2, wgName2, properties2);
        workloadGroupMgr.createWorkloadGroup(cg2, wg2, false);

        WorkloadGroupKey key2 = WorkloadGroupKey.get(cg2, wgName2);
        nameToRG = workloadGroupMgr.getNameToWorkloadGroup();
        Assert.assertEquals(2, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(key2));
        WorkloadGroup group2 = nameToRG.get(key2);
        idToRG = workloadGroupMgr.getIdToWorkloadGroup();
        Assert.assertEquals(2, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group2.getId()));
        Assert.assertTrue(key2.getComputeGroup().equals(wg2.getComputeGroup()));

        // 3 test memory limit exceeds, it will success
        Map<String, String> properties3 = Maps.newHashMap();
        properties3.put(WorkloadGroup.MIN_CPU_PERCENT, "20");
        properties3.put(WorkloadGroup.MAX_MEMORY_PERCENT, "90%");
        properties3.put(WorkloadGroup.COMPUTE_GROUP, cg1);
        String wgName3 = "wg3";
        long wgId3 = 3;
        properties3.put(WorkloadGroup.MAX_MEMORY_PERCENT, "1%");
        workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(wgId3, wgName3, properties3), false);

        // test sum of  min cpu percent > 100, it will fail
        try {
            Map<String, String> propertiesErrorMincpu = Maps.newHashMap();
            propertiesErrorMincpu.put(WorkloadGroup.MIN_CPU_PERCENT, "80");
            propertiesErrorMincpu.put(WorkloadGroup.MAX_MEMORY_PERCENT, "90%");
            propertiesErrorMincpu.put(WorkloadGroup.COMPUTE_GROUP, cg1);
            propertiesErrorMincpu.put(WorkloadGroup.MAX_MEMORY_PERCENT, "1%");
            workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(11, "wg_err_mincpu", propertiesErrorMincpu), false);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(true);
        }

        // test sum of  min memory percent > 100, it will fail
        try {
            Map<String, String> propertiesErrorMinmem = Maps.newHashMap();
            propertiesErrorMinmem.put(WorkloadGroup.MIN_MEMORY_PERCENT, "80%");
            propertiesErrorMinmem.put(WorkloadGroup.MAX_MEMORY_PERCENT, "90%");
            propertiesErrorMinmem.put(WorkloadGroup.COMPUTE_GROUP, cg1);
            propertiesErrorMinmem.put(WorkloadGroup.MAX_MEMORY_PERCENT, "1%");
            workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(11, "wg_err_minmem", propertiesErrorMinmem), false);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(true);
        }

        // 4 test create duplicate workload group error.
        workloadGroupMgr.createWorkloadGroup(cg1, wg1, true);
        try {
            // create wg1 in cg1, it should fail
            workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(4, wgName1, properties1), false);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("already has workload group"));
        }
        Map<String, String> properties4 = Maps.newHashMap();
        properties4.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        properties4.put(WorkloadGroup.COMPUTE_GROUP, cg2);
        // create wg1 in cg2, it should be success
        workloadGroupMgr.createWorkloadGroup(cg2, new WorkloadGroup(4, wgName1, properties4), false);

        // test workload group's min cpu percent > max cpu percent
        try {
            Map<String, String> propertiesMinCpu = Maps.newHashMap();
            propertiesMinCpu.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
            propertiesMinCpu.put(WorkloadGroup.MAX_CPU_PERCENT, "5");
            propertiesMinCpu.put(WorkloadGroup.MAX_MEMORY_PERCENT, "50%");
            propertiesMinCpu.put(WorkloadGroup.COMPUTE_GROUP, cg1);
            // create wg1 in cg1, it should fail
            workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(11, "test_min_cpu", propertiesMinCpu), false);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(true);
        }

        // test workload group's min memory percent > max memory percent
        try {
            Map<String, String> propertiesMinMemory = Maps.newHashMap();
            propertiesMinMemory.put(WorkloadGroup.MIN_MEMORY_PERCENT, "10");
            propertiesMinMemory.put(WorkloadGroup.MAX_MEMORY_PERCENT, "5");
            propertiesMinMemory.put(WorkloadGroup.COMPUTE_GROUP, cg1);
            // create wg1 in cg1, it should fail
            workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(11, "test_min_memory", propertiesMinMemory), false);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testGetWorkloadGroup() throws UserException {
        Config.enable_workload_group = true;
        WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();
        long wgId1 = 1;
        String wgName1 = "wg1";
        String cgName1 = "cg1";
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        properties1.put(WorkloadGroup.COMPUTE_GROUP, cgName1);
        workloadGroupMgr.createWorkloadGroup(cgName1, new WorkloadGroup(wgId1, wgName1, properties1), false);

        long wgId2 = 2;
        String wgName2 = "wg2";
        String cgName2 = "cg2";
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(WorkloadGroup.MIN_CPU_PERCENT, "20");
        properties2.put(WorkloadGroup.COMPUTE_GROUP, cgName2);
        workloadGroupMgr.createWorkloadGroup(cgName2, new WorkloadGroup(wgId2, wgName2, properties2), false);

        workloadGroupMgr.createWorkloadGroup(cgName1, new WorkloadGroup(100, "normal", properties1), false);
        workloadGroupMgr.createWorkloadGroup(cgName2, new WorkloadGroup(101, "normal", properties1), false);


        // 1 test get workload group by ConnectContext
        ConnectContext ctx = new ConnectContext();
        // 1.1 not set wg, get normal
        ctx.setComputeGroup(new ComputeGroup(cgName1, cgName1, null));
        List<TPipelineWorkloadGroup> ret = workloadGroupMgr.getWorkloadGroup(ctx)
                .stream()
                .map(e -> e.toThrift())
                .collect(Collectors.toList());
        Assert.assertTrue(ret.get(0).getId() == 100);

        ctx.setComputeGroup(new ComputeGroup(cgName2, cgName2, null));
        Assert.assertTrue(workloadGroupMgr.getWorkloadGroup(ctx).get(0).getId() == 101);


        // 1.2 get from user prop

        // 1.3 get from session
        ctx.getSessionVariable().setWorkloadGroup(wgName2);
        Assert.assertTrue(workloadGroupMgr.getWorkloadGroup(ctx).size() == 1);
        Assert.assertTrue(workloadGroupMgr.getWorkloadGroup(ctx).get(0).getId() == wgId2);

        // 1.4 get multi workload group
        Set<String> cgSet = Sets.newHashSet();
        cgSet.add(cgName1);
        cgSet.add(cgName2);
        long wgId3 = 3;
        ComputeGroup mergedComputeGroup = new MergedComputeGroup(
                String.join(",", cgSet), cgSet, null);
        ctx.setComputeGroup(mergedComputeGroup);
        Map<String, String> prop3 = Maps.newHashMap();
        prop3.put(WorkloadGroup.COMPUTE_GROUP, cgName1);
        workloadGroupMgr.createWorkloadGroup(cgName1, new WorkloadGroup(wgId3, wgName2, prop3), false);
        List<TPipelineWorkloadGroup> tPipWgList = workloadGroupMgr.getWorkloadGroup(ctx)
                .stream()
                .map(e -> e.toThrift())
                .collect(Collectors.toList());
        Set<Long> idSet = Sets.newHashSet();
        for (TPipelineWorkloadGroup tpip : tPipWgList) {
            idSet.add(tpip.getId());
        }

        Assert.assertTrue(idSet.size() == 2);
        Assert.assertTrue(idSet.contains(wgId2));
        Assert.assertTrue(idSet.contains(wgId3));

        // 1.5 test get failed
        ctx.getSessionVariable().setWorkloadGroup("abc");
        try {
            workloadGroupMgr.getWorkloadGroup(ctx)
                .stream()
                .map(e -> e.toThrift()).collect(Collectors.toList());
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Can not find workload group"));
        }
    }

    @Test
    public void testAlterWorkloadGroup() throws UserException {
        Config.enable_workload_group = true;
        WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();
        workloadGroupMgr.getIdToWorkloadGroup().clear();
        workloadGroupMgr.getNameToWorkloadGroup().clear();

        Map<String, String> p0 = Maps.newHashMap();
        try {
            workloadGroupMgr.alterWorkloadGroup(new ComputeGroup("", "", null), "", p0);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("should contain at least one property"));
        }

        p0.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        try {
            workloadGroupMgr.alterWorkloadGroup(new ComputeGroup("", "", null), "abc", p0);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Can not find workload group"));
        }

        long wgId1 = 1;
        String wgName1 = "wg1";
        String cgName1 = "cg1";
        Map<String, String> prop1 = Maps.newHashMap();
        prop1.put(WorkloadGroup.COMPUTE_GROUP, cgName1);
        prop1.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        workloadGroupMgr.createWorkloadGroup(cgName1, new WorkloadGroup(wgId1, wgName1, prop1), false);
        Assert.assertTrue(Long.valueOf(
                workloadGroupMgr.getNameToWorkloadGroup().get(WorkloadGroupKey.get(cgName1, wgName1)).getProperties()
                        .get(WorkloadGroup.MIN_CPU_PERCENT)) == 10);

        // test alter failed
        String cgName2 = "cg2";
        String wgName2 = "wg2";
        Map<String, String> prop2 = Maps.newHashMap();
        prop2.put(WorkloadGroup.MIN_CPU_PERCENT, "20");
        try {
            workloadGroupMgr.alterWorkloadGroup(new ComputeGroup(cgName2, cgName2, null), wgName2, prop2);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Can not find workload group"));
        }

        // test alter success
        workloadGroupMgr.alterWorkloadGroup(new ComputeGroup(cgName1, cgName1, null), wgName1, prop2);
        WorkloadGroup wg = workloadGroupMgr.getNameToWorkloadGroup().get(WorkloadGroupKey.get(cgName1, wgName1));
        Assert.assertTrue(Long.valueOf(wg.getProperties().get(WorkloadGroup.MIN_CPU_PERCENT)) == 20);
    }

    // before:
    //   empty cg: wg1, wg2, wg3(id=3)
    //   cg1: wg3(id=4)
    // after:
    //   cg1: wg1, wg2, wg3(id=4)
    //   cg2: wg1, wg2, wg3
    @Test
    public void testBindWorkloadGroupToCg() throws DdlException {
        WorkloadGroupMgr wgMgr = new WorkloadGroupMgr();
        wgMgr.getIdToNameMap().clear();
        wgMgr.getNameToWorkloadGroup().clear();

        long wgId1 = 1;
        String wgName1 = "wg1";
        Map<String, String> prop1 = Maps.newHashMap();
        prop1.put(WorkloadGroup.MIN_CPU_PERCENT, "12");
        WorkloadGroup wg1 = new WorkloadGroup(wgId1, wgName1, prop1);
        wgMgr.getIdToWorkloadGroup().put(wgId1, wg1);
        wgMgr.getNameToWorkloadGroup().put(WorkloadGroupKey.get(WorkloadGroupMgr.EMPTY_COMPUTE_GROUP, wgName1), wg1);

        long wgId2 = 2;
        String wgName2 = "wg2";
        Map<String, String> prop2 = Maps.newHashMap();
        prop2.put(WorkloadGroup.MIN_CPU_PERCENT, "12");
        WorkloadGroup wg2 = new WorkloadGroup(wgId2, wgName2, prop2);
        wgMgr.getIdToWorkloadGroup().put(wgId2, wg2);
        wgMgr.getNameToWorkloadGroup().put(WorkloadGroupKey.get(WorkloadGroupMgr.EMPTY_COMPUTE_GROUP, wgName2), wg2);

        long wgId3 = 3;
        String wgName3 = "wg3";
        Map<String, String> prop3 = Maps.newHashMap();
        prop3.put(WorkloadGroup.MIN_CPU_PERCENT, "12");
        WorkloadGroup wg3 = new WorkloadGroup(wgId3, wgName3, prop3);
        wgMgr.getIdToWorkloadGroup().put(wgId3, wg3);
        wgMgr.getNameToWorkloadGroup().put(WorkloadGroupKey.get(WorkloadGroupMgr.EMPTY_COMPUTE_GROUP, wgName3), wg3);


        // create a duplicate wg3 which binds to a compute group
        String cg1 = "cg1";

        long wgId4 = 4;
        String wgName4 = wgName3;
        Map<String, String> prop4 = Maps.newHashMap();
        prop4.put(WorkloadGroup.MIN_CPU_PERCENT, "15");
        prop4.put(WorkloadGroup.COMPUTE_GROUP, cg1);
        WorkloadGroup wg4 = new WorkloadGroup(wgId4, wgName4, prop4);
        wgMgr.createWorkloadGroup(cg1, wg4, false);


        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().size() == 4);
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().size() == 4);
        Assert.assertTrue(wgMgr.getOldWorkloadGroup().size() == 3);

        String cg2 = "cg2";
        Set<String> cgSet = Sets.newHashSet();
        cgSet.add(cg1);
        cgSet.add(cg2);

        for (WorkloadGroup oldWg : wgMgr.getOldWorkloadGroup()) {
            wgMgr.bindWorkloadGroupToComputeGroup(cgSet, oldWg);
        }

        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().size() == 6);
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().size() == 6);
        Assert.assertTrue(wgMgr.getOldWorkloadGroup().size() == 0);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().get(wgId1) == null);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().get(wgId2) == null);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().get(wgId3) == null);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().get(wgId4).equals(wg4));

        for (String cgName : cgSet) {
            WorkloadGroup wg11 = wgMgr.getNameToWorkloadGroup().get(WorkloadGroupKey.get(cgName, wgName1));
            WorkloadGroup wg22 = wgMgr.getNameToWorkloadGroup().get(WorkloadGroupKey.get(cgName, wgName2));
            WorkloadGroup wg33 = wgMgr.getNameToWorkloadGroup().get(WorkloadGroupKey.get(cgName, wgName3));

            Assert.assertTrue(wgMgr.getIdToWorkloadGroup().containsKey(wg11.getId()));
            Assert.assertTrue(wgMgr.getIdToWorkloadGroup().containsKey(wg22.getId()));
            Assert.assertTrue(wgMgr.getIdToWorkloadGroup().containsKey(wg33.getId()));

            Assert.assertTrue(wg11.getComputeGroup().equals(cgName));
            Assert.assertTrue(wg22.getComputeGroup().equals(cgName));
            Assert.assertTrue(wg33.getComputeGroup().equals(cgName));

            Assert.assertTrue(wg11.getProperties().get(WorkloadGroup.MIN_CPU_PERCENT).equals("12"));
            Assert.assertTrue(wg22.getProperties().get(WorkloadGroup.MIN_CPU_PERCENT).equals("12"));
            if (cg1.equals(cgName)) {
                Assert.assertTrue(wg33.getProperties().get(WorkloadGroup.MIN_CPU_PERCENT).equals("15"));
            } else {
                Assert.assertTrue(wg33.getProperties().get(WorkloadGroup.MIN_CPU_PERCENT).equals("12"));
            }

        }

    }

    @Test
    public void testMultiTagCreateWorkloadGroup() throws UserException {
        Config.enable_workload_group = true;
        String[] props = {WorkloadGroup.MAX_MEMORY_PERCENT};
        for (String propName : props) {
            WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();

            // create cg1.wg1 with 30%
            String cgName1 = "cg1";
            String wgName11 = "wg1";
            Map<String, String> prop1 = Maps.newHashMap();
            prop1.put(WorkloadGroup.COMPUTE_GROUP, cgName1);
            prop1.put(propName, "30%");
            workloadGroupMgr.createWorkloadGroup(cgName1, new WorkloadGroup(1, wgName11, prop1), false);

            // create cg2.wg1 with 71%
            String cgName2 = "cg2";
            String wgName21 = "wg1";
            Map<String, String> prop2 = Maps.newHashMap();
            prop2.put(WorkloadGroup.COMPUTE_GROUP, cgName2);
            prop2.put(propName, "71%");
            workloadGroupMgr.createWorkloadGroup(cgName2, new WorkloadGroup(2, wgName21, prop2), false);

            // create cg1.wg2 with 71%, it should be failed.
            String wgName12 = "wg2";
            Map<String, String> prop3 = Maps.newHashMap();
            prop3.put(WorkloadGroup.COMPUTE_GROUP, cgName1);
            prop3.put(propName, "70%");
            workloadGroupMgr.createWorkloadGroup(cgName1, new WorkloadGroup(3, wgName12, prop3), false);

            // create cg2.wg2 with limit 20%, it should be succ.
            String wgName22 = "wg2";
            Map<String, String> prop5 = Maps.newHashMap();
            prop5.put(WorkloadGroup.COMPUTE_GROUP, cgName2);
            prop5.put(propName, "9%");
            workloadGroupMgr.createWorkloadGroup(cgName2, new WorkloadGroup(5, wgName22, prop5), false);
        }
    }


    @Test
    public void testMultiTagAlterWorkloadGroup() throws UserException {
        Config.enable_workload_group = true;
        String[] props = {WorkloadGroup.MAX_MEMORY_PERCENT, WorkloadGroup.MAX_CPU_PERCENT};
        String cg1 = "cg1";
        String cg2 = "cg2";
        for (String prop : props) {
            WorkloadGroupMgr workloadGroupMgr = new WorkloadGroupMgr();
                // create 3 wgs for cg1
                {
                    String wgName = "wg1";
                    Map<String, String> properties = Maps.newHashMap();
                    properties.put(prop, "10%");
                    properties.put(WorkloadGroup.COMPUTE_GROUP, cg1);
                    workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(1, wgName, properties), false);
                }

                {
                    String wgName = "wg2";
                    Map<String, String> properties = Maps.newHashMap();
                    properties.put(prop, "10%");
                    properties.put(WorkloadGroup.COMPUTE_GROUP, cg1);
                    workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(2, wgName, properties), false);
                }

                {
                    String wgName = "wg3";
                    Map<String, String> properties = Maps.newHashMap();
                    properties.put(prop, "10%");
                    properties.put(WorkloadGroup.COMPUTE_GROUP, cg1);
                    workloadGroupMgr.createWorkloadGroup(cg1, new WorkloadGroup(3, wgName, properties), false);
                }

                // create 2 wgs for cg2
                {
                    String wgName = "wg1";
                    Map<String, String> properties = Maps.newHashMap();
                    properties.put(prop, "10%");
                    properties.put(WorkloadGroup.COMPUTE_GROUP, cg2);
                    workloadGroupMgr.createWorkloadGroup(cg2, new WorkloadGroup(4, wgName, properties), false);
                }

                {
                    String wgName = "wg2";
                    Map<String, String> properties = Maps.newHashMap();
                    properties.put(prop, "10%");
                    properties.put(WorkloadGroup.COMPUTE_GROUP, cg2);
                    workloadGroupMgr.createWorkloadGroup(cg2, new WorkloadGroup(5, wgName, properties), false);
                }

                // alter cg1.wg1 failed
                {
                    Map<String, String> properties = Maps.newHashMap();
                    properties.put(prop, "90%");
                    try {
                        workloadGroupMgr.alterWorkloadGroup(new ComputeGroup(cg1, cg1, null), "wg1", properties);
                    } catch (DdlException e) {
                        Assert.assertTrue(e.getMessage().contains("current sum val:110"));
                        Assert.assertTrue(e.getMessage().contains("cg1"));
                        Assert.assertFalse(e.getMessage().contains("cg2"));
                    }
                }

                // alter cg1.wg1 succ
                {
                    Map<String, String> properties = Maps.newHashMap();
                    properties.put(prop, "20%");
                    workloadGroupMgr.alterWorkloadGroup(new ComputeGroup(cg1, cg1, null), "wg1", properties);
                }
        }
    }

    @Test
    public void testReplayWorkloadGroup() {
        WorkloadGroupMgr wgMgr = new WorkloadGroupMgr();
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().size() == 0);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().size() == 0);


        // 1 test replay create
        WorkloadGroup wg1 = new WorkloadGroup(1, "wg1", Maps.newHashMap());
        wgMgr.replayCreateWorkloadGroup(wg1);

        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().size() == 1);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().size() == 1);
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().get(wg1.getWorkloadGroupKey())
                .equals(wgMgr.getIdToWorkloadGroup().get(wg1.getId())));

        // 2 test replay alter
        Map<String, String> pop2 = Maps.newHashMap();
        pop2.put("MIN_CPU_PERCENT", "2345");
        WorkloadGroup wg2 = new WorkloadGroup(1, "wg1", pop2);
        wgMgr.replayAlterWorkloadGroup(wg2);
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().get(wg2.getWorkloadGroupKey())
                .equals(wgMgr.getIdToWorkloadGroup().get(wg2.getId())));
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().get(wg2.getWorkloadGroupKey()).getProperties().get("MIN_CPU_PERCENT")
                .equals("2345"));
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().size() == 1);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().size() == 1);

        // 3 test replay drop
        DropWorkloadGroupOperationLog dropLog = new DropWorkloadGroupOperationLog(1);
        wgMgr.replayDropWorkloadGroup(dropLog);
        Assert.assertTrue(wgMgr.getNameToWorkloadGroup().size() == 0);
        Assert.assertTrue(wgMgr.getIdToWorkloadGroup().size() == 0);
    }
}
