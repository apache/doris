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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class BackendsProcDirTest {
    private Backend b1;
    private Backend b2;

    private SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
    private TabletInvertedIndex tabletInvertedIndex = Mockito.mock(TabletInvertedIndex.class);
    private Env env = Mockito.mock(Env.class);
    private EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> mockedEnvStatic;

    @Before
    public void setUp() {
        b1 = new Backend(1000, "host1", 10000);
        b1.updateOnce(10001, 10003, 10005);
        b2 = new Backend(1001, "host2", 20000);
        b2.updateOnce(20001, 20003, 20005);

        Mockito.when(env.getNextId()).thenReturn(10000L);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(systemInfoService.getBackend(1000)).thenReturn(b1);
        Mockito.when(systemInfoService.getBackend(1001)).thenReturn(b2);
        Mockito.when(systemInfoService.getBackend(1002)).thenReturn(null);
        Mockito.when(tabletInvertedIndex.getTabletNumByBackendId(Mockito.anyLong())).thenReturn(2);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(tabletInvertedIndex);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
    }

    @After
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    @Test
    public void testRegister() {
        BackendsProcDir dir;

        dir = new BackendsProcDir(systemInfoService);
        Assert.assertFalse(dir.register("100000", new BaseProcDir()));
    }

    @Test(expected = AnalysisException.class)
    public void testLookupNormal() throws AnalysisException {
        BackendsProcDir dir;
        ProcNodeInterface node;

        dir = new BackendsProcDir(systemInfoService);
        try {
            node = dir.lookup("1000");
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof BackendProcNode);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        dir = new BackendsProcDir(systemInfoService);
        try {
            node = dir.lookup("1001");
            Assert.assertNotNull(node);
            Assert.assertTrue(node instanceof BackendProcNode);
        } catch (AnalysisException e) {
            Assert.fail();
        }

        dir = new BackendsProcDir(systemInfoService);
        node = dir.lookup("1002");
        Assert.fail();
    }

    @Test
    public void testLookupInvalid() {
        BackendsProcDir dir;

        dir = new BackendsProcDir(systemInfoService);
        try {
            dir.lookup(null);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        try {
            dir.lookup("");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFetchResultNormal() throws AnalysisException {
        BackendsProcDir dir;
        ProcResult result;

        dir = new BackendsProcDir(systemInfoService);
        result = dir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);
    }

    @Test
    public void testBackendInfoFieldOrder() throws AnalysisException {
        b1.setCpuCores(192);
        b1.setLastUpdateMs(System.currentTimeMillis());
        b1.setRunningTasks(10L);

        Mockito.when(systemInfoService.getAllBackendIds(false)).thenReturn(Lists.newArrayList(1000L));
        Mockito.when(systemInfoService.getTabletNumByBackendId(1000L)).thenReturn(10);

        BackendsProcDir dir = new BackendsProcDir(systemInfoService);
        ProcResult result = dir.fetchResult();

        List<String> columnNames = result.getColumnNames();

        int cpuCoresIdx = columnNames.indexOf("CpuCores");
        int memoryIdx = columnNames.indexOf("Memory");
        int liveSinceIdx = columnNames.indexOf("LiveSince");
        int runningTasksIdx = columnNames.indexOf("RunningTasks");
        int nodeRoleIdx = columnNames.indexOf("NodeRole");

        Assert.assertTrue("CpuCores should be before Memory", cpuCoresIdx < memoryIdx);
        Assert.assertTrue("Memory should be before LiveSince", memoryIdx < liveSinceIdx);
        Assert.assertTrue("LiveSince should be before RunningTasks", liveSinceIdx < runningTasksIdx);
        Assert.assertTrue("RunningTasks should be before NodeRole", runningTasksIdx < nodeRoleIdx);
    }
}
