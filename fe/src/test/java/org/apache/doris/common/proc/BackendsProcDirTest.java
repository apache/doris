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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest(Catalog.class)
public class BackendsProcDirTest {
    private static Backend b1;
    private static Backend b2;

    private static SystemInfoService systemInfoService;
    private static TabletInvertedIndex tabletInvertedIndex;

    private static Catalog catalog;
    private static EditLog editLog;

    // construct test case
    @BeforeClass
    public static void setUpClass() {
        editLog = EasyMock.createMock(EditLog.class);
        editLog.logAddBackend(EasyMock.anyObject(Backend.class));
        EasyMock.expectLastCall().anyTimes();
        editLog.logDropBackend(EasyMock.anyObject(Backend.class));
        EasyMock.expectLastCall().anyTimes();
        editLog.logBackendStateChange(EasyMock.anyObject(Backend.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(editLog);

        catalog = EasyMock.createMock(Catalog.class);
        EasyMock.expect(catalog.getNextId()).andReturn(10000L).anyTimes();
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        catalog.clear();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(catalog);

        b1 = new Backend(1000, "host1", 10000);
        b1.updateOnce(10001, 10003, 10005);
        b2 = new Backend(1001, "host2", 20000);
        b2.updateOnce(20001, 20003, 20005);

        systemInfoService = EasyMock.createNiceMock(SystemInfoService.class);
        EasyMock.expect(systemInfoService.getBackend(1000)).andReturn(b1).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(1001)).andReturn(b2).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(1002)).andReturn(null).anyTimes();
        EasyMock.replay(systemInfoService);

        tabletInvertedIndex = EasyMock.createNiceMock(TabletInvertedIndex.class);
        EasyMock.expect(tabletInvertedIndex.getTabletNumByBackendId(EasyMock.anyLong())).andReturn(2).anyTimes();
        EasyMock.replay(tabletInvertedIndex);

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(tabletInvertedIndex).anyTimes();
        PowerMock.replay(Catalog.class);
    }

    @Before
    public void setUp() {
        // systemInfoService = EasyMock.createNiceMock(SystemInfoService.class);
    }

    @After
    public void tearDown() {
        // systemInfoService = null;
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
        ProcNodeInterface node;

        dir = new BackendsProcDir(systemInfoService);
        try {
            node = dir.lookup(null);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        try {
            node = dir.lookup("");
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

}
