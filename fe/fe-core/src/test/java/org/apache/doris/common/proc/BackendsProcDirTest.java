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

import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BackendsProcDirTest {
    private Backend b1;
    private Backend b2;

    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    private TabletInvertedIndex tabletInvertedIndex;
    @Mocked
    private Env env;
    @Mocked
    private EditLog editLog;

    @Before
    public void setUp() {
        b1 = new Backend(1000, "host1", 10000);
        b1.updateOnce(10001, 10003, 10005);
        b2 = new Backend(1001, "host2", 20000);
        b2.updateOnce(20001, 20003, 20005);

        new Expectations() {
            {
                editLog.logAddBackend((Backend) any);
                minTimes = 0;

                editLog.logDropBackend((Backend) any);
                minTimes = 0;

                editLog.logBackendStateChange((Backend) any);
                minTimes = 0;

                env.getNextId();
                minTimes = 0;
                result = 10000L;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                env.clear();
                minTimes = 0;

                systemInfoService.getBackend(1000);
                minTimes = 0;
                result = b1;

                systemInfoService.getBackend(1001);
                minTimes = 0;
                result = b2;

                systemInfoService.getBackend(1002);
                minTimes = 0;
                result = null;

                tabletInvertedIndex.getTabletNumByBackendId(anyLong);
                minTimes = 0;
                result = 2;
            }
        };

        new Expectations(env) {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = tabletInvertedIndex;

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

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
}
