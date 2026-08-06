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

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class BackendProcNodeTest {
    private Backend b1;
    private Env env;
    private EditLog editLog;
    private MockedStatic<Env> mockedEnv;

    @Before
    public void setUp() {
        env = Mockito.mock(Env.class);
        editLog = Mockito.mock(EditLog.class);

        Mockito.when(env.getNextId()).thenReturn(10000L);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        b1 = new Backend(1000, "host1", 10000);
        b1.updateOnce(10001, 10003, 10005);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        disks.put("/home/disk1", new DiskInfo("/home/disk1"));
        ImmutableMap<String, DiskInfo> immutableMap = ImmutableMap.copyOf(disks);
        b1.setDisks(immutableMap);
    }

    @After
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    @Test
    public void testResultNormal() throws AnalysisException {
        BackendProcNode node = new BackendProcNode(b1);
        ProcResult result;

        // fetch result
        result = node.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertTrue(result.getRows().size() >= 1);
        Assert.assertEquals(Lists.newArrayList("RootPath", "DataUsedCapacity", "OtherUsedCapacity", "AvailCapacity",
                "TotalCapacity", "TotalUsedPct", "State", "PathHash", "StorageMedium"), result.getColumnNames());
    }

}
