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

import com.google.common.collect.Lists;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.system.SystemInfoService;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class RoutineLoadManagerTest {

    private static final int DEFAULT_BE_CONCURRENT_TASK_NUM = 100;

    @Mocked
    private SystemInfoService systemInfoService;

    @Test
    public void testGetMinTaskBeId() throws LoadException {
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        new Expectations() {
            {
                systemInfoService.getBackendIds(true);
                result = beIds;
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
            }
        };

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.addNumOfConcurrentTasksByBeId(1L);
        Assert.assertEquals(2L, routineLoadManager.getMinTaskBeId());
    }

    @Test
    public void testGetTotalIdleTaskNum() {
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        new Expectations() {
            {
                systemInfoService.getBackendIds(true);
                result = beIds;
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
            }
        };

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.addNumOfConcurrentTasksByBeId(1L);
        Assert.assertEquals(DEFAULT_BE_CONCURRENT_TASK_NUM * 2 - 1, routineLoadManager.getClusterIdleSlotNum());
    }

    @Test
    public void testUpdateBeIdTaskMaps() {
        List<Long> oldBeIds = Lists.newArrayList();
        oldBeIds.add(1L);
        oldBeIds.add(2L);

        List<Long> newBeIds = Lists.newArrayList();
        newBeIds.add(1L);
        newBeIds.add(3L);

        new Expectations() {
            {
                systemInfoService.getBackendIds(true);
                returns(oldBeIds, newBeIds);
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
            }
        };

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.updateBeIdTaskMaps();
    }

}
