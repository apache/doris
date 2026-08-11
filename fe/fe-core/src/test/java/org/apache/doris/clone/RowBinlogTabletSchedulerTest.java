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

package org.apache.doris.clone;

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class RowBinlogTabletSchedulerTest {
    private MockedStatic<Env> mockedEnvStatic;
    private TabletScheduler tabletScheduler;

    @Before
    public void setUp() {
        SystemInfoService infoService = new SystemInfoService();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(infoService);

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getColocateTableIndex()).thenReturn(new ColocateTableIndex());
        tabletScheduler = new TabletScheduler(env, infoService, new LocalTabletInvertedIndex(),
                new TabletSchedulerStat(), "");
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void rowBinlogRequiredPathFiltersOutOtherDisks() {
        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, 4L, 5L, new ReplicaAllocation((short) 3), System.currentTimeMillis());
        tabletCtx.setRowBinlogRequiredDestPathHashByBackend(ImmutableMap.of(10001L, 70001L));

        List<RootPathLoadStatistic> paths = Lists.newArrayList(
                path(10001L, "/path1", 70001L),
                path(10001L, "/path2", 70002L),
                path(10001L, "/path3", 70003L));

        Deencapsulation.invoke(tabletScheduler, "filterRowBinlogRequiredPaths", tabletCtx, 10001L, paths);

        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(70001L, paths.get(0).getPathHash());
    }

    @Test
    public void rowBinlogRequiredPathDoesNotFallbackToOtherPath() {
        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, 4L, 5L, new ReplicaAllocation((short) 3), System.currentTimeMillis());
        tabletCtx.setRowBinlogRequiredDestPathHashByBackend(ImmutableMap.of(10001L, 79999L));

        List<RootPathLoadStatistic> paths = Lists.newArrayList(
                path(10001L, "/path1", 70001L),
                path(10001L, "/path2", 70002L));

        Deencapsulation.invoke(tabletScheduler, "filterRowBinlogRequiredPaths", tabletCtx, 10001L, paths);

        Assert.assertTrue(paths.isEmpty());
    }

    private RootPathLoadStatistic path(long backendId, String path, long pathHash) {
        return new RootPathLoadStatistic(backendId, path, pathHash, TStorageMedium.HDD,
                1024L * 1024L, 1024L, DiskState.ONLINE);
    }
}
