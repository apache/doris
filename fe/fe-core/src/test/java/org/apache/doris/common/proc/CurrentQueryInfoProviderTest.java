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

import org.apache.doris.common.profile.Counter;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class CurrentQueryInfoProviderTest {
    @Test
    public void testGetInstanceStatisticsFromPipelineTaskProfiles() throws Exception {
        TUniqueId instanceId0 = new TUniqueId(1, 2);
        TUniqueId instanceId1 = new TUniqueId(3, 4);
        RuntimeProfile queryProfile = new RuntimeProfile("DetailProfile");
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        RuntimeProfile fragmentProfile = new RuntimeProfile("Fragment 0");
        RuntimeProfile pipelineProfile = new RuntimeProfile("Pipeline 0(host=127.0.0.1:9050)");
        queryProfile.addChild(fragmentsProfile, true);
        fragmentsProfile.addChild(fragmentProfile, true);
        fragmentProfile.addChild(pipelineProfile, true);
        pipelineProfile.addChild(buildTaskProfile(instanceId0, "PipelineTask(index=0)", 10, 100), true);
        pipelineProfile.addChild(buildTaskProfile(instanceId0, "PipelineTask(index=1)", 20, 200), true);
        pipelineProfile.addChild(buildTaskProfile(instanceId1, "PipelineTask(index=2)", 30, 300), true);

        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .profile(queryProfile)
                .fragmentInstanceInfos(Lists.newArrayList(
                        new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                                .fragmentId("0")
                                .instanceId(instanceId0)
                                .address(new TNetworkAddress("127.0.0.1", 9050))
                                .build(),
                        new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                                .fragmentId("0")
                                .instanceId(instanceId1)
                                .address(new TNetworkAddress("127.0.0.1", 9050))
                                .build()))
                .build();

        Collection<CurrentQueryInfoProvider.InstanceStatistics> statistics =
                new CurrentQueryInfoProvider().getInstanceStatistics(item);
        Map<String, CurrentQueryInfoProvider.InstanceStatistics> statisticsMap = statistics.stream()
                .collect(Collectors.toMap(stat -> DebugUtil.printId(stat.getInstanceId()), stat -> stat));

        Assert.assertEquals(2, statisticsMap.size());
        Assert.assertEquals(30, statisticsMap.get(DebugUtil.printId(instanceId0)).getRowsReturned());
        Assert.assertEquals(300, statisticsMap.get(DebugUtil.printId(instanceId0)).getScanBytes());
        Assert.assertEquals(30, statisticsMap.get(DebugUtil.printId(instanceId1)).getRowsReturned());
        Assert.assertEquals(300, statisticsMap.get(DebugUtil.printId(instanceId1)).getScanBytes());
    }

    @Test
    public void testGetInstanceStatisticsFromLegacyInstanceProfile() throws Exception {
        TUniqueId instanceId = new TUniqueId(5, 6);
        RuntimeProfile queryProfile = new RuntimeProfile("DetailProfile");
        RuntimeProfile instanceProfile =
                new RuntimeProfile("Instance " + DebugUtil.printId(instanceId) + " (host=127.0.0.1:9050)");
        instanceProfile.addChild(buildOperatorProfile(40, 400), true);
        queryProfile.addChild(instanceProfile, true);

        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .profile(queryProfile)
                .fragmentInstanceInfos(Lists.newArrayList(
                        new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                                .fragmentId("0")
                                .instanceId(instanceId)
                                .address(new TNetworkAddress("127.0.0.1", 9050))
                                .build()))
                .build();

        Collection<CurrentQueryInfoProvider.InstanceStatistics> statistics =
                new CurrentQueryInfoProvider().getInstanceStatistics(item);

        Assert.assertEquals(1, statistics.size());
        CurrentQueryInfoProvider.InstanceStatistics instanceStatistics = statistics.iterator().next();
        Assert.assertEquals(40, instanceStatistics.getRowsReturned());
        Assert.assertEquals(400, instanceStatistics.getScanBytes());
    }

    @Test
    public void testGetInstanceStatisticsFromIncompleteProfile() throws Exception {
        TUniqueId instanceId = new TUniqueId(7, 8);
        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .profile(new RuntimeProfile("DetailProfile"))
                .fragmentInstanceInfos(Lists.newArrayList(
                        new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                                .fragmentId("0")
                                .instanceId(instanceId)
                                .address(new TNetworkAddress("127.0.0.1", 9050))
                                .build()))
                .build();

        Collection<CurrentQueryInfoProvider.InstanceStatistics> statistics =
                new CurrentQueryInfoProvider().getInstanceStatistics(item);

        Assert.assertEquals(1, statistics.size());
        CurrentQueryInfoProvider.InstanceStatistics instanceStatistics = statistics.iterator().next();
        Assert.assertEquals(0, instanceStatistics.getRowsReturned());
        Assert.assertEquals(0, instanceStatistics.getScanBytes());
    }

    private RuntimeProfile buildTaskProfile(TUniqueId instanceId, String taskName, long rowsReturned,
            long compressedBytesRead) {
        RuntimeProfile taskProfile = new RuntimeProfile(taskName);
        taskProfile.addInfoString(CurrentQueryInfoProvider.FRAGMENT_INSTANCE_ID, DebugUtil.printId(instanceId));
        taskProfile.addChild(buildOperatorProfile(rowsReturned, compressedBytesRead), true);
        return taskProfile;
    }

    private RuntimeProfile buildOperatorProfile(long rowsReturned, long compressedBytesRead) {
        RuntimeProfile operatorProfile = new RuntimeProfile("operator");
        Counter rowsReturnedCounter = operatorProfile.addCounter(
                "RowsReturned", TUnit.UNIT, RuntimeProfile.ROOT_COUNTER);
        rowsReturnedCounter.setValue(TUnit.UNIT, rowsReturned);
        Counter compressedBytesReadCounter = operatorProfile.addCounter(
                "CompressedBytesRead", TUnit.BYTES, RuntimeProfile.ROOT_COUNTER);
        compressedBytesReadCounter.setValue(TUnit.BYTES, compressedBytesRead);
        return operatorProfile;
    }
}
