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

package org.apache.doris.common.profile;

import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExecutionProfileTest {

    // A fragment whose backends report different pipeline counts must not blow up the
    // merge. pipelineSize is taken as the max across backends, so a backend with fewer
    // pipelines has no profile at the higher indices; before the fix, indexing it threw
    // IndexOutOfBoundsException and getPipelineAggregatedProfile lost the whole tree.
    @Test
    public void testAggregatedProfileUnevenPipelineCounts() {
        TUniqueId queryId = new TUniqueId(1L, 2L);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, Lists.newArrayList(0));

        // Backend A has two pipelines, backend B only one.
        TNetworkAddress beA = new TNetworkAddress("127.0.0.1", 9050);
        TNetworkAddress beB = new TNetworkAddress("127.0.0.2", 9050);
        executionProfile.setMultiBeProfile(0, beA, Lists.newArrayList(
                pipelineWithTasks("Pipeline 0(host=" + beA + ")", 2),
                pipelineWithTasks("Pipeline 1(host=" + beA + ")", 2)));
        executionProfile.setMultiBeProfile(0, beB, Lists.newArrayList(
                pipelineWithTasks("Pipeline 0(host=" + beB + ")", 2)));

        // Must not throw despite the uneven pipeline counts.
        RuntimeProfile result = executionProfile.getPipelineAggregatedProfile(Maps.newHashMap());

        Assert.assertNotNull(result);
        Assert.assertEquals("Fragments", result.getName());
        // The max pipeline count (2) is used, so both pipelines are represented.
        List<Pair<RuntimeProfile, Boolean>> fragments = result.getChildList();
        Assert.assertEquals(1, fragments.size());
        RuntimeProfile fragment0 = fragments.get(0).first;
        Assert.assertEquals("Fragment 0", fragment0.getName());
        Assert.assertEquals(2, fragment0.getChildList().size());
    }

    private RuntimeProfile pipelineWithTasks(String name, int taskNum) {
        RuntimeProfile pipeline = new RuntimeProfile(name);
        for (int i = 0; i < taskNum; i++) {
            pipeline.addChild(new RuntimeProfile(name + "-task" + i), true);
        }
        return pipeline;
    }
}
