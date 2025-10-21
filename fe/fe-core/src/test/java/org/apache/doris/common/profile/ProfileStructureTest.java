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

public class ProfileStructureTest {

    @Test
    public void testToString() {
        // Create a mock query ID
        TUniqueId queryId = new TUniqueId(1L, 2L);

        // Create ExecutionProfile with two fragments
        ExecutionProfile profile = new ExecutionProfile(queryId, Lists.newArrayList(0, 1));

        // Convert to string
        String result = profile.toString();

        // Verify the structure of the output
        Assert.assertTrue("Should contain DetailProfile", result.contains("DetailProfile"));
        Assert.assertTrue("Should contain Fragments section", result.contains("  Fragments:"));
        Assert.assertTrue("Should contain Fragment 0", result.contains("    Fragment 0:"));
        Assert.assertTrue("Should contain Fragment 1", result.contains("    Fragment 1:"));
        Assert.assertTrue("Should contain LoadChannels section", result.contains("  LoadChannels:"));
    }

    @Test
    public void testPrettyPrint() {
        TUniqueId queryId = new TUniqueId(1L, 2L);
        ExecutionProfile profile = new ExecutionProfile(queryId, Lists.newArrayList(0, 1));

        StringBuilder sb = new StringBuilder();
        profile.prettyPrint(sb, "  ");
        String result = sb.toString();

        Assert.assertTrue("Should contain proper indentation", result.contains("    Fragments:"));
        Assert.assertTrue("Should contain Fragment 0 with indentation", result.contains("      Fragment 0:"));
        Assert.assertTrue("Should contain Fragment 1 with indentation", result.contains("      Fragment 1:"));
        Assert.assertTrue("Should contain LoadChannels with indentation", result.contains("    LoadChannels:"));
    }

    @Test
    public void testGetPipelineAggregatedProfile() {
        TUniqueId queryId = new TUniqueId(1L, 2L);
        ExecutionProfile profile = new ExecutionProfile(queryId, Lists.newArrayList(0));

        // Mock two BE addresses
        TNetworkAddress be1 = new TNetworkAddress("127.0.0.1", 9050);
        TNetworkAddress be2 = new TNetworkAddress("127.0.0.2", 9050);

        // Create mock pipeline profiles for BE1
        List<RuntimeProfile> be1Profiles = Lists.newArrayList();
        RuntimeProfile pipeline0Be1 = new RuntimeProfile("Pipeline 0(host=" + be1 + ")");

        RuntimeProfile pipeline1Be1 = new RuntimeProfile("Pipeline 1(host=" + be1 + ")");
        be1Profiles.add(pipeline0Be1);
        be1Profiles.add(pipeline1Be1);

        pipeline0Be1.addChild(new RuntimeProfile("Task 1"), true);
        pipeline0Be1.addChild(new RuntimeProfile("Task 2"), true);
        pipeline1Be1.addChild(new RuntimeProfile("Task 1"), true);
        pipeline1Be1.addChild(new RuntimeProfile("Task 2"), true);

        // Create mock pipeline profiles for BE2
        List<RuntimeProfile> be2Profiles = Lists.newArrayList();
        RuntimeProfile pipeline0Be2 = new RuntimeProfile("Pipeline 0(host=" + be2 + ")");
        RuntimeProfile pipeline1Be2 = new RuntimeProfile("Pipeline 1(host=" + be2 + ")");
        be2Profiles.add(pipeline0Be2);
        be2Profiles.add(pipeline1Be2);

        pipeline0Be2.addChild(new RuntimeProfile("Task 1"), true);
        pipeline0Be2.addChild(new RuntimeProfile("Task 2"), true);
        pipeline1Be2.addChild(new RuntimeProfile("Task 1"), true);
        pipeline1Be2.addChild(new RuntimeProfile("Task 2"), true);

        // Set profiles for both BEs
        profile.setMultiBeProfile(0, be1, be1Profiles);
        profile.setMultiBeProfile(0, be2, be2Profiles);

        // Get aggregated profile
        RuntimeProfile result = profile.getPipelineAggregatedProfile(Maps.newHashMap());

        // Verify root structure
        Assert.assertEquals("Fragments", result.getName());
        List<Pair<RuntimeProfile, Boolean>> fragments = result.getChildList();
        Assert.assertEquals(1, fragments.size());

        // Verify Fragment structure
        RuntimeProfile fragment0 = fragments.get(0).first;
        Assert.assertEquals("Fragment 0", fragment0.getName());

        // Verify Pipeline structure
        List<Pair<RuntimeProfile, Boolean>> pipelines = fragment0.getChildList();
        Assert.assertEquals(2, pipelines.size());

        // Verify pipeline names and instance counts
        RuntimeProfile pipeline0 = pipelines.get(0).first;
        RuntimeProfile pipeline1 = pipelines.get(1).first;

        Assert.assertTrue("Pipeline 0 should contain instance count",
                pipeline0.getName().contains("Pipeline 0(instance_num=4)"));
        Assert.assertTrue("Pipeline 1 should contain instance count",
                pipeline1.getName().contains("Pipeline 1(instance_num=4)"));
    }
}
