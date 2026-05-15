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

import org.apache.doris.common.Status;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TProfileNodeReport;
import org.apache.doris.thrift.TProfileNodeType;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ExecutionProfileTest {

    @Test
    public void testUpdateProfileBuildsDisplayTreeFromStructuredProfileNodeType() {
        ExecutionProfile executionProfile = new ExecutionProfile(new TUniqueId(1, 2), Lists.newArrayList(0));
        TQueryProfile queryProfile = buildQueryProfile(Lists.newArrayList(
                buildProfileNodeReport(TProfileNodeType.FRAGMENT_LEVEL, -1, "fragment-level", false),
                buildProfileNodeReport(TProfileNodeType.PIPELINE_LEVEL, 7, "pipeline", false)));

        Status status = executionProfile.updateProfile(queryProfile, new TNetworkAddress("be1", 9050), false);

        Assert.assertEquals(TStatusCode.OK, status.getErrorCode());
        RuntimeProfile fragmentProfile = executionProfile.getRoot()
                .getChildMap().get("Fragments")
                .getChildMap().get("Fragment 0");
        Assert.assertTrue(fragmentProfile.getChildMap()
                .containsKey("FragmentLevelProfile:(host=TNetworkAddress(hostname:be1, port:9050))"));
        Assert.assertTrue(fragmentProfile.getChildMap()
                .containsKey("Pipeline 7(host=TNetworkAddress(hostname:be1, port:9050))"));
    }

    @Test
    public void testUpdateProfileOnlyAddsPipelineReportsToAggregatedProfile() {
        ExecutionProfile executionProfile = new ExecutionProfile(new TUniqueId(1, 2), Lists.newArrayList(0));
        TQueryProfile queryProfile = buildQueryProfile(Lists.newArrayList(
                buildProfileNodeReport(TProfileNodeType.FRAGMENT_LEVEL, -1, "fragment-level", true),
                buildProfileNodeReport(TProfileNodeType.PIPELINE_LEVEL, 7, "pipeline", true)));

        Status status = executionProfile.updateProfile(queryProfile, new TNetworkAddress("be1", 9050), false);

        Assert.assertEquals(TStatusCode.OK, status.getErrorCode());

        RuntimeProfile aggregatedProfile = executionProfile.getPipelineAggregatedProfile(Maps.newHashMap());
        RuntimeProfile aggregatedFragmentProfile = aggregatedProfile.getChildMap().get("Fragment 0");
        Assert.assertEquals(1, aggregatedFragmentProfile.getChildList().size());
        Assert.assertTrue(aggregatedFragmentProfile.getChildList().get(0).first.getName()
                .contains("Pipeline 0(instance_num=1)"));
    }

    private TQueryProfile buildQueryProfile(List<TProfileNodeReport> profileNodeReports) {
        TQueryProfile queryProfile = new TQueryProfile();
        queryProfile.setQueryId(new TUniqueId(1, 2));
        Map<Integer, List<TProfileNodeReport>> fragmentReports = Maps.newHashMap();
        fragmentReports.put(0, profileNodeReports);
        queryProfile.setFragmentIdToProfileNodeReports(fragmentReports);
        return queryProfile;
    }

    private TProfileNodeReport buildProfileNodeReport(TProfileNodeType type, int pipelineId, String name,
            boolean withTaskProfile) {
        TProfileNodeReport report = new TProfileNodeReport();
        report.setProfile(buildRuntimeProfileTree(name, withTaskProfile));
        report.setProfileNodeType(type);
        if (type == TProfileNodeType.PIPELINE_LEVEL) {
            report.setPipelineId(pipelineId);
        }
        return report;
    }

    private TRuntimeProfileTree buildRuntimeProfileTree(String name, boolean withTaskProfile) {
        TRuntimeProfileTree profileTree = new TRuntimeProfileTree();
        TRuntimeProfileNode root = new TRuntimeProfileNode();
        root.setName(name);
        root.setNumChildren(withTaskProfile ? 1 : 0);
        root.setCounters(Lists.newArrayList());
        root.setMetadata(0);
        root.setIndent(true);
        root.setInfoStrings(Maps.newHashMap());
        root.setInfoStringsDisplayOrder(Lists.newArrayList());
        root.setChildCountersMap(Maps.newHashMap());
        root.setTimestamp(-1);
        profileTree.addToNodes(root);
        if (withTaskProfile) {
            profileTree.addToNodes(buildRuntimeProfileNode("task"));
        }
        return profileTree;
    }

    private TRuntimeProfileNode buildRuntimeProfileNode(String name) {
        TRuntimeProfileNode node = new TRuntimeProfileNode();
        node.setName(name);
        node.setNumChildren(0);
        node.setCounters(Lists.newArrayList());
        node.setMetadata(0);
        node.setIndent(true);
        node.setInfoStrings(Maps.newHashMap());
        node.setInfoStringsDisplayOrder(Lists.newArrayList());
        node.setChildCountersMap(Maps.newHashMap());
        node.setTimestamp(-1);
        return node;
    }
}
