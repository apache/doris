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

package org.apache.doris.qe.runtime;

import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDetailedReportParams;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class PipelineExecutionTaskBuilderTest {
    @Test
    void buildSingleFragmentPipelineTaskRegistersBackendWithExecutionProfile() {
        TUniqueId queryId = new TUniqueId(1, 2);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, Lists.newArrayList(0));
        Backend backend = new Backend(1L, "127.0.0.1", 9050);

        Map<Integer, SingleFragmentPipelineTask> tasks =
                PipelineExecutionTaskBuilder.buildSingleFragmentPipelineTask(
                        executionProfile, backend, createFragmentParamsList(0));

        Assertions.assertEquals(1, tasks.size());
        Assertions.assertTrue(tasks.containsKey(0));

        executionProfile.updateProfile(createQueryProfile(queryId, 0), backend.getHeartbeatAddress(), true);
        Assertions.assertTrue(executionProfile.isCompleted());
    }

    private static TPipelineFragmentParamsList createFragmentParamsList(int fragmentId) {
        TPipelineInstanceParams instanceParams = new TPipelineInstanceParams();
        instanceParams.setFragmentInstanceId(new TUniqueId(3, 4));

        TPipelineFragmentParams fragmentParams = new TPipelineFragmentParams();
        fragmentParams.setFragmentId(fragmentId);
        fragmentParams.setLocalParams(Lists.newArrayList(instanceParams));

        TPipelineFragmentParamsList paramsList = new TPipelineFragmentParamsList();
        paramsList.setParamsList(Lists.newArrayList(fragmentParams));
        return paramsList;
    }

    private static TQueryProfile createQueryProfile(TUniqueId queryId, int fragmentId) {
        TQueryProfile queryProfile = new TQueryProfile();
        queryProfile.setQueryId(queryId);
        queryProfile.putToFragmentIdToProfile(fragmentId, Lists.newArrayList(createReportParam()));
        return queryProfile;
    }

    private static TDetailedReportParams createReportParam() {
        TRuntimeProfileNode node = new TRuntimeProfileNode();
        node.setName("PipelineProfile");
        node.setNumChildren(0);
        node.setCounters(Lists.newArrayList());
        node.setMetadata(0);
        node.setIndent(false);
        node.setInfoStrings(Maps.newHashMap());
        node.setInfoStringsDisplayOrder(Lists.newArrayList());
        node.setChildCountersMap(Maps.newHashMap());
        node.setTimestamp(0);

        TRuntimeProfileTree tree = new TRuntimeProfileTree();
        tree.setNodes(Lists.newArrayList(node));

        TDetailedReportParams params = new TDetailedReportParams();
        params.setProfile(tree);
        params.setIsFragmentLevel(false);
        return params;
    }
}
