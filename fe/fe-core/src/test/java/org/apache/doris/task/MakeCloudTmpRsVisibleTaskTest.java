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

package org.apache.doris.task;

import org.apache.doris.thrift.TMakeCloudTmpRsVisibleRequest;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MakeCloudTmpRsVisibleTaskTest {
    @Test
    public void testToThriftIncludesLastActiveClusterInfo() {
        MakeCloudTmpRsVisibleTask task = new MakeCloudTmpRsVisibleTask(
                100L, 200L, List.of(300L, 301L), Map.of(400L, 500L), 600L,
                "cluster_b", List.of(300L));

        TMakeCloudTmpRsVisibleRequest request = task.toThrift();

        Assert.assertEquals("cluster_b", request.getLoadClusterId());
        Assert.assertEquals(List.of(300L), request.getLastActiveTabletIds());
    }
}
