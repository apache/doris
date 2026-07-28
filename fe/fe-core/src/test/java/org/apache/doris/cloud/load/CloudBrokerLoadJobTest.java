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

package org.apache.doris.cloud.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class CloudBrokerLoadJobTest {
    @Test
    public void testBuildConnectContextKeepsStableComputeGroupIdAfterRename() throws Exception {
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfoService.getClusterNameByClusterId("compute-group-id"))
                .thenReturn("renamed-compute-group");
        Env env = Mockito.mock(Env.class);
        CloudBrokerLoadJob job = new CloudBrokerLoadJob();
        Map<String, String> sessionVariables = Deencapsulation.getField(job, "sessionVariables");
        sessionVariables.put(CloudBrokerLoadJob.CLOUD_CLUSTER_ID, "compute-group-id");

        ConnectContext.remove();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            try (AutoCloseConnectContext ignored = Deencapsulation.invoke(job, "buildConnectContext")) {
                Assert.assertEquals("renamed-compute-group",
                        ConnectContext.get().getSessionVariable().getCloudCluster());
                Assert.assertEquals("compute-group-id", ConnectContext.get().getComputeGroupId());
            }
        } finally {
            ConnectContext.remove();
        }
    }
}
