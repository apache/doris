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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.BrokerPendingTaskAttachment;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

class CloudBrokerLoadJobTest {
    @Test
    void testWaitForAutoStartBeforePlanningLoadingTask() throws Exception {
        String originalCloudUniqueId = Config.cloud_unique_id;
        ConnectContext.remove();
        TestCloudBrokerLoadJob loadJob = new TestCloudBrokerLoadJob();
        loadJob.setComputeGroupId("compute-group-id");
        BrokerPendingTaskAttachment attachment = Mockito.mock(BrokerPendingTaskAttachment.class);
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Env env = Mockito.mock(Env.class);
        DdlException autoStartFailure = new DdlException("compute group is manually shut down");
        Mockito.when(systemInfoService.getClusterNameByClusterId("compute-group-id"))
                .thenReturn("current-compute-group");
        Mockito.when(systemInfoService.waitForAutoStart("current-compute-group")).thenThrow(autoStartFailure);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "test-cloud";
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            DdlException thrown = Assertions.assertThrows(
                    DdlException.class, () -> loadJob.createTaskForTest(attachment));

            Assertions.assertSame(autoStartFailure, thrown);
            Mockito.verify(systemInfoService).waitForAutoStart("current-compute-group");
            Mockito.verify(attachment, Mockito.never()).getFileStatusByTable(Mockito.any());
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    private static class TestCloudBrokerLoadJob extends CloudBrokerLoadJob {
        void setComputeGroupId(String computeGroupId) {
            sessionVariables.put(CLOUD_CLUSTER_ID, computeGroupId);
        }

        void createTaskForTest(BrokerPendingTaskAttachment attachment) throws UserException {
            createTask(null, null, Collections.emptyList(), false, 0, null, attachment);
        }
    }
}
