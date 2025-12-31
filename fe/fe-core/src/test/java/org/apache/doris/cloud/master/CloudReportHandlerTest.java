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

package org.apache.doris.cloud.master;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Test;

import java.util.HashMap;

public class CloudReportHandlerTest {

    @Mocked
    private CloudEnv mockCloudEnv;

    @Test
    public void testTabletReportWhenTabletRebalancerNotInitialized() {
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return mockCloudEnv;
            }
        };
        new Expectations() {
            {
                mockCloudEnv.isRebalancerInited();
                result = false;
            }
        };

        CloudReportHandler handler = new CloudReportHandler();
        handler.tabletReport(1001L, new HashMap<>(), new HashMap<>(), 1L, 10L);
        // If the tabletRebalancer is not initialized,
        // the tabletReport should not call mockCloudEnv.getCurrentSystemInfo
        new Verifications() {
            {
                mockCloudEnv.getCurrentSystemInfo();
                times = 0;
            }
        };
    }
}
