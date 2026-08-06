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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartitionsProcDirTest {
    private String originDeployMode;
    private String originCloudUniqueId;

    @Before
    public void setUp() {
        originDeployMode = Config.deploy_mode;
        originCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originDeployMode;
        Config.cloud_unique_id = originCloudUniqueId;
    }

    @Test
    public void testDisplayInNonCloudMode() {
        Assert.assertEquals("HDD", PartitionsProcDir.getStorageMediumDisplay("HDD"));
        Assert.assertEquals("tag.location.default: 1",
                PartitionsProcDir.getReplicaAllocationDisplay("tag.location.default: 1"));
    }

    @Test
    public void testDisplayInCloudMode() {
        Config.deploy_mode = "cloud";
        Assert.assertEquals(PartitionsProcDir.CLOUD_STORAGE_MEDIUM_DISPLAY,
                PartitionsProcDir.getStorageMediumDisplay("HDD"));
        Assert.assertEquals(FeConstants.null_string,
                PartitionsProcDir.getReplicaAllocationDisplay("tag.location.default: 1"));
    }
}
