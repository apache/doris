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

package org.apache.doris.qe;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class LoadContextTest {
    @Test
    public void testErrorTabletsAddSharedNothingParticipationMarkers() {
        String oldDeployMode = Config.deploy_mode;
        String oldCloudUniqueId = Config.cloud_unique_id;
        int oldMaxErrorTablets = Config.max_error_tablet_of_broker_load;
        try {
            Config.deploy_mode = "";
            Config.cloud_unique_id = "";
            Config.max_error_tablet_of_broker_load = 0;
            LoadContext context = new LoadContext();
            TErrorTabletInfo first = new TErrorTabletInfo();
            first.setTabletId(100L);
            first.setMsg("first destination failed");
            TErrorTabletInfo second = new TErrorTabletInfo();
            second.setTabletId(100L);
            second.setMsg("second destination failed");

            context.updateErrorTabletInfos(Arrays.asList(first, second));
            List<TTabletCommitInfo> commitInfos = context.getCommitInfos();
            Assertions.assertEquals(1, commitInfos.size());
            Assertions.assertEquals(100L, commitInfos.get(0).getTabletId());
            Assertions.assertEquals(TabletCommitInfo.PARTITION_INVOLVEMENT_BACKEND_ID,
                    commitInfos.get(0).getBackendId());
            Assertions.assertTrue(context.getErrorTabletInfos().isEmpty());
        } finally {
            Config.deploy_mode = oldDeployMode;
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.max_error_tablet_of_broker_load = oldMaxErrorTablets;
        }
    }

    @Test
    public void testCloudErrorTabletsDoNotAddParticipationMarkers() {
        String oldDeployMode = Config.deploy_mode;
        String oldCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.deploy_mode = "cloud";
            Config.cloud_unique_id = "cloud-test";
            LoadContext context = new LoadContext();
            TErrorTabletInfo error = new TErrorTabletInfo();
            error.setTabletId(100L);

            context.updateErrorTabletInfos(Arrays.asList(error));
            Assertions.assertTrue(context.getCommitInfos().isEmpty());
        } finally {
            Config.deploy_mode = oldDeployMode;
            Config.cloud_unique_id = oldCloudUniqueId;
        }
    }
}
