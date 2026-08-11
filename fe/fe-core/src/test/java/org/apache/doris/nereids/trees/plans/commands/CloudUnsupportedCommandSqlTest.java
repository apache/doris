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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class CloudUnsupportedCommandSqlTest extends TestWithFeService {
    private static final List<String> UNSUPPORTED_SQL = Arrays.asList(
            "ALTER RESOURCE missing_cloud_resource PROPERTIES (\"s3.connection.maximum\" = \"100\")",
            "ALTER STORAGE POLICY missing_cloud_policy PROPERTIES (\"cooldown_ttl\" = \"86400\")");

    @Test
    public void testMissingCloudRestrictionsThroughSqlEntry() {
        String originalDeployMode = Config.deploy_mode;
        String originalCloudUniqueId = Config.cloud_unique_id;
        UserIdentity originalUserIdentity = connectContext.getCurrentUserIdentity();
        try {
            Config.deploy_mode = "cloud";
            Config.cloud_unique_id = "";

            for (UserIdentity userIdentity : Arrays.asList(UserIdentity.ADMIN, UserIdentity.ROOT)) {
                connectContext.setCurrentUserIdentity(userIdentity);
                for (String sql : UNSUPPORTED_SQL) {
                    IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                            () -> executeSql(sql));
                    Assertions.assertEquals("errCode = 2, detailMessage = Unsupported operation",
                            exception.getMessage());
                }
            }
        } finally {
            connectContext.setCurrentUserIdentity(originalUserIdentity);
            Config.deploy_mode = originalDeployMode;
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
