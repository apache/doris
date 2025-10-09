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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AdminSetAutoClusterSnapshotCommandTest {
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

    private void runBefore() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        Config.deploy_mode = "";
        Map<String, String> properties = new HashMap<>();
        properties.put("max_reserved_snapshots", "10");
        properties.put("snapshot_interval_seconds", "3600");
        AdminSetAutoClusterSnapshotCommand command = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "The sql is illegal in disk mode");

        Config.deploy_mode = "cloud";
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        // test invalid property
        properties.put("a", "test_s_label");
        AdminSetAutoClusterSnapshotCommand command1 = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate(connectContext), "Unknown property");
        properties.remove("a");

        // test invalid max_reserved_snapshots
        properties.put("max_reserved_snapshots", "a");
        AdminSetAutoClusterSnapshotCommand command2 = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "Invalid value");

        properties.put("max_reserved_snapshots", "-1");
        AdminSetAutoClusterSnapshotCommand command3 = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "value should in");

        properties.put("max_reserved_snapshots", "40");
        AdminSetAutoClusterSnapshotCommand command4 = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "value should in");
        properties.put("max_reserved_snapshots", "1");

        // test invalid snapshot_interval_seconds
        properties.put("snapshot_interval_seconds", "a");
        AdminSetAutoClusterSnapshotCommand command5 = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext),
                "Invalid value");

        properties.put("snapshot_interval_seconds", "1000");
        AdminSetAutoClusterSnapshotCommand command6 = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command6.validate(connectContext),
                "value minimum is");
        properties.put("snapshot_interval_seconds", "3600");
    }

    @Test
    public void testValidateNoPriviledge() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        Config.deploy_mode = "cloud";

        Map<String, String> properties = new HashMap<>();
        AdminSetAutoClusterSnapshotCommand command = new AdminSetAutoClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (ADMIN) privilege(s) for this operation");
    }
}
