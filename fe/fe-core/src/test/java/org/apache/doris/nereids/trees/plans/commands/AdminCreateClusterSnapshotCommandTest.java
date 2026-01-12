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
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdminCreateClusterSnapshotCommandTest {
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
        AdminCreateClusterSnapshotCommand command = new AdminCreateClusterSnapshotCommand(new HashMap<>());
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "The sql is illegal in disk mode");

        Config.deploy_mode = "cloud";
        List<Pair<Map<String, String>, String>> properties = new ArrayList<>();
        // missing property
        properties.add(Pair.of(new HashMap<>(), "Property 'ttl' is required."));
        properties.add(Pair.of(ImmutableMap.of("label", "a"), "Property 'ttl' is required"));
        properties.add(Pair.of(ImmutableMap.of("ttl", "3600"), "Property 'label' is required."));
        // invalid value
        properties.add(Pair.of(ImmutableMap.of("ttl", "a", "label", "a"), "Invalid value"));
        properties.add(Pair.of(ImmutableMap.of("ttl", "0", "label", "a"), "Property 'ttl' must be positive"));
        properties.add(Pair.of(ImmutableMap.of("ttl", "3600", "label", ""), "Property 'label' cannot be empty"));
        // unknown property
        properties.add(Pair.of(ImmutableMap.of("ttl", "0", "a", "b"), "Unknown property"));
        // normal case
        properties.add(Pair.of(ImmutableMap.of("ttl", "3600", "label", "abc"), ""));

        for (Pair<Map<String, String>, String> entry : properties) {
            AdminCreateClusterSnapshotCommand command0 = new AdminCreateClusterSnapshotCommand(entry.first);
            String error = entry.second;
            if (error.isEmpty()) {
                command0.validate(connectContext);
            } else {
                Assertions.assertThrows(AnalysisException.class, () -> command0.validate(connectContext), error);
            }
        }
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
        AdminCreateClusterSnapshotCommand command = new AdminCreateClusterSnapshotCommand(properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (ADMIN) privilege(s) for this operation");
    }
}
