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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AddBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddObserverOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterLoadErrorUrlOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropAllBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropObserverOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyFrontendOrBackendHostNameOp;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AlterSystemCommandTest {
    @Mocked
    private Env env;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private AccessControllerManager accessControllerManager;
    private List<String> hostPorts = ImmutableList.of("127.0.0.1:9050", "127.0.0.1:19050");
    private List<String> hostPortsErr = ImmutableList.of("127.0.0.1:89050", "355.0.0.1:19050");
    private Map<String, String> properties = ImmutableMap.of();
    private String brokerName = "brocker1";

    @Test
    void testValidateNormal() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.OPERATOR);
                minTimes = 0;
                result = true;
            }
        };
        // test addBackend
        AlterSystemCommand addBackend = new AlterSystemCommand(
                new AddBackendOp(hostPorts, properties), PlanType.ALTER_SYSTEM_ADD_BACKEND);
        Assertions.assertDoesNotThrow(() -> addBackend.validate(connectContext));

        // test addBackend with wrong port
        AlterSystemCommand addBackend2 = new AlterSystemCommand(
                new AddBackendOp(hostPortsErr, properties), PlanType.ALTER_SYSTEM_ADD_BACKEND);
        Assertions.assertThrows(AnalysisException.class, () -> addBackend2.validate(connectContext),
                "Encounter unknown exception: errCode = 2, detailMessage = Port is out of range: 89050");

        Map<String, String> properties = ImmutableMap.of("tag.compute_group_name", "another_compute_group",
                "tag.location", "location");
        AlterSystemCommand addBackend3 = new AlterSystemCommand(
                new AddBackendOp(hostPorts, properties), PlanType.ALTER_SYSTEM_ADD_BACKEND);
        Assertions.assertDoesNotThrow(() -> addBackend3.validate(connectContext));

        // test dropBackend
        AlterSystemCommand dropBackend = new AlterSystemCommand(
                new DropBackendOp(hostPorts, false), PlanType.ALTER_SYSTEM_DROP_BACKEND);
        Assertions.assertDoesNotThrow(() -> dropBackend.validate(connectContext));

        AlterSystemCommand dropBackend2 = new AlterSystemCommand(
                new DropBackendOp(hostPorts, true), PlanType.ALTER_SYSTEM_DROP_BACKEND);
        Assertions.assertDoesNotThrow(() -> dropBackend2.validate(connectContext));

        // test addObserver
        AlterSystemCommand addObserver = new AlterSystemCommand(
                new AddObserverOp(hostPorts.get(0)), PlanType.ALTER_SYSTEM_ADD_OBSERVER);
        Assertions.assertDoesNotThrow(() -> addObserver.validate(connectContext));

        // test dropObserver
        AlterSystemCommand dropObserver = new AlterSystemCommand(
                new DropObserverOp(hostPorts.get(0)), PlanType.ALTER_SYSTEM_DROP_OBSERVER);
        Assertions.assertDoesNotThrow(() -> dropObserver.validate(connectContext));

        // test addFollower
        AlterSystemCommand addFollower = new AlterSystemCommand(
                new AddFollowerOp(hostPorts.get(0)), PlanType.ALTER_SYSTEM_ADD_FOLLOWER);
        Assertions.assertDoesNotThrow(() -> addFollower.validate(connectContext));

        // test dropFollower
        AlterSystemCommand dropFollower = new AlterSystemCommand(
                new DropFollowerOp(hostPorts.get(0)), PlanType.ALTER_SYSTEM_DROP_FOLLOWER);
        Assertions.assertDoesNotThrow(() -> dropFollower.validate(connectContext));

        // test addBroker
        AlterSystemCommand addBroker = new AlterSystemCommand(
                new AddBrokerOp(brokerName, hostPorts), PlanType.ALTER_SYSTEM_ADD_BROKER);
        Assertions.assertDoesNotThrow(() -> addBroker.validate(connectContext));

        // test dropBroker
        AlterSystemCommand dropBroker = new AlterSystemCommand(
                new DropBrokerOp(brokerName, hostPorts), PlanType.ALTER_SYSTEM_DROP_BROKER);
        Assertions.assertDoesNotThrow(() -> dropBroker.validate(connectContext));

        // test dropAllBroker
        AlterSystemCommand dropAllBroker = new AlterSystemCommand(
                new DropAllBrokerOp(brokerName), PlanType.ALTER_SYSTEM_DROP_ALL_BROKER);
        Assertions.assertDoesNotThrow(() -> dropAllBroker.validate(connectContext));

        // test alterLoadErrorUrl
        AlterSystemCommand alterLoadErrorUrl = new AlterSystemCommand(
                new AlterLoadErrorUrlOp(properties), PlanType.ALTER_SYSTEM_SET_LOAD_ERRORS_HU);
        Assertions.assertDoesNotThrow(() -> alterLoadErrorUrl.validate(connectContext));

        // test modifyBackend
        AlterSystemCommand modifyBackend = new AlterSystemCommand(
                new ModifyBackendOp(hostPorts, properties), PlanType.ALTER_SYSTEM_MODIFY_BACKEND);
        Assertions.assertDoesNotThrow(() -> modifyBackend.validate(connectContext));

        // test modifyFrontendOrBackendHostName
        for (ModifyFrontendOrBackendHostNameOp.ModifyOpType type : ModifyFrontendOrBackendHostNameOp.ModifyOpType.values()) {
            for (String newHost : Arrays.asList("localhost", "192.168.10.10")) {
                AlterSystemCommand command = new AlterSystemCommand(
                        new ModifyFrontendOrBackendHostNameOp(hostPorts.get(0), newHost, type),
                        PlanType.ALTER_SYSTEM_MODIFY_FRONTEND_OR_BACKEND_HOSTNAME);
                Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
            }
            for (String newHost : Arrays.asList("localhost2", "192.168.10.300")) {
                AlterSystemCommand command = new AlterSystemCommand(
                        new ModifyFrontendOrBackendHostNameOp(hostPorts.get(0), newHost, type),
                        PlanType.ALTER_SYSTEM_MODIFY_FRONTEND_OR_BACKEND_HOSTNAME);
                Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                        "Unknown hostname:  " + newHost + ": Name or service not known");
            }
        }
    }

    @Test
    void testValidateNoPrivilege() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.OPERATOR);
                minTimes = 0;
                result = false;
            }
        };

        // test addBackend with no privilege
        AlterSystemCommand addBackend = new AlterSystemCommand(
                new AddBackendOp(hostPorts, properties), PlanType.ALTER_SYSTEM_ADD_BACKEND);
        Assertions.assertThrows(AnalysisException.class, () -> addBackend.validate(connectContext),
                "Access denied; you need (at least one of) the (NODE) privilege(s) for this operation");
    }
}

