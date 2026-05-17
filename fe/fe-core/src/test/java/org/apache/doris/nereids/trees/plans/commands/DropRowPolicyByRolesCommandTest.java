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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;

public class DropRowPolicyByRolesCommandTest extends TestWithFeService {
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.GRANT));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        DropRowPolicyByRolesCommand command = new DropRowPolicyByRolesCommand(
                Arrays.asList("role1", "role2"));
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testValidateNoPrivilege() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.GRANT));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        DropRowPolicyByRolesCommand command = new DropRowPolicyByRolesCommand(
                Arrays.asList("role1"));
        Assertions.assertThrows(Exception.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateEmptyRoleName() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.GRANT));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        DropRowPolicyByRolesCommand command = new DropRowPolicyByRolesCommand(
                Arrays.asList(""));
        Assertions.assertThrows(Exception.class, () -> command.validate(connectContext));
    }

    @Test
    public void testGetRoleNames() {
        DropRowPolicyByRolesCommand command = new DropRowPolicyByRolesCommand(
                Arrays.asList("role1", "role2", "role3"));
        Assertions.assertEquals(Arrays.asList("role1", "role2", "role3"), command.getRoleNames());
    }

    @Test
    public void testParseBacktickRoleName() {
        NereidsParser parser = new NereidsParser();
        String[] sqls = {
            "DROP ROW POLICY FOR ROLE role1",
            "DROP ROW POLICY FOR ROLE role1, role2",
            "DROP ROW POLICY FOR ROLE `test-role`",
            "DROP ROW POLICY FOR ROLE `test-role`, role2",
        };
        for (String sql : sqls) {
            Assertions.assertDoesNotThrow(() -> {
                Plan plan = parser.parseSingle(sql);
                Assertions.assertInstanceOf(DropRowPolicyByRolesCommand.class, plan);
            }, "Failed to parse: " + sql);
        }
    }
}
