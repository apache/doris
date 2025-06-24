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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class RevokeRoleCommandTest extends TestWithFeService {
    @Test
    public void testValidate() throws DdlException {
        GrantRoleCommand grantRoleCommand = new GrantRoleCommand(new UserIdentity("test", "%"), Arrays.asList("test"));
        Assertions.assertDoesNotThrow(() -> grantRoleCommand.validate());

        String grantRoleSql = "GRANT 'role1','role2' TO 'jack'@'%'";
        String createUserSql = "CREATE USER 'jack'";
        String role1 = "CREATE ROLE role1";
        String role2 = "CREATE ROLE role2";

        NereidsParser nereidsParser = new NereidsParser();

        LogicalPlan logicalPlan = nereidsParser.parseSingle(createUserSql);
        Assertions.assertTrue(logicalPlan instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) logicalPlan).run(connectContext, null));

        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(role1);
        Assertions.assertTrue(logicalPlan1 instanceof CreateRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalPlan1).run(connectContext, null));

        LogicalPlan logicalPlan2 = nereidsParser.parseSingle(role2);
        Assertions.assertTrue(logicalPlan2 instanceof CreateRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalPlan2).run(connectContext, null));

        LogicalPlan logicalPlan3 = nereidsParser.parseSingle(grantRoleSql);
        Assertions.assertTrue(logicalPlan3 instanceof GrantRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantRoleCommand) logicalPlan3).run(connectContext, null));

        String revkeSql = "REVOKE 'role1','role2' FROM 'jack'@'%';";
        LogicalPlan plan = nereidsParser.parseSingle(revkeSql);
        Assertions.assertTrue(plan instanceof RevokeRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeRoleCommand) plan).run(connectContext, null));
    }

    @Test
    public void testUserFail() {
        Assertions.assertThrows(NullPointerException.class, () -> new RevokeRoleCommand(new UserIdentity("", "%"), null));

        RevokeRoleCommand grantRoleCommand = new RevokeRoleCommand(new UserIdentity("", "%"), Arrays.asList("test"));
        Assertions.assertThrows(AnalysisException.class, () -> grantRoleCommand.validate());
    }
}
