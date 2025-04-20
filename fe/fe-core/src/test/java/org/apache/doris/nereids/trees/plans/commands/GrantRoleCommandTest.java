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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class GrantRoleCommandTest extends TestWithFeService {
    @Test
    public void testValidate() throws DdlException {
        GrantRoleCommand grantRoleCommand = new GrantRoleCommand(new UserIdentity("test", "%"), Arrays.asList("test"));
        Assertions.assertDoesNotThrow(() -> grantRoleCommand.validate());

        String query = "GRANT 'role1','role2' TO 'jack'@'%'";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(query);
        Assertions.assertTrue(plan instanceof GrantRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantRoleCommand) plan).validate());
        Env.getCurrentEnv().getAuth().grantRoleCommand((GrantRoleCommand) plan);
    }

    @Test
    public void testUserFail() {
        GrantRoleCommand grantRoleCommand = new GrantRoleCommand(new UserIdentity("", "%"), null);
        Assertions.assertThrows(AnalysisException.class, () -> grantRoleCommand.validate());

        GrantRoleCommand grantRoleCommand1 = new GrantRoleCommand(new UserIdentity("", "%"), Arrays.asList("test"));
        Assertions.assertThrows(AnalysisException.class, () -> grantRoleCommand1.validate());
    }
}
