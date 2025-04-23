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

public class RevokeRoleCommandTest extends TestWithFeService {
    @Test
    public void testValidate() throws DdlException {
        RevokeRoleCommand grantRoleCommand = new RevokeRoleCommand(new UserIdentity("test", "%"), Arrays.asList("test"));
        Assertions.assertDoesNotThrow(() -> grantRoleCommand.validate());

        Env.getCurrentEnv().getAuth().createRole("test", false, "");

        String query = "REVOKE 'test' FROM 'admin';";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(query);
        Assertions.assertTrue(plan instanceof RevokeRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeRoleCommand) plan).validate());
        Env.getCurrentEnv().getAuth().revokeRole((RevokeRoleCommand) plan);
    }

    @Test
    public void testUserFail() {
        RevokeRoleCommand grantRoleCommand = new RevokeRoleCommand(new UserIdentity("", "%"), null);
        Assertions.assertThrows(AnalysisException.class, () -> grantRoleCommand.validate());

        RevokeRoleCommand grantRoleCommand1 = new RevokeRoleCommand(new UserIdentity("", "%"), Arrays.asList("test"));
        Assertions.assertThrows(AnalysisException.class, () -> grantRoleCommand1.validate());
    }
}
