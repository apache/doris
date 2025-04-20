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

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class GrantResourcePrivilegeCommandTest extends TestWithFeService {
    @Test
    public void testValidate() {
        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALL));
        ResourcePattern resourcePattern = new ResourcePattern("test", ResourceTypeEnum.CLUSTER);
        GrantResourcePrivilegeCommand command = new GrantResourcePrivilegeCommand(
                privileges,
                Optional.of(resourcePattern),
                Optional.empty(),
                Optional.of("test"),
                Optional.empty());
        Assertions.assertDoesNotThrow(() -> command.validate());
    }

    @Test
    public void testResource() throws DdlException {
        String resourceQuery = "GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(resourceQuery);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan).validate());
        Env.getCurrentEnv().getAuth().grantResourcePrivilegeCommand((GrantResourcePrivilegeCommand) plan);

        resourceQuery = "GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';";
        LogicalPlan plan1 = nereidsParser.parseSingle(resourceQuery);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan1).validate());
        Env.getCurrentEnv().getAuth().grantResourcePrivilegeCommand((GrantResourcePrivilegeCommand) plan1);

        resourceQuery = "GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';";
        LogicalPlan plan2 = nereidsParser.parseSingle(resourceQuery);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan2).validate());
        Env.getCurrentEnv().getAuth().grantResourcePrivilegeCommand((GrantResourcePrivilegeCommand) plan2);
    }

    @Test
    public void testWorkload() throws DdlException {
        String workGroupQuery = "GRANT USAGE_PRIV ON WORKLOAD GROUP 'g1' TO ROLE 'my_role';";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(workGroupQuery);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan).validate());
        Env.getCurrentEnv().getAuth().grantResourcePrivilegeCommand((GrantResourcePrivilegeCommand) plan);

        workGroupQuery = "GRANT USAGE_PRIV ON WORKLOAD GROUP 'g1' TO 'jack'@'%';";
        LogicalPlan plan1 = nereidsParser.parseSingle(workGroupQuery);
        Assertions.assertTrue(plan1 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan1).validate());
        Env.getCurrentEnv().getAuth().grantResourcePrivilegeCommand((GrantResourcePrivilegeCommand) plan1);

        workGroupQuery = "GRANT USAGE_PRIV ON WORKLOAD GROUP '%' TO 'jack'@'%';";
        LogicalPlan plan2 = nereidsParser.parseSingle(workGroupQuery);
        Assertions.assertTrue(plan2 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan2).validate());
        Env.getCurrentEnv().getAuth().grantResourcePrivilegeCommand((GrantResourcePrivilegeCommand) plan2);
    }
}
