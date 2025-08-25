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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RevokeResourcePrivilegeCommandTest extends TestWithFeService {
    @Test
    public void testResource() {
        String createUserSql = "CREATE USER 'jack'";
        String createRoleSql = "CREATE ROLE role1";
        String resourceSql = "GRANT USAGE_PRIV ON RESOURCE 'jdbc_resource' TO 'jack'@'%';";
        String createJdbcResourceSql = "CREATE EXTERNAL RESOURCE \"jdbc_resource\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + " \"type\" = \"jdbc\",\n"
                + " \"user\" = \"jdbc_user\",\n"
                + " \"password\" = \"jdbc_passwd\",\n"
                + " \"jdbc_url\" = \"jdbc:mysql://127.0.0.1:3316/doris_test?useSSL=false\",\n"
                + " \"driver_url\" = \"https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar\",\n"
                + " \"driver_class\" = \"com.mysql.cj.jdbc.Driver\"\n"
                + ");";

        NereidsParser nereidsParser = new NereidsParser();

        LogicalPlan logicalPlan = nereidsParser.parseSingle(createUserSql);
        Assertions.assertTrue(logicalPlan instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) logicalPlan).run(connectContext, null));

        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(createRoleSql);
        Assertions.assertTrue(logicalPlan1 instanceof CreateRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalPlan1).run(connectContext, null));

        LogicalPlan logicalPlan2 = nereidsParser.parseSingle(createJdbcResourceSql);
        Assertions.assertTrue(logicalPlan2 instanceof CreateResourceCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateResourceCommand) logicalPlan2).run(connectContext, null));

        LogicalPlan plan = nereidsParser.parseSingle(resourceSql);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan).run(connectContext, null));

        resourceSql = "GRANT USAGE_PRIV ON RESOURCE 'jdbc_resource' TO ROLE 'role1';";
        LogicalPlan plan1 = nereidsParser.parseSingle(resourceSql);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan1).run(connectContext, null));

        resourceSql = "GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';";
        LogicalPlan plan2 = nereidsParser.parseSingle(resourceSql);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan2).run(connectContext, null));

        String revokeSql = "REVOKE USAGE_PRIV ON RESOURCE 'jdbc_resource' FROM ROLE 'role1';";
        LogicalPlan revokeplan1 = nereidsParser.parseSingle(revokeSql);
        Assertions.assertTrue(revokeplan1 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan1).run(connectContext, null));

        revokeSql = "REVOKE USAGE_PRIV ON RESOURCE * FROM 'jack'@'%';";
        LogicalPlan revokeplan2 = nereidsParser.parseSingle(revokeSql);
        Assertions.assertTrue(revokeplan2 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan2).run(connectContext, null));
    }

    @Test
    public void testWorkload() {
        String createWorkLoadSql = "create workload group if not exists g1 \n"
                + "properties (  \n"
                + "\"min_memory_percent\"=\"10\", \n"
                + "\"max_memory_percent\"=\"30%\" \n"
                + ");";
        String createUserSql = "CREATE USER 'jack1'";
        String createRoleSql = "CREATE ROLE role2";
        String workGroupSql = "GRANT USAGE_PRIV ON WORKLOAD GROUP 'g1' TO ROLE 'role2';";

        NereidsParser nereidsParser = new NereidsParser();

        LogicalPlan logicalPlan = nereidsParser.parseSingle(createUserSql);
        Assertions.assertTrue(logicalPlan instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) logicalPlan).run(connectContext, null));

        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(createRoleSql);
        Assertions.assertTrue(logicalPlan1 instanceof CreateRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalPlan1).run(connectContext, null));

        LogicalPlan logicalPlan2 = nereidsParser.parseSingle(createWorkLoadSql);
        Assertions.assertTrue(logicalPlan2 instanceof CreateWorkloadGroupCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateWorkloadGroupCommand) logicalPlan2).run(connectContext, null));

        LogicalPlan plan = nereidsParser.parseSingle(workGroupSql);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan).run(connectContext, null));

        workGroupSql = "GRANT USAGE_PRIV ON WORKLOAD GROUP 'g1' TO 'jack1'@'%';";
        LogicalPlan plan1 = nereidsParser.parseSingle(workGroupSql);
        Assertions.assertTrue(plan1 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan1).run(connectContext, null));

        workGroupSql = "GRANT USAGE_PRIV ON WORKLOAD GROUP '%' TO 'jack1'@'%';";
        LogicalPlan plan2 = nereidsParser.parseSingle(workGroupSql);
        Assertions.assertTrue(plan2 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan2).run(connectContext, null));

        String revokeSql = "REVOKE USAGE_PRIV ON WORKLOAD GROUP 'g1' FROM 'jack1'@'%';";
        LogicalPlan revokeplan1 = nereidsParser.parseSingle(revokeSql);
        Assertions.assertTrue(revokeplan1 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan1).run(connectContext, null));

        revokeSql = "REVOKE USAGE_PRIV ON WORKLOAD GROUP '%' FROM 'jack1'@'%';";
        LogicalPlan revokeplan2 = nereidsParser.parseSingle(revokeSql);
        Assertions.assertTrue(revokeplan2 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan2).run(connectContext, null));
    }
}
