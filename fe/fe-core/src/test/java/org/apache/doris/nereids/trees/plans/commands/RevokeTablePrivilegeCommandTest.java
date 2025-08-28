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

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class RevokeTablePrivilegeCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        String createTableStr = "create table test.test_revoke_table(d1 date, k1 int, k2 bigint)"
                + "duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1)"
                + "(PARTITION p20210901 VALUES [('2021-09-01'), ('2021-09-02')))"
                + "distributed by hash(k1) buckets 2 "
                + "properties('replication_num' = '1');";
        createTable(createTableStr);
    }

    @Test
    public void testValidate() {
        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALL));
        TablePattern tablePattern = new TablePattern("test", "test_revoke_table");
        RevokeTablePrivilegeCommand command = new RevokeTablePrivilegeCommand(
                privileges,
                tablePattern,
                Optional.empty(),
                Optional.of("test"));
        Assertions.assertDoesNotThrow(() -> command.validate());
    }

    @Test
    public void testRevokeTable() {
        String grantTablePrivilegeSql = "GRANT LOAD_PRIV ON internal.test.* TO ROLE 'role_reovke_table';";
        String grantTablePrivilegeSql1 = "GRANT LOAD_PRIV,ALTER_PRIV,LOAD_PRIV ON test.test_revoke_table  TO 'jack_revoke';";
        String createRoleSql = "CREATE ROLE role_reovke_table";
        String createUserSql = "CREATE USER 'jack_revoke'";

        NereidsParser nereidsParser = new NereidsParser();

        LogicalPlan logicalPlan = nereidsParser.parseSingle(createRoleSql);
        Assertions.assertTrue(logicalPlan instanceof CreateRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalPlan).run(connectContext, null));

        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(createUserSql);
        Assertions.assertTrue(logicalPlan1 instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) logicalPlan1).run(connectContext, null));

        // grant table
        LogicalPlan plan = nereidsParser.parseSingle(grantTablePrivilegeSql);
        Assertions.assertTrue(plan instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan).run(connectContext, null));

        LogicalPlan plan1 = nereidsParser.parseSingle(grantTablePrivilegeSql1);
        Assertions.assertTrue(plan1 instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan1).run(connectContext, null));

        // revoke table
        String revokeSql = "REVOKE LOAD_PRIV ON internal.test.* FROM ROLE 'role_reovke_table';";
        LogicalPlan revokeplan1 = nereidsParser.parseSingle(revokeSql);
        Assertions.assertTrue(revokeplan1 instanceof RevokeTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeTablePrivilegeCommand) revokeplan1).run(connectContext, null));

        revokeSql = "REVOKE LOAD_PRIV,ALTER_PRIV,LOAD_PRIV ON test.test_revoke_table FROM 'jack_revoke';";
        LogicalPlan revokeplan2 = nereidsParser.parseSingle(revokeSql);
        Assertions.assertTrue(revokeplan2 instanceof RevokeTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeTablePrivilegeCommand) revokeplan2).run(connectContext, null));
    }
}
