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
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class GrantTablePrivilegeCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        String createTableStr = "create table test.test_table(d1 date, k1 int, k2 bigint)"
                + "duplicate key(d1, k1) "
                + "PARTITION BY RANGE(d1)"
                + "(PARTITION p20210901 VALUES [('2021-09-01'), ('2021-09-02')))"
                + "distributed by hash(k1) buckets 2 "
                + "properties('replication_num' = '1');";
        createTable(createTableStr);
    }

    @Test
    public void testValidate() throws DdlException {
        List<AccessPrivilegeWithCols> privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALL));
        TablePattern tablePattern = new TablePattern("test", "test_table");
        GrantTablePrivilegeCommand command = new GrantTablePrivilegeCommand(
                privileges, tablePattern, Optional.empty(), Optional.of("test"));
        Assertions.assertDoesNotThrow(() -> command.validate());
    }

    @Test
    public void testGrantTablePrivilege() {
        String grantTablePrivilegeSql = "GRANT LOAD_PRIV ON internal.test.* TO ROLE 'role1';";
        String createRoleSql = "CREATE ROLE role1";
        String createUserSql = "CREATE USER 'jack'";

        NereidsParser nereidsParser = new NereidsParser();

        LogicalPlan logicalPlan = nereidsParser.parseSingle(createRoleSql);
        Assertions.assertTrue(logicalPlan instanceof CreateRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalPlan).run(connectContext, null));

        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(createUserSql);
        Assertions.assertTrue(logicalPlan1 instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) logicalPlan1).run(connectContext, null));

        LogicalPlan plan = nereidsParser.parseSingle(grantTablePrivilegeSql);
        Assertions.assertTrue(plan instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan).run(connectContext, null));

        grantTablePrivilegeSql = "GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON test.test_table  TO 'jack'";
        LogicalPlan plan1 = nereidsParser.parseSingle(grantTablePrivilegeSql);
        Assertions.assertTrue(plan1 instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan1).run(connectContext, null));

        grantTablePrivilegeSql = "GRANT SELECT_PRIV ON *.*.* TO 'jack'";
        LogicalPlan plan2 = nereidsParser.parseSingle(grantTablePrivilegeSql);
        Assertions.assertTrue(plan2 instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan2).run(connectContext, null));

        // grant database test all table
        grantTablePrivilegeSql = "GRANT SELECT_PRIV ON test TO 'jack'";
        LogicalPlan plan3 = nereidsParser.parseSingle(grantTablePrivilegeSql);
        Assertions.assertTrue(plan3 instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan3).run(connectContext, null));
    }

    @Test
    public void testFail() {
        String query = "GRANT LOAD_PRIV ON internal.test1.* TO ROLE 'my_role';";

        NereidsParser nereidsParser = new NereidsParser();

        // test database not exist
        LogicalPlan plan = nereidsParser.parseSingle(query);
        Assertions.assertTrue(plan instanceof GrantTablePrivilegeCommand);
        Assertions.assertThrows(DdlException.class, () -> ((GrantTablePrivilegeCommand) plan).run(connectContext, null));
    }
}
