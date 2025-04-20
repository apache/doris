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

public class GrantTablePrivilegeCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        String createTableStr = "create table test.test(d1 date, k1 int, k2 bigint)"
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
        TablePattern tablePattern = new TablePattern("test", "test");
        GrantTablePrivilegeCommand command = new GrantTablePrivilegeCommand(
                privileges, tablePattern, Optional.empty(), Optional.of("test"));
        Assertions.assertDoesNotThrow(() -> command.validate());

        String query = "GRANT LOAD_PRIV ON internal.test.* TO ROLE 'my_role';";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(query);
        Assertions.assertTrue(plan instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan).validate());
        Env.getCurrentEnv().getAuth().grantTablePrivilegeCommand((GrantTablePrivilegeCommand) plan);
    }

    @Test
    public void testFail() {
        String query = "GRANT LOAD_PRIV ON internal.test1.* TO ROLE 'my_role';";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan plan = nereidsParser.parseSingle(query);
        Assertions.assertTrue(plan instanceof GrantTablePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantTablePrivilegeCommand) plan).validate());
        Assertions.assertThrows(DdlException.class, () -> Env.getCurrentEnv().getAuth().grantTablePrivilegeCommand((GrantTablePrivilegeCommand) plan));
    }
}
