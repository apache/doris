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
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class ShowCreateTableCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    @Override
    public void createTable(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        ((CreateTableCommand) plan).run(connectContext, null);
    }

    @Test
    public void testCreateTableDefaultValueMath() throws Exception {
        // test
        String sql = "create table if not exists test.tbl\n" + "(k1 int, k2 double default E, k3 double default PI)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1'); ";
        createTable(sql);
        ShowCreateTableCommand showCreateTableCommand = new ShowCreateTableCommand(new TableNameInfo("test", "tbl"),
                false);
        ShowResultSet result = showCreateTableCommand.doRun(connectContext, null);
        String resultStr = result.getResultRows().stream().flatMap(List::stream).collect(Collectors.joining(" "));
        Assertions.assertTrue(resultStr.contains("`k2` double NULL DEFAULT E"));
        Assertions.assertTrue(resultStr.contains("`k3` double NULL DEFAULT PI"));
    }
}
