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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AdminSetPartitionVersionCommandTest extends TestWithFeService {
    @Test
    public void testNormal() throws Exception {
        // test logical plan
        String sql = "ADMIN SET TABLE __internal_schema.audit_log PARTITION VERSION PROPERTIES(\"partition_id\" = \"10075\", \"visible_version\" = \"100\");";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetPartitionVersionCommand);

        // test object
        TableNameInfo tableInfo = new TableNameInfo("internal", "test", "t1");
        Map<String, String> properties = new HashMap<>();
        properties.put("partition_id", "10086");
        properties.put("visible_version", "100");
        AdminSetPartitionVersionCommand command = new AdminSetPartitionVersionCommand(tableInfo, properties);
        Assertions.assertThrows(DdlException.class, () -> command.run(connectContext, null));
    }

    @Test
    public void testEmptyConfig() {
        String sql = "ADMIN SET TABLE test.t1 PARTITION VERSION PROPERTIES(\"a\" = \"b\")";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetPartitionVersionCommand);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ((AdminSetPartitionVersionCommand) plan).run(connectContext, null));
        Assertions.assertTrue(exception.getMessage().contains("Should specify 'partition_id' property."));
    }
}
