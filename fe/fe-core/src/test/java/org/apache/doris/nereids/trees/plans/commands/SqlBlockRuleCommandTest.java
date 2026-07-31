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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ShowResultSetMetaData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlBlockRuleCommandTest {
    private static final String CREATE_RULE =
            "create sql_block_rule %s properties(\"require_partition_filter\" = \"true\", "
                    + "\"global\" = \"true\", \"enable\" = \"true\")";
    private static final String CREATE_RULE_WITH_PARTITION_NUM =
            "create sql_block_rule %s properties(\"require_partition_filter\" = \"true\", "
                    + "\"partition_num\" = \"3\", \"global\" = \"true\", \"enable\" = \"true\")";
    private static final String CREATE_RULE_WITH_SQL =
            "create sql_block_rule %s properties(\"require_partition_filter\" = \"true\", "
                    + "\"sql\" = \"select \\\\*\", \"global\" = \"true\", \"enable\" = \"true\")";
    private static final String CREATE_RULE_WITH_SQL_HASH =
            "create sql_block_rule %s properties(\"require_partition_filter\" = \"true\", "
                    + "\"sqlHash\" = \"abc\", \"global\" = \"true\", \"enable\" = \"true\")";
    private static final String ALTER_RULE =
            "alter sql_block_rule %s properties(\"require_partition_filter\" = \"true\")";
    private static final String ALTER_RULE_INVALID_REQUIRE_PARTITION_FILTER =
            "alter sql_block_rule %s properties(\"require_partition_filter\" = \"not_a_bool\")";

    @Test
    public void testCreateRequirePartitionFilterRule() throws Exception {
        String ruleName = "test_require_partition_filter_create";
        LogicalPlan plan = new NereidsParser().parseSingle(String.format(CREATE_RULE, ruleName));
        Assertions.assertInstanceOf(CreateSqlBlockRuleCommand.class, plan);

        CreateSqlBlockRuleCommand command = (CreateSqlBlockRuleCommand) plan;
        command.setProperties(command.properties);
        Assertions.assertTrue(command.getRequirePartitionFilter());
        Assertions.assertTrue(command.getGlobal());
        Assertions.assertTrue(command.getEnable());
    }

    @Test
    public void testAlterRequirePartitionFilterRule() throws Exception {
        String ruleName = "test_require_partition_filter_alter";
        LogicalPlan alterPlan = new NereidsParser().parseSingle(String.format(ALTER_RULE, ruleName));
        Assertions.assertInstanceOf(AlterSqlBlockRuleCommand.class, alterPlan);

        AlterSqlBlockRuleCommand command = (AlterSqlBlockRuleCommand) alterPlan;
        command.setProperties(command.properties);
        Assertions.assertTrue(command.getRequirePartitionFilter());
    }

    @Test
    public void testRequirePartitionFilterConflictsWithSql() {
        String ruleName = "test_require_partition_filter_sql_conflict";
        LogicalPlan plan = new NereidsParser().parseSingle(String.format(CREATE_RULE_WITH_SQL, ruleName));
        Assertions.assertInstanceOf(CreateSqlBlockRuleCommand.class, plan);

        Assertions.assertThrows(AnalysisException.class,
                () -> ((CreateSqlBlockRuleCommand) plan).setProperties(((CreateSqlBlockRuleCommand) plan).properties));
    }

    @Test
    public void testRequirePartitionFilterConflictsWithSqlHash() {
        String ruleName = "test_require_partition_filter_sql_hash_conflict";
        LogicalPlan plan = new NereidsParser().parseSingle(String.format(CREATE_RULE_WITH_SQL_HASH, ruleName));
        Assertions.assertInstanceOf(CreateSqlBlockRuleCommand.class, plan);

        Assertions.assertThrows(AnalysisException.class,
                () -> ((CreateSqlBlockRuleCommand) plan).setProperties(((CreateSqlBlockRuleCommand) plan).properties));
    }

    @Test
    public void testRequirePartitionFilterCoexistsWithPartitionNum() throws Exception {
        String ruleName = "test_require_partition_filter_partition_num";
        LogicalPlan plan = new NereidsParser().parseSingle(String.format(CREATE_RULE_WITH_PARTITION_NUM, ruleName));
        Assertions.assertInstanceOf(CreateSqlBlockRuleCommand.class, plan);

        CreateSqlBlockRuleCommand command = (CreateSqlBlockRuleCommand) plan;
        command.setProperties(command.properties);
        Assertions.assertEquals(3L, command.getPartitionNum());
        Assertions.assertTrue(command.getRequirePartitionFilter());
    }

    @Test
    public void testAlterRequirePartitionFilterRejectInvalidBoolean() {
        String ruleName = "test_require_partition_filter_invalid_boolean";
        LogicalPlan alterPlan = new NereidsParser().parseSingle(
                String.format(ALTER_RULE_INVALID_REQUIRE_PARTITION_FILTER, ruleName));
        Assertions.assertInstanceOf(AlterSqlBlockRuleCommand.class, alterPlan);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ((AlterSqlBlockRuleCommand) alterPlan)
                        .setProperties(((AlterSqlBlockRuleCommand) alterPlan).properties));
        Assertions.assertTrue(exception.getMessage().contains("require_partition_filter should be a boolean"));
    }

    @Test
    public void testShowSqlBlockRuleRequirePartitionFilterColumnUseBooleanType() {
        ShowSqlBlockRuleCommand command = new ShowSqlBlockRuleCommand(null);
        ShowResultSetMetaData metaData = command.getMetaData();
        Assertions.assertEquals(9, metaData.getColumnCount());
        Assertions.assertEquals(PrimitiveType.BOOLEAN,
                metaData.getColumn(8).getType().getPrimitiveType());
    }
}
