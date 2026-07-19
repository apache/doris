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

package org.apache.doris.qe;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

class S3ExpressImportScopeTest {

    @Test
    void enableOnlyForTopLevelOneShotInsertIntoTable() {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            StatementContext statementContext = new StatementContext();
            LogicalPlan insert = new NereidsParser().parseSingle(
                    "INSERT INTO test_db.target_table SELECT * FROM S3("
                            + "\"uri\" = \"s3://analytics--usw2-az1--x-s3/data.parquet\", "
                            + "\"s3.provider\" = \"AWS\", \"s3.region\" = \"us-west-2\", "
                            + "\"format\" = \"parquet\")");
            Assertions.assertEquals(InsertIntoTableCommand.class, insert.getClass());

            StmtExecutor.configureS3ExpressImportRead(statementContext, insert, false);
            Assertions.assertTrue(statementContext.isS3ExpressImportRead());

            StmtExecutor.configureS3ExpressImportRead(statementContext, insert, true);
            Assertions.assertFalse(statementContext.isS3ExpressImportRead());
            InsertOverwriteTableCommand overwrite = new InsertOverwriteTableCommand(
                    plan(PlanType.LOGICAL_RESULT_SINK), Optional.empty(), Optional.empty(), Optional.empty());
            StmtExecutor.configureS3ExpressImportRead(statementContext, overwrite, false);
            Assertions.assertFalse(statementContext.isS3ExpressImportRead());
            StmtExecutor.configureS3ExpressImportRead(
                    statementContext, plan(PlanType.INSERT_INTO_TVF_COMMAND), false);
            Assertions.assertFalse(statementContext.isS3ExpressImportRead());
            StmtExecutor.configureS3ExpressImportRead(
                    statementContext, plan(PlanType.LOGICAL_RESULT_SINK), false);
            Assertions.assertFalse(statementContext.isS3ExpressImportRead());
        } finally {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private static LogicalPlan plan(PlanType type) {
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        Mockito.when(plan.getType()).thenReturn(type);
        return plan;
    }
}
