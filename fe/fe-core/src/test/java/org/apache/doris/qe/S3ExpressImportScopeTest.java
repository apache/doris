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

import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
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
        InsertIntoTableCommand insert = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(insert.getType()).thenReturn(PlanType.INSERT_INTO_TABLE_COMMAND);

        Assertions.assertTrue(StmtExecutor.shouldEnableS3ExpressImportRead(insert, false));
        Assertions.assertTrue(StmtExecutor.shouldEnableS3ExpressImportRead(
                new ExplainCommand(ExplainLevel.NORMAL, insert, false), false));

        Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(insert, true));
        InsertOverwriteTableCommand overwrite = new InsertOverwriteTableCommand(
                plan(PlanType.LOGICAL_RESULT_SINK), Optional.empty(), Optional.empty(), Optional.empty());
        Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                overwrite, false));
        Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                new ExplainCommand(ExplainLevel.NORMAL, overwrite, false), false));
        Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                plan(PlanType.INSERT_INTO_TVF_COMMAND), false));
        Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                plan(PlanType.LOGICAL_RESULT_SINK), false));
    }

    private static LogicalPlan plan(PlanType type) {
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        Mockito.when(plan.getType()).thenReturn(type);
        return plan;
    }
}
