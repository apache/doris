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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.WarmupSelectCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class S3ExpressImportScopeTest {

    private static final String S3_SELECT_SQL = "SELECT * FROM S3("
            + "\"uri\" = \"s3://analytics--usw2-az1--x-s3/data/file.csv\", "
            + "\"s3.provider\" = \"AWS\", "
            + "\"format\" = \"csv\")";

    @Test
    void enableOnlyForTopLevelOneShotSelectAndInsert() {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setDatabase("test");
        context.setThreadLocalInfo();
        try {
            NereidsParser parser = new NereidsParser();

            LogicalPlan select = parser.parseSingle(S3_SELECT_SQL);
            Assertions.assertEquals(PlanType.LOGICAL_UNBOUND_RESULT_SINK, select.getType());
            Assertions.assertTrue(StmtExecutor.shouldEnableS3ExpressImportRead(select, false));
            Assertions.assertTrue(StmtExecutor.shouldEnableS3ExpressImportRead(
                    parser.parseSingle("EXPLAIN " + S3_SELECT_SQL), false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(select, true));

            LogicalPlan insert = parser.parseSingle("INSERT INTO target_table " + S3_SELECT_SQL);
            Assertions.assertEquals(InsertIntoTableCommand.class, insert.getClass());
            Assertions.assertTrue(StmtExecutor.shouldEnableS3ExpressImportRead(insert, false));
            Assertions.assertTrue(StmtExecutor.shouldEnableS3ExpressImportRead(
                    parser.parseSingle("EXPLAIN INSERT INTO target_table " + S3_SELECT_SQL), false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(insert, true));

            LogicalPlan overwrite = parser.parseSingle("INSERT OVERWRITE TABLE target_table " + S3_SELECT_SQL);
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                    overwrite, false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                    parser.parseSingle("EXPLAIN INSERT OVERWRITE TABLE target_table " + S3_SELECT_SQL), false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                    parser.parseSingle("CREATE TABLE target_table AS " + S3_SELECT_SQL), false));
            LogicalPlan outfile = parser.parseSingle(S3_SELECT_SQL
                    + " INTO OUTFILE \"file:///tmp/s3-express-select\" FORMAT AS CSV");
            Assertions.assertEquals(PlanType.LOGICAL_FILE_SINK, outfile.getType());
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(outfile, false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                    plan(PlanType.INSERT_INTO_TVF_COMMAND), false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                    plan(PlanType.COPY_INTO_COMMAND), false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                    Mockito.mock(WarmupSelectCommand.class), false));
            Assertions.assertFalse(StmtExecutor.shouldEnableS3ExpressImportRead(
                    Mockito.mock(InsertIntoDictionaryCommand.class), false));
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
