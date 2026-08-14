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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InsertIntoTableCommandRetryTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("insert_retry_test");
        connectContext.setDatabase("insert_retry_test");
        createTable("create table insert_retry_test.target (k1 int, k2 int) "
                + "distributed by hash(k1) buckets 1 properties('replication_num'='1')");
    }

    @Test
    public void testInternalReplanResetsMaterializedViewPlanningState() throws Exception {
        String sql = "insert into insert_retry_test.target select 1, 2 where false";
        InsertIntoTableCommand parsedCommand = (InsertIntoTableCommand) new NereidsParser().parseSingle(sql);
        Database database = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("insert_retry_test");
        TableIf targetTable = database.getTableOrMetaException("target");
        TableIf replacedTable = Mockito.mock(TableIf.class);
        Mockito.when(replacedTable.getId()).thenReturn(targetTable.getId() + 1);
        AtomicInteger targetResolutions = new AtomicInteger();
        InsertIntoTableCommand command = new InsertIntoTableCommand(
                parsedCommand, PlanType.INSERT_INTO_TABLE_COMMAND) {
            @Override
            protected TableIf getTargetTableIf(ConnectContext ctx, List<String> qualifiedTargetTableName) {
                int resolution = targetResolutions.incrementAndGet();
                if (resolution == 2) {
                    return replacedTable;
                }
                if (resolution == 3) {
                    Assertions.assertTrue(ctx.getStatementContext().getMvCanRewritePartitionsMap().isEmpty());
                }
                return targetTable;
            }

            @Override
            protected boolean needAuthCheck(TableIf targetTableIf) {
                return false;
            }
        };

        connectContext.setQueryId(new TUniqueId(1, 2));
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        StatementContext statementContext = connectContext.getStatementContext();
        statementContext.getMvCanRewritePartitionsMap().put(
                new BaseTableInfo(new TableNameInfo("internal", "db", "mv")),
                Collections.singleton(Mockito.mock(Partition.class)));

        command.initPlan(connectContext, executor, true);

        Assertions.assertEquals(4, targetResolutions.get());
    }
}
