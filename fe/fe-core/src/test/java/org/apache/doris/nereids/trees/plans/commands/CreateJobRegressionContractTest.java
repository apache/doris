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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateJobRegressionContractTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("job_contract_test");
        createTable("CREATE TABLE src(k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES(\"replication_num\"=\"1\")");
    }

    @Test
    public void unsupportedUpdateKeepsLegacyErrorPrefix() throws Exception {
        String updateSql = "UPDATE job_contract_test.src SET k=2 WHERE k=1";
        String sql = "CREATE JOB update_contract ON SCHEDULE AT CURRENT_TIMESTAMP DO " + updateSql;
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.getState().reset();
        try (StatementContext statement = new StatementContext(connectContext, new OriginStatement(sql, 0))) {
            connectContext.setStatementContext(statement);
            CreateJobCommand command = Assertions.assertInstanceOf(CreateJobCommand.class,
                    new NereidsParser().parseSingle(sql));
            StmtExecutor executor = new StmtExecutor(connectContext, new LogicalPlanAdapter(command, statement));
            connectContext.setExecutor(executor);

            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> command.run(connectContext, executor));
            Assertions.assertTrue(error.getMessage().contains("Not support this sql :"), error.getMessage());
            Assertions.assertTrue(error.getMessage().contains(UpdateCommand.class.getName()), error.getMessage());
            Assertions.assertFalse(error.getMessage().contains(updateSql), error.getMessage());
            Assertions.assertInstanceOf(AnalysisException.class, error.getCause());
            Assertions.assertEquals(error.getMessage(), error.getCause().getMessage());
        } finally {
            connectContext.setExecutor(null);
            connectContext.setStatementContext(null);
            ConnectContext.remove();
        }
    }
}
