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

package org.apache.doris.arrowflight;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapGroupCommitInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.proto.Data;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

class FlightGroupCommitStateTest extends TestWithFeService {
    private static final String FIRST_INSERT = "insert into flight_group_commit_state.target values (1)";
    private static final String DISABLE_GROUP_COMMIT = "set group_commit = 'off_mode'";
    private static final String NEXT_INSERT = "insert into flight_group_commit_state.target "
            + "select k from flight_group_commit_state.target";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("flight_group_commit_state");
        createTable("create table flight_group_commit_state.target (k int) "
                + "duplicate key(k) distributed by hash(k) buckets 1 "
                + "properties('replication_num'='1', 'light_schema_change'='true')");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nextCommandDoesNotInheritPreviousGroupCommitDecision(boolean failFirstInsert) throws Exception {
        checkDecisions(false, failFirstInsert);
    }

    @Test
    void statementsInOneRequestHaveIndependentGroupCommitDecisions() throws Exception {
        checkDecisions(true, false);
    }

    private void checkDecisions(boolean batch, boolean failFirstInsert) throws Exception {
        ConnectContext session = ConnectContext.forFlight("group-commit-state-peer");
        session.setCurrentUserIdentity(UserIdentity.ROOT);
        session.setEnv(Env.getCurrentEnv());
        session.getSessionVariable().groupCommit = "sync_mode";
        List<Boolean> decisions = new ArrayList<>();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(session);
                MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                        (executor, construction) -> {
                            LogicalPlanAdapter statement = (LogicalPlanAdapter) construction.arguments().get(1);
                            LogicalPlan plan = statement.getLogicalPlan();
                            Mockito.when(executor.getProfile()).thenReturn(new Profile());
                            Mockito.when(executor.getParsedStmt()).thenReturn(statement);
                            Mockito.doAnswer(call -> {
                                // Use real parsing, SET execution and group-commit analysis. Stop at the
                                // execution boundary: this test needs no backend write or transaction.
                                if (plan instanceof SetOptionsCommand) {
                                    ((SetOptionsCommand) plan).run(session, executor);
                                } else {
                                    OlapGroupCommitInsertExecutor.analyzeGroupCommit(session, plan);
                                }
                                decisions.add(session.isGroupCommit());
                                if (failFirstInsert && decisions.size() == 1) {
                                    throw new UserException("insert failed after group-commit analysis");
                                }
                                return null;
                            }).when(executor).execute();
                        })) {
            if (batch) {
                run(session, FIRST_INSERT + ";" + DISABLE_GROUP_COMMIT + ";" + NEXT_INSERT, false);
            } else {
                run(session, FIRST_INSERT, failFirstInsert);
                run(session, DISABLE_GROUP_COMMIT, false);
                run(session, NEXT_INSERT, false);
            }
            Assertions.assertEquals(List.of(true, false, false), decisions);
            Assertions.assertEquals(3, executors.constructed().size());
            Assertions.assertEquals("off_mode", session.getSessionVariable().groupCommit);
        }
    }

    private void run(ConnectContext session, String sql, boolean expectedFailure) throws Exception {
        session.executeCommand(0, () -> {
            try (FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(session) {
                @Override
                protected void auditAfterExec(String originalSql, StatementBase statement,
                        Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
                    // The query pipeline is intentionally replaced at its backend execution boundary.
                }
            }) {
                processor.handleQuery(sql);
                Assertions.assertEquals(expectedFailure, session.getState().getStateType() == MysqlStateType.ERR,
                        session.getState().getErrorMessage());
            }
            return null;
        });
    }
}
