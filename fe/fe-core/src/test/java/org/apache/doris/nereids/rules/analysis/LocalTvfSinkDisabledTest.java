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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class LocalTvfSinkDisabledTest {
    @Test
    void disabledLocalRejectsInsertAndExplainBeforeProperties() {
        for (boolean authorized : new boolean[] {false, true}) {
            for (String prefix : new String[] {"", "EXPLAIN "}) {
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> analyzeInsert(prefix + "INSERT INTO local('format'='csv') SELECT 1", authorized));
                Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            }
        }
    }

    @Test
    void disabledLocalKeepsRemoteSinkValidation() {
        for (String name : new String[] {"s3", "hdfs"}) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> analyzeInsert("INSERT INTO " + name + "('format'='csv') SELECT 1", false));
            Assertions.assertTrue(exception.getMessage().contains("requires 'file_path'"));
        }
    }

    private Plan analyzeInsert(String sql, boolean authorized) {
        ConnectContext context = MemoTestUtils.createConnectContext();
        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.ADMIN)).thenReturn(authorized);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            LogicalPlan parsed = new NereidsParser().parseSingle(sql);
            if (parsed instanceof ExplainCommand) {
                parsed = ((ExplainCommand) parsed).getLogicalPlan();
            }
            InsertIntoTVFCommand command = (InsertIntoTVFCommand) parsed;
            return PlanChecker.from(context).analyze(command.getExplainPlan(context)).getPlan();
        }
    }
}
