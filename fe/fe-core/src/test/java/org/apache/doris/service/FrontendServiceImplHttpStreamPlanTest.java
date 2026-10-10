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

package org.apache.doris.service;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStreamLoadPutRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FrontendServiceImplHttpStreamPlanTest {

    private static final String LOAD_SQL = "insert into t select s.k, d.v from http_stream(\"format\" = \"csv\") s"
            + " join ice.db.big d on s.k = d.k";

    /**
     * The backend runs the plan of an http_stream load as it is, never through a coordinator's exec(), and only a plan
     * of TVF scans passes. One refused after it was planned - its SELECT also reads a batch-mode table - leaves no
     * split generation running for that table: the request is all the statement is on this frontend.
     */
    @Test
    public void testAnHttpStreamLoadRefusedAfterPlanningStopsWhatItsPlanningStarted() throws Exception {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement(LOAD_SQL, 0));
        SplitAssignment undispatched = Mockito.mock(SplitAssignment.class);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setLoadSql(LOAD_SQL);
        Method initHttpStreamPlan = FrontendServiceImpl.class.getDeclaredMethod("initHttpStreamPlan",
                TStreamLoadPutRequest.class, ConnectContext.class);
        initHttpStreamPlan.setAccessible(true);

        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                (executor, construction) -> {
                    // What the constructor does with the context.
                    ctx.setStatementContext(statementContext);
                    Mockito.when(executor.generateHttpStreamPlan(Mockito.any())).thenAnswer(invocation -> {
                        // The scan of the table the SELECT joins started generating its splits while planned.
                        statementContext.addSplitAssignmentStartedWhilePlanning(undispatched);
                        throw new AnalysisException("plan is invalid: the load reads a table besides the TVF");
                    });
                })) {
            InvocationTargetException e = Assertions.assertThrows(InvocationTargetException.class,
                    () -> initHttpStreamPlan.invoke(new FrontendServiceImpl(null), request, ctx));
            Assertions.assertTrue(e.getCause() instanceof UserException, String.valueOf(e.getCause()));
            Assertions.assertEquals(1, executors.constructed().size());
        }

        Mockito.verify(undispatched).stopIfNotDispatched();
    }
}
