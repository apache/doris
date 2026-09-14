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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ShowRoutineLoadTaskCommandTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_db");
    }

    @Test
    public void testValidate() {
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("test")),
                new StringLiteral("test"));
        ShowRoutineLoadTaskCommand command = new ShowRoutineLoadTaskCommand("test_db", where);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));

        Expression where2 = new EqualTo(new UnboundSlot(Lists.newArrayList("JobName")),
                new StringLiteral("1111"));
        ShowRoutineLoadTaskCommand command2 = new ShowRoutineLoadTaskCommand("test_db", where2);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("TaskId")),
                new IntegerLiteral(1111));
        ShowRoutineLoadTaskCommand command3 = new ShowRoutineLoadTaskCommand("test_db", where3);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext));

        //test whereClause is null
        ShowRoutineLoadTaskCommand command4 = new ShowRoutineLoadTaskCommand("test_db", null);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext));
    }

    /**
     * A refusal on a multi-table job names the user and the database, in that order.
     *
     * <p>{@code ERR_DBACCESS_DENIED_ERROR} reads "Access denied for user '%s' to database '%s'" - two
     * placeholders. It used to be given four arguments, the privilege and the remote IP among them, which
     * shifted every value one place along: the message named the privilege as the user and the user as the
     * database. The two sibling refusals on this statement family are pinned the same way; this was the
     * third.
     */
    @Test
    public void testARefusedMultiTableJobNamesTheUserAndTheDatabase() throws Exception {
        Env env = Mockito.mock(Env.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        RoutineLoadJob multiTableJob = Mockito.mock(RoutineLoadJob.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            ctxStatic.when(ConnectContext::get).thenReturn(ctx);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(ctx.getQualifiedUser()).thenReturn("analyst");
            // ErrorReport records the refusal on the connection's state before throwing it.
            Mockito.when(ctx.getState()).thenReturn(new QueryState());
            Mockito.when(routineLoadManager.getJob("test_db", "job1")).thenReturn(multiTableJob);
            Mockito.when(multiTableJob.isMultiTable()).thenReturn(true);
            Mockito.when(multiTableJob.getTableName()).thenReturn(null);
            Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(false);

            ShowRoutineLoadTaskCommand command = new ShowRoutineLoadTaskCommand("test_db",
                    new EqualTo(new UnboundSlot(Lists.newArrayList("JobName")), new StringLiteral("job1")));
            command.validate(connectContext);

            AnalysisException refused = Assertions.assertThrows(AnalysisException.class,
                    () -> Deencapsulation.invoke(command, "handleShowRoutineLoadTask"));

            Assertions.assertTrue(refused.getMessage().contains("user 'analyst'"),
                    "the refusal does not name the account it refused: " + refused.getMessage());
            Assertions.assertTrue(refused.getMessage().contains("database 'test_db'"),
                    "the refusal does not name the database it refused on, so the arguments are out of"
                            + " step with the two placeholders again: " + refused.getMessage());
        }
    }
}
