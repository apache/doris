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
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ShowRoutineLoadCommandTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_db");
    }

    @Test
    public void testValidate() {
        LabelNameInfo labelNameInfo = new LabelNameInfo("test_db", "test_label");
        ShowRoutineLoadCommand command = new ShowRoutineLoadCommand(labelNameInfo, null, false);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals(24, command.getMetaData().getColumnCount());
        Assertions.assertEquals("FirstErrorMsg", command.getMetaData().getColumn(23).getName());
    }

    /**
     * SHOW ALL ROUTINE LOAD lists history, so it meets jobs whose table has since been dropped as a matter of
     * course. getTableName() throws for such a job and leaves no name to ask about, and a table privilege
     * cannot be asked about without one: AuthorizedResource.Table refuses it with "table is required", which
     * fails the whole statement rather than the one job. What decides the job is LOAD on the database, which
     * is what the absent name used to reduce to.
     */
    @Test
    public void testAJobWhoseTableIsGoneIsDecidedOnTheDatabase() throws Exception {
        Env env = Mockito.mock(Env.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        RoutineLoadJob droppedTableJob = Mockito.mock(RoutineLoadJob.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            ctxStatic.when(ConnectContext::get).thenReturn(ctx);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(ctx.getDatabase()).thenReturn("db1");
            Mockito.when(routineLoadManager.getJob("db1", null, true, null))
                    .thenReturn(Lists.newArrayList(droppedTableJob));
            Mockito.when(droppedTableJob.getId()).thenReturn(7L);
            Mockito.when(droppedTableJob.isMultiTable()).thenReturn(false);
            Mockito.when(droppedTableJob.getTableName())
                    .thenThrow(new MetaNotFoundException("failed to get table. table id: 42"));
            Mockito.when(droppedTableJob.getShowInfo())
                    .thenReturn(Lists.newArrayList("7", "job1", "2026-01-01 00:00:00"));
            // As AuthorizedResource.Table does: there is no asking about a table privilege without a table.
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.isNull(), Mockito.any(PrivPredicate.class)))
                    .thenThrow(new NullPointerException("table is required"));

            ShowRoutineLoadCommand command = new ShowRoutineLoadCommand(null, null, true);
            command.validate(ctx);

            Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(true);
            ShowResultSet withTheGrant = Deencapsulation.invoke(command, "handleShowRoutineLoad");
            Assertions.assertEquals(1, withTheGrant.getResultRows().size(),
                    "a job whose table is gone was hidden from an account holding database level LOAD");

            Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(false);
            ShowResultSet withoutTheGrant = Deencapsulation.invoke(command, "handleShowRoutineLoad");
            Assertions.assertTrue(withoutTheGrant.getResultRows().isEmpty(),
                    "a job whose table is gone was shown to an account holding no database level LOAD");
        }
    }

    /**
     * The name belongs to the job it was read from. Held across the loop, a job that names no table was
     * decided against whichever table the job before it named - a grant on one table deciding a job that has
     * nothing to do with it.
     */
    @Test
    public void testTheTableNameIsNotCarriedFromOneJobToTheNext() throws Exception {
        Env env = Mockito.mock(Env.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        RoutineLoadJob grantedJob = Mockito.mock(RoutineLoadJob.class);
        RoutineLoadJob droppedTableJob = Mockito.mock(RoutineLoadJob.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            ctxStatic.when(ConnectContext::get).thenReturn(ctx);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(ctx.getDatabase()).thenReturn("db1");
            Mockito.when(routineLoadManager.getJob("db1", null, true, null))
                    .thenReturn(Lists.newArrayList(grantedJob, droppedTableJob));

            Mockito.when(grantedJob.getId()).thenReturn(1L);
            Mockito.when(grantedJob.isMultiTable()).thenReturn(false);
            Mockito.when(grantedJob.getTableName()).thenReturn("t1");
            Mockito.when(grantedJob.getShowInfo())
                    .thenReturn(Lists.newArrayList("1", "grantedJob", "2026-01-01 00:00:00"));

            Mockito.when(droppedTableJob.getId()).thenReturn(2L);
            Mockito.when(droppedTableJob.isMultiTable()).thenReturn(false);
            Mockito.when(droppedTableJob.getTableName())
                    .thenThrow(new MetaNotFoundException("failed to get table. table id: 42"));
            Mockito.when(droppedTableJob.getShowInfo())
                    .thenReturn(Lists.newArrayList("2", "droppedTableJob", "2026-01-02 00:00:00"));

            // LOAD on t1 and nothing else: not on the database, and not on any other table.
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.eq("t1"), Mockito.eq(PrivPredicate.LOAD))).thenReturn(true);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.isNull(), Mockito.any(PrivPredicate.class)))
                    .thenThrow(new NullPointerException("table is required"));
            Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(false);

            ShowRoutineLoadCommand command = new ShowRoutineLoadCommand(null, null, true);
            command.validate(ctx);
            ShowResultSet result = Deencapsulation.invoke(command, "handleShowRoutineLoad");

            Assertions.assertEquals(1, result.getResultRows().size(),
                    "a job naming no table was decided by the grant on the table the job before it named");
            Assertions.assertEquals("grantedJob", result.getResultRows().get(0).get(1));
        }
    }
}
