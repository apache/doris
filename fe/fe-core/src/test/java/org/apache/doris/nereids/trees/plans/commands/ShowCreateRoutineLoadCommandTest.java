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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.ShowCreateRoutineLoadCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ShowCreateRoutineLoadCommandTest {

    @Test
    public void test() throws Exception {
        ConnectContext ctx = new ConnectContext();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDB", "label0");
        ShowCreateRoutineLoadCommand command = new ShowCreateRoutineLoadCommand(labelNameInfo, true);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));
    }

    /**
     * A multi table job names no table, so what decides whether its statement may be shown is LOAD on the
     * database - the same rule the create path and PAUSE/RESUME apply to it.
     *
     * <p>The table-level grant is allowed here on purpose and must not be what lets the job through: a job
     * with no table has no table privilege to ask about, so were that the question, a job an account may
     * operate would silently vanish from {@code SHOW ALL CREATE ROUTINE LOAD} - or fail the statement
     * outright, as AuthorizedResource.Table refuses a question that names no table.
     */
    @Test
    public void testAMultiTableJobIsShownOnTheDatabaseLevelGrant() throws Exception {
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
            Mockito.when(routineLoadManager.getJob("db1", "job1", true, null))
                    .thenReturn(Lists.newArrayList(multiTableJob));
            Mockito.when(multiTableJob.isMultiTable()).thenReturn(true);
            Mockito.when(multiTableJob.getTableName()).thenReturn(null);
            Mockito.when(multiTableJob.getId()).thenReturn(7L);
            Mockito.when(multiTableJob.getShowCreateInfo()).thenReturn("CREATE ROUTINE LOAD job1 ...");
            // Held on the table, deliberately not on the database.
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.nullable(String.class),
                    Mockito.any(PrivPredicate.class))).thenReturn(true);

            ShowCreateRoutineLoadCommand command =
                    new ShowCreateRoutineLoadCommand(new LabelNameInfo("db1", "job1"), true);

            Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(false);
            ShowResultSet withoutTheGrant = Deencapsulation.invoke(command, "handleShowCreateRoutineLoad");
            Assertions.assertTrue(withoutTheGrant.getResultRows().isEmpty(),
                    "a multi table job was shown to an account holding no database level LOAD");

            Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(true);
            ShowResultSet withTheGrant = Deencapsulation.invoke(command, "handleShowCreateRoutineLoad");
            Assertions.assertEquals(1, withTheGrant.getResultRows().size(),
                    "a multi table job was hidden from an account holding database level LOAD");
        }
    }
}
