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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DropMaterializedViewCommandTest {

    @Test
    public void testEmptyMVName() {
        try {
            new DropMaterializedViewCommand(new TableNameInfo(), false, null);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("mvName"));
        }
    }

    @Test
    public void testNoPermission() {
        ConnectContext ctx = new ConnectContext();
        TableNameInfo tableName = new TableNameInfo("internal", "db1", "t1");

        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.ALTER)).thenReturn(false);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            DropMaterializedViewCommand command = new DropMaterializedViewCommand(tableName, false, "test");
            try {
                command.validate(ctx);
                Assertions.fail();
            } catch (UserException e) {
                Assertions.assertTrue(e.getMessage().contains("Access denied;"));
            }
        }

    }
}
