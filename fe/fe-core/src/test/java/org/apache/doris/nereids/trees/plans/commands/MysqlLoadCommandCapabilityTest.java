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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.loadv2.MysqlLoadManager;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.nereids.load.NereidsDataDescription;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlDataDescription;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlLoadCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;

public class MysqlLoadCommandCapabilityTest {
    @Test
    public void testLocalFilesCapabilityOnlyRequiredForClientUploads() throws Exception {
        for (boolean clientLocal : new boolean[] {false, true}) {
            for (boolean supportsLocalFiles : new boolean[] {false, true}) {
                ConnectContext context = new ConnectContext();
                Env mockEnv = Mockito.mock(Env.class);
                LoadManager loadManager = Mockito.mock(LoadManager.class);
                MysqlLoadManager mysqlLoadManager = Mockito.mock(MysqlLoadManager.class);
                Mockito.when(mockEnv.getInternalCatalog()).thenReturn(Mockito.mock(InternalCatalog.class));
                context.setEnv(mockEnv);
                context.setCapability(new MysqlCapability(supportsLocalFiles
                        ? MysqlCapability.Flag.CLIENT_LOCAL_FILES.getFlagBit() : 0));
                Mockito.when(mockEnv.getLoadManager()).thenReturn(loadManager);
                Mockito.when(loadManager.getMysqlLoadManager()).thenReturn(mysqlLoadManager);
                MysqlDataDescription description = Mockito.mock(MysqlDataDescription.class);
                Mockito.when(description.isClientLocal()).thenReturn(clientLocal);
                Mockito.when(mysqlLoadManager.executeMySqlLoadJob(Mockito.eq(context),
                        Mockito.eq(description), Mockito.anyString())).thenReturn(new LoadJobRowResult());
                MysqlLoadCommand command = new MysqlLoadCommand(description, new HashMap<>(), "test");
                Deencapsulation.invoke(command, "handleMysqlLoadCommand", context);
                boolean rejected = clientLocal && !supportsLocalFiles;
                Assertions.assertEquals(rejected ? QueryState.MysqlStateType.ERR : QueryState.MysqlStateType.OK,
                        context.getState().getStateType());
                Mockito.verify(mysqlLoadManager, Mockito.times(rejected ? 0 : 1))
                        .executeMySqlLoadJob(Mockito.eq(context), Mockito.eq(description), Mockito.anyString());

                context.getState().reset();
                NereidsDataDescription nereidsDescription = Mockito.mock(NereidsDataDescription.class);
                Mockito.when(nereidsDescription.isClientLocal()).thenReturn(clientLocal);
                Mockito.when(mysqlLoadManager.executeMySqlLoadJobFromCommand(Mockito.eq(context),
                        Mockito.eq(nereidsDescription), Mockito.anyString())).thenReturn(new LoadJobRowResult());
                LoadCommand loadCommand = Mockito.mock(LoadCommand.class, Mockito.CALLS_REAL_METHODS);
                Deencapsulation.setField(loadCommand, "etlJobType", EtlJobType.LOCAL_FILE);
                Mockito.doReturn(Collections.singletonList(nereidsDescription)).when(loadCommand).getDataDescriptions();
                loadCommand.handleLoadCommand(context, null);
                Assertions.assertEquals(rejected ? QueryState.MysqlStateType.ERR : QueryState.MysqlStateType.OK,
                        context.getState().getStateType());
                Mockito.verify(mysqlLoadManager, Mockito.times(rejected ? 0 : 1))
                        .executeMySqlLoadJobFromCommand(Mockito.eq(context), Mockito.eq(nereidsDescription),
                                Mockito.anyString());
            }
        }
    }
}
