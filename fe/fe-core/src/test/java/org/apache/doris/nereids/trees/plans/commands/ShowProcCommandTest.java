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
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class ShowProcCommandTest {
    @Test
    void usesOneProcResultForHeaderAndRows() throws Exception {
        String path = "/dbs/1/2/index_schema/3";
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(access.checkGlobalPriv(context, PrivPredicate.ADMIN_OR_NODE)).thenReturn(true);
        ProcService service = Mockito.mock(ProcService.class);
        ProcNodeInterface node = Mockito.mock(ProcNodeInterface.class);
        Mockito.when(service.open(path)).thenReturn(node);
        BaseProcResult proc = new BaseProcResult(ImmutableList.of("Field"),
                ImmutableList.of(ImmutableList.of("payload.key")));
        Mockito.when(node.fetchResult()).thenReturn(proc).thenThrow(new AnalysisException("second fetch failed"));
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ProcService> procStatic = Mockito.mockStatic(ProcService.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            procStatic.when(ProcService::getInstance).thenReturn(service);
            ShowResultSet result = new ShowProcCommand(path).doRun(context, null);
            Assertions.assertEquals(1, result.getMetaData().getColumnCount());
            Assertions.assertEquals("Field", result.getMetaData().getColumn(0).getName());
            Assertions.assertEquals(ImmutableList.of(ImmutableList.of("payload.key")), result.getResultRows());
            Mockito.verify(node).fetchResult();
        }
    }
}
