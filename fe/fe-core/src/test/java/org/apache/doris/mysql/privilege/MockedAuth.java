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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.VariableMgr;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MockedAuth {

    public static void mockedAccess(AccessControllerManager accessManager) {
        Mockito.when(accessManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.any(PrivPredicate.class)))
                .thenReturn(true);

        Mockito.when(accessManager.checkDbPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(PrivPredicate.class)))
                .thenReturn(true);

        Mockito.when(accessManager.checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class)))
                .thenReturn(true);
    }

    // NOTE: API change - now returns MockedStatic<ConnectContext> that callers must close
    // (e.g., store in a field and call close() in @After / @AfterEach, or use try-with-resources).
    // Callers also need to create ctx with Mockito.mock(ConnectContext.class) instead of @Mocked.
    public static MockedStatic<ConnectContext> mockedConnectContext(ConnectContext ctx, String user, String ip) {
        MockedStatic<ConnectContext> mockedStatic = Mockito.mockStatic(ConnectContext.class);
        mockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(ctx.getQualifiedUser()).thenReturn(user);
        Mockito.when(ctx.getRemoteIP()).thenReturn(ip);
        Mockito.when(ctx.getState()).thenReturn(new QueryState());

        UserIdentity userIdentity = new UserIdentity(user, ip);
        userIdentity.setIsAnalyzed();
        Mockito.when(ctx.getCurrentUserIdentity()).thenReturn(userIdentity);

        Mockito.when(ctx.getSessionVariable()).thenReturn(VariableMgr.newSessionVariable());

        return mockedStatic;
    }
}
