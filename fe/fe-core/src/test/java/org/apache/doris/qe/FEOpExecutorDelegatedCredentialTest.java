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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.OptionalLong;

public class FEOpExecutorDelegatedCredentialTest {
    @Test
    public void testBuildStmtForwardParamsCarriesDelegatedCredential() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext context = new ConnectContext();
            context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
            context.setRemoteIP("127.0.0.1");
            context.setSessionContext(SessionContext.of(new DelegatedCredential(
                    DelegatedCredential.Type.ID_TOKEN, "forwarded-id-token", OptionalLong.of(12345L))));

            String delegatedCredentialSessionId = context.getSessionContext().getSessionId();
            TMasterOpRequest request = new TestFEOpExecutor(context).build();

            Assertions.assertTrue(request.isSetDelegatedCredentialSessionId());
            Assertions.assertEquals(delegatedCredentialSessionId, request.getDelegatedCredentialSessionId());
            Assertions.assertTrue(request.isSetDelegatedCredentialType());
            Assertions.assertTrue(request.isSetDelegatedCredentialToken());
            Assertions.assertTrue(request.isSetDelegatedCredentialExpiresAtMillis());
            Assertions.assertEquals(DelegatedCredential.Type.ID_TOKEN.name(), request.getDelegatedCredentialType());
            Assertions.assertEquals("forwarded-id-token", request.getDelegatedCredentialToken());
            Assertions.assertEquals(12345L, request.getDelegatedCredentialExpiresAtMillis());
        }
    }

    private static class TestFEOpExecutor extends FEOpExecutor {
        private TestFEOpExecutor(ConnectContext context) {
            super(new TNetworkAddress("127.0.0.1", 9010), new OriginStatement("select 1", 0), context, true);
        }

        private TMasterOpRequest build() throws AnalysisException {
            return buildStmtForwardParams();
        }
    }
}
