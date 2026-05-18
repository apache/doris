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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.proto.Data;
import org.apache.doris.thrift.TMasterOpRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

class ConnectProcessorDelegatedCredentialTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void testHandleQueryRejectsExpiredDelegatedCredential() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setSessionContext(SessionContext.of(new DelegatedCredential(DelegatedCredential.Type.ID_TOKEN,
                "id-token", OptionalLong.of(1L))));
        RecordingConnectProcessor processor = new RecordingConnectProcessor(context);

        processor.handle("select 1");

        Assertions.assertFalse(processor.executed);
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals(ErrorCode.ERR_ACCESS_DENIED_ERROR, context.getState().getErrorCode());
        Assertions.assertTrue(context.getState().getErrorMessage().contains("expired"));
    }

    @Test
    void testHandleQueryAllowsUnexpiredDelegatedCredential() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setSessionContext(SessionContext.of(new DelegatedCredential(DelegatedCredential.Type.ID_TOKEN,
                "id-token", OptionalLong.of(System.currentTimeMillis() + 60_000L))));
        RecordingConnectProcessor processor = new RecordingConnectProcessor(context);

        processor.handle("select 1");

        Assertions.assertTrue(processor.executed);
        Assertions.assertEquals(QueryState.MysqlStateType.OK, context.getState().getStateType());
    }

    @Test
    void testRestoreForwardedDelegatedCredential() {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setDelegatedCredentialSessionId("forwarded-session-id");
        request.setDelegatedCredentialType(DelegatedCredential.Type.ACCESS_TOKEN.name());
        request.setDelegatedCredentialToken("forwarded-access-token");
        request.setDelegatedCredentialExpiresAtMillis(12345L);
        ConnectContext context = new ConnectContext();

        ConnectProcessor.restoreForwardedSessionContext(context, request);

        Assertions.assertEquals("forwarded-session-id", context.getSessionContext().getSessionId());
        DelegatedCredential credential = context.getSessionContext().getDelegatedCredential().get();
        Assertions.assertEquals(DelegatedCredential.Type.ACCESS_TOKEN, credential.getType());
        Assertions.assertEquals("forwarded-access-token", credential.getToken());
        Assertions.assertEquals(12345L, credential.getExpiresAtMillis().getAsLong());
    }

    private static class RecordingConnectProcessor extends ConnectProcessor {
        private boolean executed;

        private RecordingConnectProcessor(ConnectContext context) {
            super(context);
            this.connectType = ConnectType.MYSQL;
        }

        private void handle(String originStmt) throws Exception {
            handleQuery(originStmt);
        }

        @Override
        public void executeQuery(String originStmt) {
            executed = true;
        }

        @Override
        protected void auditAfterExec(String origStmt, StatementBase parsedStmt,
                Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        }
    }
}
