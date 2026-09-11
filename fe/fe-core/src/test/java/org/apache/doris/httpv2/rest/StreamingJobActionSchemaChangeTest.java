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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TokenManager;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

public class StreamingJobActionSchemaChangeTest extends TestWithFeService {
    private static final String DB_NAME = "streaming_schema_change_test";
    private static final String TABLE_NAME = "token_auth_tbl";
    private final StreamingJobAction action = new StreamingJobAction(new TableSchemaAction());

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB_NAME);
        createTable("CREATE TABLE " + DB_NAME + "." + TABLE_NAME + " (k1 INT) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
        Mockito.when(job.getJobId()).thenReturn(123L);
        Mockito.when(job.getCreateUser()).thenReturn(connectContext.getCurrentUserIdentity());
        Mockito.when(job.getCurrentDbName()).thenReturn(DB_NAME);
        Env.getCurrentEnv().getJobManager().createJobInternal(job, true);
    }

    @Test
    public void testExecuteSchemaChangeWithToken() throws Exception {
        HttpServletRequest request = tokenRequest();
        Map<String, String> body = Collections.singletonMap(
                "stmt", "ALTER TABLE " + DB_NAME + "." + TABLE_NAME + " ADD COLUMN added_col INT");

        ResponseEntity<?> result = (ResponseEntity<?>) action.executeSchemaChange(body, request);

        ResponseBody<?> responseBody = (ResponseBody<?>) result.getBody();
        Assertions.assertEquals(RestApiStatusCode.OK.code, responseBody.getCode());
    }

    @Test
    public void testExecuteSchemaChangeRejectsNonMaster() throws Exception {
        HttpServletRequest request = tokenRequest();
        Map<String, String> body = Collections.singletonMap("stmt", "ALTER TABLE forwarded_tbl ADD COLUMN k2 INT");
        Env follower = Mockito.mock(Env.class);
        TokenManager tokenManager = Mockito.mock(TokenManager.class);
        Mockito.when(follower.isMaster()).thenReturn(false);
        Mockito.when(tokenManager.checkAuthToken(Mockito.anyString())).thenReturn(true);
        Mockito.when(follower.getTokenManager()).thenReturn(tokenManager);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class)) {
            env.when(Env::getCurrentEnv).thenReturn(follower);
            ResponseEntity<?> result = (ResponseEntity<?>) action.executeSchemaChange(body, request);

            ResponseBody<?> responseBody = (ResponseBody<?>) result.getBody();
            Assertions.assertEquals(RestApiStatusCode.COMMON_ERROR.code, responseBody.getCode());
            Assertions.assertEquals("Schema change must be executed on the master FE", responseBody.getData());
            Assertions.assertTrue(executors.constructed().isEmpty());
        }
    }

    @Test
    public void testGetTableSchemaWithToken() throws Exception {
        HttpServletRequest request = tokenRequest();
        ResponseEntity<?> result = (ResponseEntity<?>) action.getTableSchema(DB_NAME, TABLE_NAME, request);

        ResponseBody<?> responseBody = (ResponseBody<?>) result.getBody();
        Assertions.assertEquals(RestApiStatusCode.OK.code, responseBody.getCode());
        Assertions.assertEquals(200, ((Map<?, ?>) responseBody.getData()).get("status"));
    }

    private HttpServletRequest tokenRequest() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        Mockito.when(request.getHeader("jobId")).thenReturn("123");
        Mockito.when(request.getHeader("token"))
                .thenReturn(Env.getCurrentEnv().getTokenManager().acquireToken());
        String invalidBasic = Base64.getEncoder().encodeToString(
                "admin:invalid-password".getBytes(StandardCharsets.UTF_8));
        Mockito.when(request.getHeader("Authorization")).thenReturn("Basic " + invalidBasic);
        return request;
    }
}
