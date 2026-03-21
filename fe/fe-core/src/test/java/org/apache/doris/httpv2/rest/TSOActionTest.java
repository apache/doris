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
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TSOActionTest {
    private Env env;
    private TestableTSOAction action;

    @Before
    public void setUp() {
        new EnvMockUp();
        env = Mockito.mock(Env.class);
        EnvMockUp.CURRENT_ENV.set(env);
        action = new TestableTSOAction();
    }

    @Test
    public void testGetTSOEnvNullReturnsBadRequest() {
        EnvMockUp.CURRENT_ENV.set(null);
        ResponseEntity<?> resp = (ResponseEntity<?>) action.getTSO(mockRequest(), mockResponse());
        ResponseBody<?> body = (ResponseBody<?>) resp.getBody();
        Assert.assertNotNull(body);
        Assert.assertEquals(RestApiStatusCode.BAD_REQUEST.code, body.getCode());
    }

    @Test
    public void testGetTSOEnvNotReadyReturnsBadRequest() {
        Mockito.when(env.isReady()).thenReturn(false);
        ResponseEntity<?> resp = (ResponseEntity<?>) action.getTSO(mockRequest(), mockResponse());
        ResponseBody<?> body = (ResponseBody<?>) resp.getBody();
        Assert.assertNotNull(body);
        Assert.assertEquals(RestApiStatusCode.BAD_REQUEST.code, body.getCode());
    }

    @Test
    public void testGetTSONotMasterReturnsBadRequest() {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(false);
        ResponseEntity<?> resp = (ResponseEntity<?>) action.getTSO(mockRequest(), mockResponse());
        ResponseBody<?> body = (ResponseBody<?>) resp.getBody();
        Assert.assertNotNull(body);
        Assert.assertEquals(RestApiStatusCode.BAD_REQUEST.code, body.getCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetTSOSuccessPayloadConsistency() {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getWindowEndTSO()).thenReturn(12345L);
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        long currentTso = TSOTimestamp.composeTimestamp(1000L, 12L);
        Mockito.when(tsoService.getCurrentTSO()).thenReturn(currentTso);

        ResponseEntity<?> resp = (ResponseEntity<?>) action.getTSO(mockRequest(), mockResponse());
        ResponseBody<?> body = (ResponseBody<?>) resp.getBody();
        Assert.assertNotNull(body);
        Assert.assertEquals(RestApiStatusCode.OK.code, body.getCode());

        Map<String, Object> data = (Map<String, Object>) body.getData();
        Assert.assertEquals(12345L, ((Number) data.get("window_end_physical_time")).longValue());
        Assert.assertEquals(currentTso, ((Number) data.get("current_tso")).longValue());
        Assert.assertEquals(TSOTimestamp.extractPhysicalTime(currentTso),
                ((Number) data.get("current_tso_physical_time")).longValue());
        Assert.assertEquals(TSOTimestamp.extractLogicalCounter(currentTso),
                ((Number) data.get("current_tso_logical_counter")).longValue());
    }

    private static HttpServletRequest mockRequest() {
        return Mockito.mock(HttpServletRequest.class);
    }

    private static HttpServletResponse mockResponse() {
        return Mockito.mock(HttpServletResponse.class);
    }

    private static final class TestableTSOAction extends TSOAction {
        @Override
        public ActionAuthorizationInfo executeCheckPassword(HttpServletRequest request, HttpServletResponse response) {
            return null;
        }
    }

    private static final class EnvMockUp extends MockUp<Env> {
        private static final AtomicReference<Env> CURRENT_ENV = new AtomicReference<>();

        @Mock
        public static Env getCurrentEnv() {
            return CURRENT_ENV.get();
        }
    }
}

