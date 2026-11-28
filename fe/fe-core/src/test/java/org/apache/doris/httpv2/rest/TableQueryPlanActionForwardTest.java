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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import com.sun.net.httpserver.HttpServer;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import okhttp3.Credentials;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableQueryPlanActionForwardTest {
    private final Env env = Mockito.mock(Env.class);
    private final TableQueryPlanAction action = Mockito.spy(new TableQueryPlanAction());
    private final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    private final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    private MockedStatic<Env> mockedEnv;
    private boolean originalRunningUnitTest;

    @BeforeEach
    public void setUp() {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(request.getScheme()).thenReturn("http");
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        FeConstants.runningUnitTest = originalRunningUnitTest;
    }

    @ParameterizedTest
    @CsvSource(value = {"NULL, false", "false, false", "true, true"}, nullValues = "NULL")
    public void testLocalRequestDoesNotForward(String forward, boolean master) throws IOException {
        Mockito.when(env.isMaster()).thenReturn(master);
        Mockito.when(request.getParameter("forward")).thenReturn(forward);
        Mockito.doReturn(null).when(action).executeCheckPassword(request, response);
        setRequestBody("{}");

        ResponseEntity<?> result = (ResponseEntity<?>) action.query_plan("testDb", "testTbl", request, response);

        Assertions.assertEquals("POST body must contains [sql] root object",
                ((ResponseBody<?>) result.getBody()).getData());
        Mockito.verify(action, Mockito.never()).forwardToMaster(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"POST", "GET"})
    public void testForwardQueryPlan(String method) throws IOException {
        String sqlBody = "{\"sql\":\"select k1 as `列` from testDb.testTbl\"}";
        String path = "/api/testDb/testTbl/_query_plan";
        String rootAuth = Credentials.basic("root", "");
        HttpServer master = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        master.createContext(path, exchange -> {
            try (exchange) {
                Map<String, String> received = new HashMap<>();
                received.put("body", new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                received.put("authorization", exchange.getRequestHeaders().getFirst("Authorization"));
                received.put("query", exchange.getRequestURI().getQuery());
                received.put("method", exchange.getRequestMethod());
                byte[] result = JSONValue.toJSONString(received).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, result.length);
                exchange.getResponseBody().write(result);
            }
        });
        master.start();
        try {
            Mockito.doReturn(null).when(action).executeCheckPassword(request, response);
            Mockito.when(request.getMethod()).thenReturn(method);
            Mockito.when(request.getParameter("forward")).thenReturn("true");
            Mockito.when(request.getRequestURI()).thenReturn(path);
            Mockito.when(request.getQueryString()).thenReturn("forward=true");
            Mockito.when(request.getServerName()).thenReturn("127.0.0.1");
            Mockito.when(request.getServerPort()).thenReturn(master.getAddress().getPort());
            Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
            Mockito.when(request.getHeaderNames()).thenReturn(
                    Collections.enumeration(List.of("Authorization", "Content-Type")));
            Mockito.when(request.getHeader("Authorization")).thenReturn(rootAuth);
            Mockito.when(request.getHeader("Content-Type")).thenReturn("application/json; charset=utf-8");
            setRequestBody(sqlBody);

            Object result = action.query_plan("testDb", "testTbl", request, response);

            Assertions.assertEquals(Map.of("body", sqlBody, "authorization", rootAuth,
                    "query", "forward=true", "method", "POST"), result);
        } finally {
            master.stop(0);
        }
    }

    @Test
    public void testForwardRequiresAuthentication() throws IOException {
        Mockito.when(request.getParameter("forward")).thenReturn("true");

        Assertions.assertThrows(UnauthorizedException.class,
                () -> action.query_plan("testDb", "testTbl", request, response));
        Mockito.verify(request, Mockito.never()).getInputStream();
        Mockito.verify(action, Mockito.never()).forwardToMaster(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testRejectedAuditHintProducesHttpPlanEvent() throws Exception {
        boolean previousPolicy = Config.query_audit_always_on;
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setThreadLocalInfo();
        try {
            Config.query_audit_always_on = true;
            Mockito.when(env.getCatalogMgr()).thenReturn(Mockito.mock(CatalogMgr.class));
            Mockito.when(env.getPluginMgr()).thenReturn(Mockito.mock(PluginMgr.class));
            Mockito.when(env.getSqlCacheManager()).thenReturn(new NereidsSqlCacheManager());
            WorkloadRuntimeStatusMgr audit = Mockito.mock(WorkloadRuntimeStatusMgr.class);
            Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(audit);
            Method handleQuery = TableQueryPlanAction.class.getDeclaredMethod(
                    "handleQuery", ConnectContext.class, String.class, String.class, String.class, Map.class);
            handleQuery.setAccessible(true);
            String sql = "SELECT /*+ SET_VAR(enable_prepared_stmt_audit_log=false) */ item FROM sample_items";
            InvocationTargetException result = Assertions.assertThrows(InvocationTargetException.class,
                    () -> handleQuery.invoke(action, context, "testDb", "sample_items", sql, new HashMap<>()));
            Assertions.assertInstanceOf(DorisHttpException.class, result.getCause());
            Assertions.assertTrue(result.getCause().getMessage().contains("enable_prepared_stmt_audit_log"),
                    result.getCause().getMessage());

            ArgumentCaptor<AuditEvent> event = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(audit).submitFinishQueryToAudit(event.capture());
            Assertions.assertEquals("HttpPlan", event.getValue().commandType);
            Assertions.assertEquals("ERR", event.getValue().state);
            Assertions.assertEquals(sql, event.getValue().stmt);
        } finally {
            Config.query_audit_always_on = previousPolicy;
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private void setRequestBody(String body) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
        ServletInputStream input = Mockito.mock(ServletInputStream.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(input.read()).thenAnswer(invocation -> stream.read());
        Mockito.when(request.getInputStream()).thenReturn(input);
    }
}
