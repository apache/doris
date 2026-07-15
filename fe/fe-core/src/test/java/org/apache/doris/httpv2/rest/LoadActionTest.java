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
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.view.RedirectView;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

public class LoadActionTest {
    private final boolean originalEnableDebugPoints = Config.enable_debug_points;
    private final String originalCloudUniqueId = Config.cloud_unique_id;
    private final boolean originalEnableGroupCommitStreamLoadBeForward =
            Config.enable_group_commit_streamload_be_forward;


    @AfterEach
    public void tearDown() {
        Config.stream_load_redirect_bounded_drain_max_bytes = 1024L * 1024 * 1024;
        Config.enable_debug_points = originalEnableDebugPoints;
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.enable_group_commit_streamload_be_forward = originalEnableGroupCommitStreamLoadBeForward;
        DebugPointUtil.clearDebugPoints();
        Thread.interrupted();
        org.apache.doris.qe.ConnectContext.remove();
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 1000;
    }

    @Test
    public void testCreateRedirectResponseReturnsRedirectViewWhenBoundedDrainDisabled() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 0;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 100;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest();
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), true, "db1", "tbl1", "label1");

        Assertions.assertTrue(result instanceof RedirectView);
        RedirectView redirectView = (RedirectView) result;
        Assertions.assertEquals("http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar", redirectView.getUrl());
        Mockito.verifyNoInteractions(response);
    }

    @Test
    public void testCreateRedirectResponseWrites307WhenBoundedDrainEnabled() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(-1, "chunked", true);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setContentType("text/html;charset=utf-8");
        Mockito.verify(response).setStatus(HttpStatus.TEMPORARY_REDIRECT.value());
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        Mockito.verify(response).flushBuffer();
        Mockito.verify(request).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseDrainsFixedLengthRequestWithinLimit() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(4, null, true);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        Mockito.verify(request).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseSkipsDrainWithoutRequestBody() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(-1, null, false);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setContentType("text/html;charset=utf-8");
        Mockito.verify(response).setStatus(HttpStatus.TEMPORARY_REDIRECT.value());
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        Mockito.verify(response).flushBuffer();
        Mockito.verify(request, Mockito.never()).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseSkipsDrainWhenContentLengthIsZero() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(0, null, false);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setContentType("text/html;charset=utf-8");
        Mockito.verify(response).setStatus(HttpStatus.TEMPORARY_REDIRECT.value());
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        Mockito.verify(response).flushBuffer();
        Mockito.verify(request, Mockito.never()).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseSkipsDrainWhenContentLengthExceedsLimit() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(16, null, false);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setContentType("text/html;charset=utf-8");
        Mockito.verify(response).setStatus(HttpStatus.TEMPORARY_REDIRECT.value());
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        Mockito.verify(response).flushBuffer();
        Mockito.verify(request, Mockito.never()).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseWithRedirectViewReturnsRedirectViewWhenDrainDisabled() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(-1, null, false);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        RedirectView redirectView = new RedirectView("http://be-host:8040/redirect");

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                redirectView, true, "db1", "tbl1", "label1");

        Assertions.assertSame(redirectView, result);
        Mockito.verifyNoInteractions(response);
        Mockito.verify(request, Mockito.never()).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseWithRedirectViewSkipsDrainWithoutRequestBody() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(-1, null, false);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        RedirectView redirectView = new RedirectView("http://be-host:8040/redirect");

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                redirectView, true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/redirect");
        Mockito.verify(request, Mockito.never()).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseWithRedirectViewDrainsChunkedRequest() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 0;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest(-1, "chunked", true);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        RedirectView redirectView = new RedirectView("http://be-host:8040/redirect");

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                redirectView, true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/redirect");
        Mockito.verify(request).getInputStream();
    }

    @Test
    public void testCreateRedirectResponseKeepsNonStreamLoadBehavior() throws Exception {
        Config.stream_load_redirect_bounded_drain_max_bytes = 8;
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 100;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest();
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), false, "db1", "tbl1", "label1");

        Assertions.assertTrue(result instanceof RedirectView);
        Mockito.verifyNoInteractions(response);
    }

    @Test
    public void testGetAllHeadersMasksSensitiveHeaders() throws Exception {
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeaderNames()).thenReturn(Collections.enumeration(Arrays.asList(
                "Authorization", "Cookie", "Set-Cookie", "token", "label")));
        Mockito.when(request.getHeader("label")).thenReturn("load_label");

        Method method = LoadAction.class.getDeclaredMethod("getAllHeaders", HttpServletRequest.class);
        method.setAccessible(true);
        String headers = (String) method.invoke(loadAction, request);

        Assertions.assertTrue(headers.contains("Authorization:***MASKED***"));
        Assertions.assertTrue(headers.contains("Cookie:***MASKED***"));
        Assertions.assertTrue(headers.contains("Set-Cookie:***MASKED***"));
        Assertions.assertTrue(headers.contains("token:***MASKED***"));
        Assertions.assertTrue(headers.contains("label:load_label"));
    }

    @Test
    public void testExecuteWithoutPasswordRedirectsToBackend() throws Exception {
        Config.enable_debug_points = true;
        DebugPointUtil.addDebugPointWithValue("LoadAction.selectRedirectBackend.backendId", 1L);
        TestLoadAction loadAction = new TestLoadAction();
        HttpServletRequest request = mockStreamLoadRequest(-1, null, false);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Backend backend = mockBackend("be-host", 8040, null);
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getBackend(1L)).thenReturn(backend);
        Mockito.when(request.getHeader("expect")).thenReturn("100-continue");

        org.apache.doris.qe.ConnectContext connectContext = new org.apache.doris.qe.ConnectContext();
        connectContext.setThreadLocalInfo();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            Object result = invokeExecuteWithoutPassword(loadAction, request, response, "db1", "tbl1", true, false);

            Assertions.assertNull(result);
            Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        }
    }

    @Test
    public void testExecuteWithClusterTokenRedirectsToBackend() throws Exception {
        Config.enable_debug_points = true;
        DebugPointUtil.addDebugPointWithValue("LoadAction.selectRedirectBackend.backendId", 1L);
        TestLoadAction loadAction = new TestLoadAction();
        HttpServletRequest request = mockStreamLoadRequest(-1, null, false);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Backend backend = mockBackend("be-host", 8040, null);
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(systemInfoService.getBackend(1L)).thenReturn(backend);
        // Provide the default catalog name required by ConnectContext.setEnv().
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn("internal");
        Mockito.when(request.getHeader("expect")).thenReturn("100-continue");
        Mockito.when(request.getHeader("label")).thenReturn("label1");
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Object result = invokeExecuteWithClusterToken(loadAction, request, response, "db1", "tbl1", true);

            Assertions.assertNull(result);
            Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        }
    }

    @Test
    public void testStreamLoadWithSqlRedirectsWhenExpectHeaderExists() {
        Config.enable_debug_points = true;
        DebugPointUtil.addDebugPointWithValue("LoadAction.selectRedirectBackend.backendId", 1L);
        TestLoadAction loadAction = new TestLoadAction();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Backend backend = mockBackend("be-host", 8040, null);
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getBackend(1L)).thenReturn(backend);
        Mockito.when(request.getHeader("sql")).thenReturn("insert into db1.tbl1 values(1)");
        Mockito.when(request.getHeader("expect")).thenReturn("100-continue");
        Mockito.when(request.getRequestURI()).thenReturn("/api/db1/tbl1/_stream_load");
        Mockito.when(request.getQueryString()).thenReturn("foo=bar");
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);
        Mockito.when(request.getContentLengthLong()).thenReturn(-1L);
        Mockito.when(request.getHeader("transfer-encoding")).thenReturn(null);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            Object result = loadAction.streamLoadWithSql(request, response);

            Assertions.assertNull(result);
            Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        }
    }

    @Test
    public void testRedirectToStreamLoadForwardBuildsForwardUrl() throws Exception {
        TestLoadAction loadAction = new TestLoadAction();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getRequestURI()).thenReturn("/api/db1/tbl1/_stream_load");
        Mockito.when(request.getQueryString()).thenReturn("k=v");
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);

        RedirectView redirectView = invokeRedirectToStreamLoadForward(loadAction, request,
                new TNetworkAddress("lb-host", 18040), "be-host:8040");

        Assertions.assertEquals("http://lb-host:18040/api/db1/tbl1/_stream_load_forward?k=v&forward_to=be-host:8040",
                redirectView.getUrl());
    }

    @Test
    public void testSplitHostAndPortParsesIpv4() throws Exception {
        LoadAction loadAction = new LoadAction();
        org.apache.doris.common.Pair<String, Integer> result = invokeSplitHostAndPort(loadAction, "10.0.0.1:8040");
        Assertions.assertEquals("10.0.0.1", result.first);
        Assertions.assertEquals(8040, result.second.intValue());
    }

    @Test
    public void testSplitHostAndPortParsesIpv6() throws Exception {
        LoadAction loadAction = new LoadAction();
        org.apache.doris.common.Pair<String, Integer> result =
                invokeSplitHostAndPort(loadAction, "[2001:db8::1]:8040");
        Assertions.assertEquals("2001:db8::1", result.first);
        Assertions.assertEquals(8040, result.second.intValue());
    }

    @Test
    public void testSelectEndpointByRedirectPolicyParsesIpv4HostAndEndpoint() throws Exception {
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader(LoadAction.HEADER_REDIRECT_POLICY))
                .thenReturn(LoadAction.REDIRECT_POLICY_PRIVATE);
        Mockito.when(request.getHeader("host")).thenReturn("10.0.0.1:8030");

        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getPrivateEndpoint()).thenReturn("192.168.1.1:8040");
        Mockito.when(backend.getPublicEndpoint()).thenReturn(null);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getHttpPort()).thenReturn(8040);

        TNetworkAddress addr = invokeSelectEndpointByRedirectPolicy(loadAction, request, backend);
        Assertions.assertEquals("192.168.1.1", addr.getHostname());
        Assertions.assertEquals(8040, addr.getPort());
    }

    @Test
    public void testSelectEndpointByRedirectPolicyParsesIpv6HostAndEndpoint() throws Exception {
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader(LoadAction.HEADER_REDIRECT_POLICY))
                .thenReturn(LoadAction.REDIRECT_POLICY_PRIVATE);
        Mockito.when(request.getHeader("host")).thenReturn("[2001:db8::1]:8030");

        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getPrivateEndpoint()).thenReturn("[fd00::1]:8040");
        Mockito.when(backend.getPublicEndpoint()).thenReturn(null);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getHttpPort()).thenReturn(8040);

        TNetworkAddress addr = invokeSelectEndpointByRedirectPolicy(loadAction, request, backend);
        Assertions.assertEquals("fd00::1", addr.getHostname());
        Assertions.assertEquals(8040, addr.getPort());
    }

    private Object invokeCreateRedirectResponse(LoadAction loadAction, HttpServletRequest request,
            HttpServletResponse response, TNetworkAddress redirectAddr, boolean isStreamLoad, String dbName,
            String tableName, String label) throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("createRedirectResponse",
                HttpServletRequest.class, HttpServletResponse.class, TNetworkAddress.class,
                boolean.class, String.class, String.class, String.class);
        method.setAccessible(true);
        return method.invoke(loadAction, request, response, redirectAddr, isStreamLoad, dbName, tableName, label);
    }

    private Object invokeCreateRedirectResponse(LoadAction loadAction, HttpServletRequest request,
            HttpServletResponse response, RedirectView redirectView, boolean isStreamLoad, String dbName,
            String tableName, String label) throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("createRedirectResponse",
                HttpServletRequest.class, HttpServletResponse.class, RedirectView.class,
                boolean.class, String.class, String.class, String.class);
        method.setAccessible(true);
        return method.invoke(loadAction, request, response, redirectView, isStreamLoad, dbName, tableName, label);
    }

    private Object invokeExecuteWithoutPassword(LoadAction loadAction, HttpServletRequest request,
            HttpServletResponse response, String db, String table, boolean isStreamLoad, boolean groupCommit)
            throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("executeWithoutPassword",
                HttpServletRequest.class, HttpServletResponse.class, String.class, String.class,
                boolean.class, boolean.class);
        method.setAccessible(true);
        return method.invoke(loadAction, request, response, db, table, isStreamLoad, groupCommit);
    }

    private Object invokeExecuteWithClusterToken(LoadAction loadAction, HttpServletRequest request,
            HttpServletResponse response, String db, String table, boolean isStreamLoad) throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("executeWithClusterToken",
                HttpServletRequest.class, HttpServletResponse.class, String.class, String.class, boolean.class);
        method.setAccessible(true);
        return method.invoke(loadAction, request, response, db, table, isStreamLoad);
    }

    private RedirectView invokeRedirectToStreamLoadForward(LoadAction loadAction, HttpServletRequest request,
            TNetworkAddress addr, String forwardTarget) throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("redirectToStreamLoadForward",
                HttpServletRequest.class, TNetworkAddress.class, String.class);
        method.setAccessible(true);
        return (RedirectView) method.invoke(loadAction, request, addr, forwardTarget);
    }

    private TNetworkAddress invokeSelectEndpointByRedirectPolicy(LoadAction loadAction, HttpServletRequest request,
            Backend backend) throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("selectEndpointByRedirectPolicy",
                HttpServletRequest.class, Backend.class);
        method.setAccessible(true);
        return (TNetworkAddress) method.invoke(loadAction, request, backend);
    }

    @SuppressWarnings("unchecked")
    private org.apache.doris.common.Pair<String, Integer> invokeSplitHostAndPort(LoadAction loadAction, String hostPort)
            throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("splitHostAndPort", String.class);
        method.setAccessible(true);
        return (org.apache.doris.common.Pair<String, Integer>) method.invoke(loadAction, hostPort);
    }

    private HttpServletRequest mockStreamLoadRequest() throws Exception {
        return mockStreamLoadRequest(-1, null, true);
    }

    // Provide explicit request body metadata so the drain decision can be verified in isolation.
    private HttpServletRequest mockStreamLoadRequest(long contentLength, String transferEncoding,
            boolean stubInputStream) throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getRequestURI()).thenReturn("/api/db1/tbl1/_stream_load");
        Mockito.when(request.getQueryString()).thenReturn("foo=bar");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);
        Mockito.when(request.getContentLengthLong()).thenReturn(contentLength);
        Mockito.when(request.getHeader("Transfer-Encoding")).thenReturn(transferEncoding);
        Mockito.when(request.getHeader("transfer-encoding")).thenReturn(transferEncoding);
        if (stubInputStream) {
            Mockito.when(request.getInputStream()).thenReturn(new IdleServletInputStream());
        }
        return request;
    }

    // Create a backend mock with only the redirect-related fields populated for lightweight controller tests.
    private Backend mockBackend(String host, int httpPort, String privateEndpoint) {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn(host);
        Mockito.when(backend.getHttpPort()).thenReturn(httpPort);
        Mockito.when(backend.getPrivateEndpoint()).thenReturn(privateEndpoint);
        Mockito.when(backend.getPublicEndpoint()).thenReturn(null);
        return backend;
    }

    private static class TestLoadAction extends LoadAction {
        @Override
        public org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo executeCheckPassword(
                HttpServletRequest request,
                HttpServletResponse response) {
            return new org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo();
        }

        @Override
        protected void checkTblAuth(org.apache.doris.analysis.UserIdentity currentUser, String db, String tbl,
                org.apache.doris.mysql.privilege.PrivPredicate predicate) {
        }

        @Override
        protected boolean checkClusterToken(String token) {
            return true;
        }
    }

    private static class IdleServletInputStream extends ServletInputStream {
        @Override
        public int read() {
            return -1;
        }

        @Override
        public int available() {
            return 0;
        }

        @Override
        public boolean isFinished() {
            return false;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
        }
    }
}
