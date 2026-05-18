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

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TNetworkAddress;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.view.RedirectView;

import java.lang.reflect.Method;

public class LoadActionTest {

    @AfterEach
    public void tearDown() {
        Config.stream_load_redirect_bounded_drain_max_bytes = 1024L * 1024 * 1024;
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
        Config.stream_load_redirect_bounded_drain_max_idle_time_ms = 100;
        LoadAction loadAction = new LoadAction();
        HttpServletRequest request = mockStreamLoadRequest();
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = invokeCreateRedirectResponse(loadAction, request, response,
                new TNetworkAddress("be-host", 8040), true, "db1", "tbl1", "label1");

        Assertions.assertNull(result);
        Mockito.verify(response).setContentType("text/html;charset=utf-8");
        Mockito.verify(response).setStatus(HttpStatus.TEMPORARY_REDIRECT.value());
        Mockito.verify(response).setHeader("Location", "http://be-host:8040/api/db1/tbl1/_stream_load?foo=bar");
        Mockito.verify(response).flushBuffer();
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

    private Object invokeCreateRedirectResponse(LoadAction loadAction, HttpServletRequest request,
            HttpServletResponse response, TNetworkAddress redirectAddr, boolean isStreamLoad, String dbName,
            String tableName, String label) throws Exception {
        Method method = LoadAction.class.getDeclaredMethod("createRedirectResponse",
                HttpServletRequest.class, HttpServletResponse.class, TNetworkAddress.class,
                boolean.class, String.class, String.class, String.class);
        method.setAccessible(true);
        return method.invoke(loadAction, request, response, redirectAddr, isStreamLoad, dbName, tableName, label);
    }

    private HttpServletRequest mockStreamLoadRequest() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getRequestURI()).thenReturn("/api/db1/tbl1/_stream_load");
        Mockito.when(request.getQueryString()).thenReturn("foo=bar");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);
        Mockito.when(request.getInputStream()).thenReturn(new IdleServletInputStream());
        return request;
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
