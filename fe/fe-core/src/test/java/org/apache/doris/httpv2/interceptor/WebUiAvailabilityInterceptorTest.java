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

package org.apache.doris.httpv2.interceptor;

import org.apache.doris.common.Config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

class WebUiAvailabilityInterceptorTest {
    private final WebUiAvailabilityInterceptor interceptor = new WebUiAvailabilityInterceptor();

    @AfterEach
    void restoreConfig() {
        Config.enable_web_ui = true;
    }

    @Test
    void disabledUiBlocksRepresentativeUiRequests() {
        Config.enable_web_ui = false;
        for (String path : new String[] {
                "/", "/assets/index.js", "/rest/v1/login", "/rest/v1/sql-sessions/id/statements"
        }) {
            AtomicInteger status = new AtomicInteger();
            Assertions.assertFalse(interceptor.preHandle(request(path), response(status), new Object()), path);
            Assertions.assertEquals(404, status.get(), path);
        }
    }

    @Test
    void disabledUiLeavesSharedHttpApisAvailable() {
        Config.enable_web_ui = false;
        Assertions.assertTrue(interceptor.preHandle(
                request("/rest/v1/system"), response(new AtomicInteger()), new Object()));
    }

    private HttpServletRequest request(String path) {
        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(), new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> {
                    if ("getRequestURI".equals(method.getName())) {
                        return path;
                    }
                    if ("getContextPath".equals(method.getName())) {
                        return "";
                    }
                    return null;
                });
    }

    private HttpServletResponse response(AtomicInteger status) {
        return (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(), new Class<?>[] {HttpServletResponse.class},
                (proxy, method, args) -> {
                    if ("setStatus".equals(method.getName())) {
                        status.set((Integer) args[0]);
                    }
                    return null;
                });
    }
}
