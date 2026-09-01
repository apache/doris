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
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Arrays;
import java.util.List;

/** Prevents serving the browser UI and its dedicated APIs when the FE Web UI is disabled. */
public class WebUiAvailabilityInterceptor implements HandlerInterceptor {
    private static final List<String> UI_PAGE_ROOTS = Arrays.asList(
            "/login", "/home", "/playground", "/system", "/log",
            "/query-profiles", "/sessions", "/configuration");
    private static final List<String> UI_API_ROOTS = Arrays.asList(
            "/rest/v1/login", "/rest/v1/logout", "/rest/v1/ui", "/rest/v1/sql-sessions");

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (Config.enable_web_ui || !isWebUiRequest(pathWithinApplication(request))) {
            return true;
        }
        response.setStatus(HttpStatus.NOT_FOUND.value());
        return false;
    }

    private String pathWithinApplication(HttpServletRequest request) {
        String path = request.getRequestURI();
        String contextPath = request.getContextPath();
        if (contextPath != null && !contextPath.isEmpty() && path.startsWith(contextPath)) {
            return path.substring(contextPath.length());
        }
        return path;
    }

    private boolean isWebUiRequest(String path) {
        if ("/".equals(path) || "/index.html".equals(path) || "/notFound".equals(path)
                || isPathAtOrBelow(path, "/assets") || isPathAtOrBelow(path, "/static")) {
            return true;
        }
        for (String root : UI_PAGE_ROOTS) {
            if (isPathAtOrBelow(path, root)) {
                return true;
            }
        }
        for (String root : UI_API_ROOTS) {
            if (isPathAtOrBelow(path, root)) {
                return true;
            }
        }
        return false;
    }

    private boolean isPathAtOrBelow(String path, String root) {
        return root.equals(path) || path.startsWith(root + "/");
    }
}
