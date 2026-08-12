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

package org.apache.doris.httpv2.ui;

import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.HttpAuthManager;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.ui.websql.WebSqlSessionManager;
import org.apache.doris.mysql.privilege.PrivPredicate;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/rest/v1/ui")
public class UiSessionController {
    private final WebSqlSessionManager webSqlSessionManager;

    public UiSessionController(WebSqlSessionManager webSqlSessionManager) {
        this.webSqlSessionManager = webSqlSessionManager;
    }

    @GetMapping("/me")
    public UiApiResponse<UiMe> me(HttpServletRequest request) {
        SessionValue session = UiRequestContext.session(request);
        boolean adminOrNode = Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(session.currentUser, PrivPredicate.ADMIN_OR_NODE);
        boolean admin = Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(session.currentUser, PrivPredicate.ADMIN);
        List<UiCapability> capabilities = UiCapabilityResolver.resolve(adminOrNode, admin);
        UiMe me = new UiMe(session.currentUser.getQualifiedUser(), capabilities, session.csrfToken);
        return new UiApiResponse<>(me, UiRequestContext.requestId(request));
    }

    @PostMapping("/logout")
    public UiApiResponse<UiLogoutResult> logout(HttpServletRequest request, HttpServletResponse response) {
        SessionValue session = UiRequestContext.session(request);
        webSqlSessionManager.closeSessionsForOwner(session.currentUser.getQualifiedUser());
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (BaseController.PALO_SESSION_ID.equals(cookie.getName())) {
                    HttpAuthManager.getInstance().removeSession(cookie.getValue());
                }
            }
        }

        Cookie expiredCookie = new Cookie(BaseController.PALO_SESSION_ID, "");
        expiredCookie.setHttpOnly(true);
        expiredCookie.setMaxAge(0);
        expiredCookie.setPath("/");
        expiredCookie.setAttribute("SameSite", "Lax");
        response.addCookie(expiredCookie);
        return new UiApiResponse<>(new UiLogoutResult(true), UiRequestContext.requestId(request));
    }

    @RequestMapping("/**")
    public ResponseEntity<UiErrorResponse> notFound(HttpServletRequest request) {
        String requestId = UiRequestContext.requestId(request);
        if (isKnownWebSqlPath(request.getRequestURI())) {
            UiErrorResponse error = new UiErrorResponse(
                    "UI_METHOD_NOT_ALLOWED", "The request method is not allowed for this UI API.",
                    requestId, null);
            return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(error);
        }
        UiErrorResponse error = new UiErrorResponse(
                "UI_NOT_FOUND", "The requested UI API does not exist.", requestId, null);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }

    private boolean isKnownWebSqlPath(String requestUri) {
        return requestUri != null && (requestUri.equals("/rest/v1/ui/sql-sessions")
                || requestUri.matches("/rest/v1/ui/sql-sessions/[^/]+")
                || requestUri.matches("/rest/v1/ui/sql-sessions/[^/]+/(statements|cancel|reset)"));
    }
}
