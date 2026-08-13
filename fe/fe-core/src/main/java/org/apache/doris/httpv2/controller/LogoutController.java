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

package org.apache.doris.httpv2.controller;

import org.apache.doris.httpv2.HttpAuthManager;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.ui.websql.WebSqlSessionManager;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;

@RestController
@RequestMapping("/rest/v1")
public class LogoutController extends BaseController {
    private final WebSqlSessionManager webSqlSessionManager;

    public LogoutController(WebSqlSessionManager webSqlSessionManager) {
        this.webSqlSessionManager = webSqlSessionManager;
    }

    @RequestMapping(path = "/logout", method = RequestMethod.POST)
    public Object login(HttpServletRequest request, HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName() != null && cookie.getName().equals(PALO_SESSION_ID)) {
                    String sessionId = cookie.getValue();
                    SessionValue session = HttpAuthManager.getInstance()
                            .getSessionValue(Collections.singletonList(sessionId));
                    if (session != null) {
                        webSqlSessionManager.closeSessionsForOwner(session.currentUser.getQualifiedUser());
                    }
                    HttpAuthManager.getInstance().removeSession(sessionId);
                }
            }
        }
        Cookie expiredCookie = new Cookie(PALO_SESSION_ID, "");
        expiredCookie.setHttpOnly(true);
        expiredCookie.setMaxAge(0);
        expiredCookie.setPath("/");
        expiredCookie.setAttribute("SameSite", "Lax");
        response.addCookie(expiredCookie);
        return ResponseEntityBuilder.ok();
    }
}
