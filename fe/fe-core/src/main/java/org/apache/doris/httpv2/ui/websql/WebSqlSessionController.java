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

package org.apache.doris.httpv2.ui.websql;

import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.ui.UiRequestContext;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1/ui/sql-sessions")
public class WebSqlSessionController {
    private final WebSqlSessionManager manager;

    public WebSqlSessionController(WebSqlSessionManager manager) {
        this.manager = manager;
    }

    @PostMapping
    public WebSqlSessionInfo create(HttpServletRequest request) {
        SessionValue login = UiRequestContext.session(request);
        WebSqlSession session = manager.createSession(owner(login), password(login));
        return new WebSqlSessionInfo(session);
    }

    @PostMapping("/{id}/statements")
    public WebSqlExecutionResult execute(@PathVariable("id") String id,
            @RequestBody Map<String, String> statement, HttpServletRequest request) {
        SessionValue login = UiRequestContext.session(request);
        return manager.execute(id, owner(login), statement == null ? null : statement.get("sql"));
    }

    @PostMapping("/{id}/cancel")
    public Map<String, Boolean> cancel(@PathVariable("id") String id, HttpServletRequest request) {
        SessionValue login = UiRequestContext.session(request);
        return Collections.singletonMap("cancelRequested", manager.cancel(id, owner(login)));
    }

    @PostMapping("/{id}/reset")
    public WebSqlSessionInfo reset(@PathVariable("id") String id, HttpServletRequest request) {
        SessionValue login = UiRequestContext.session(request);
        return new WebSqlSessionInfo(manager.reset(id, owner(login), password(login)));
    }

    @DeleteMapping("/{id}")
    public Map<String, Boolean> close(@PathVariable("id") String id, HttpServletRequest request) {
        SessionValue login = UiRequestContext.session(request);
        manager.closeSession(id, owner(login));
        return Collections.singletonMap("closed", true);
    }

    private String owner(SessionValue session) {
        return session.currentUser.getQualifiedUser();
    }

    private String password(SessionValue session) {
        return session.password == null ? "" : session.password;
    }

}
