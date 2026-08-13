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

package org.apache.doris.httpv2.websql;

import jakarta.servlet.http.HttpServletRequest;

/** Carries the authenticated Doris identity from the HTTP interceptor to the Web SQL controller. */
public final class WebSqlRequestContext {
    public static final String AUTH_ATTRIBUTE = WebSqlRequestContext.class.getName() + ".auth";

    private WebSqlRequestContext() {
    }

    public static void set(HttpServletRequest request, String owner, String password) {
        request.setAttribute(AUTH_ATTRIBUTE, new Authentication(owner, password));
    }

    public static Authentication authentication(HttpServletRequest request) {
        Object value = request.getAttribute(AUTH_ATTRIBUTE);
        if (value instanceof Authentication) {
            return (Authentication) value;
        }
        throw new WebSqlException(WebSqlError.ACCESS_DENIED);
    }

    /** Authenticated Doris credentials needed to create or reset the session's JDBC connection. */
    public static final class Authentication {
        private final String owner;
        private final String password;

        Authentication(String owner, String password) {
            this.owner = owner;
            this.password = password == null ? "" : password;
        }

        public String getOwner() {
            return owner;
        }

        public String getPassword() {
            return password;
        }
    }
}
