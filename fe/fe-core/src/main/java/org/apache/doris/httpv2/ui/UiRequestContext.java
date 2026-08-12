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

import org.apache.doris.httpv2.HttpAuthManager.SessionValue;

import jakarta.servlet.http.HttpServletRequest;

import java.util.UUID;

public final class UiRequestContext {
    public static final String REQUEST_ID_ATTRIBUTE = UiRequestContext.class.getName() + ".requestId";
    public static final String SESSION_ATTRIBUTE = UiRequestContext.class.getName() + ".session";
    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    public static final String CSRF_HEADER = "X-Doris-CSRF-Token";

    private UiRequestContext() {
    }

    public static String newRequestId() {
        return UUID.randomUUID().toString();
    }

    public static String requestId(HttpServletRequest request) {
        Object value = request.getAttribute(REQUEST_ID_ATTRIBUTE);
        return value instanceof String ? (String) value : newRequestId();
    }

    public static SessionValue session(HttpServletRequest request) {
        Object value = request.getAttribute(SESSION_ATTRIBUTE);
        if (value instanceof SessionValue) {
            return (SessionValue) value;
        }
        throw UiApiException.unauthenticated();
    }
}
