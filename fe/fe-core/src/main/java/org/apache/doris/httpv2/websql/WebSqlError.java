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

import org.springframework.http.HttpStatus;

public enum WebSqlError {
    DISABLED(HttpStatus.SERVICE_UNAVAILABLE, "WEB_SQL_DISABLED", "Web SQL sessions are disabled."),
    INVALID_STATEMENT(HttpStatus.BAD_REQUEST, "WEB_SQL_INVALID_STATEMENT",
            "Exactly one non-empty SQL statement is required."),
    SESSION_NOT_FOUND(HttpStatus.NOT_FOUND, "WEB_SQL_SESSION_NOT_FOUND", "The Web SQL session was not found."),
    SESSION_EXPIRED(HttpStatus.NOT_FOUND, "WEB_SQL_SESSION_EXPIRED",
            "The Web SQL session has expired. Please create a new session."),
    SESSION_BUSY(HttpStatus.CONFLICT, "WEB_SQL_SESSION_BUSY", "The Web SQL session is busy."),
    SESSION_LIMIT_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, "WEB_SQL_SESSION_LIMIT_EXCEEDED",
            "The Web SQL session limit has been reached."),
    AUTHENTICATION_REQUIRED(HttpStatus.UNAUTHORIZED, "WEB_SQL_AUTHENTICATION_REQUIRED",
            "Cookie or Basic authentication is required."),
    ACCESS_DENIED(HttpStatus.FORBIDDEN, "WEB_SQL_ACCESS_DENIED",
            "You do not have access to this Web SQL session."),
    CSRF_INVALID(HttpStatus.FORBIDDEN, "WEB_SQL_CSRF_INVALID",
            "The CSRF token is missing or invalid."),
    QUERY_ERROR(HttpStatus.BAD_REQUEST, "WEB_SQL_QUERY_ERROR", "The SQL statement could not be executed."),
    QUERY_TIMEOUT(HttpStatus.REQUEST_TIMEOUT, "WEB_SQL_QUERY_TIMEOUT", "The SQL statement timed out."),
    CONNECTION_ERROR(HttpStatus.SERVICE_UNAVAILABLE, "WEB_SQL_CONNECTION_ERROR",
            "A Web SQL connection could not be created.");

    private final HttpStatus status;
    private final String code;
    private final String message;

    WebSqlError(HttpStatus status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
