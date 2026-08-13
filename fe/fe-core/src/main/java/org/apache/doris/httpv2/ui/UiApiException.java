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

import org.springframework.http.HttpStatus;

public class UiApiException extends RuntimeException {
    private final HttpStatus status;
    private final String code;

    public UiApiException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public static UiApiException unauthenticated() {
        return new UiApiException(HttpStatus.UNAUTHORIZED, "UI_UNAUTHENTICATED", "Authentication is required.");
    }

    public static UiApiException adminRequired() {
        return new UiApiException(
                HttpStatus.FORBIDDEN,
                "UI_ADMIN_REQUIRED",
                "This account is authenticated but is not authorized to use the Doris Web Console.");
    }

    public static UiApiException invalidCsrf() {
        return new UiApiException(HttpStatus.FORBIDDEN, "UI_CSRF_INVALID", "The CSRF token is missing or invalid.");
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }

}
