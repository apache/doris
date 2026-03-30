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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;

import com.google.common.base.Strings;

import java.util.Objects;

public final class AuthenticationFailureSummary {
    private final AuthenticationFailureType failureType;
    private final String detailMessage;

    private AuthenticationFailureSummary(AuthenticationFailureType failureType, String detailMessage) {
        this.failureType = Objects.requireNonNull(failureType, "failureType");
        this.detailMessage = Strings.nullToEmpty(detailMessage);
    }

    public static AuthenticationFailureSummary forFailureType(AuthenticationFailureType failureType,
            String detailMessage) {
        return new AuthenticationFailureSummary(failureType, detailMessage);
    }

    public static AuthenticationFailureSummary forException(AuthenticationException exception, String fallbackMessage) {
        AuthenticationException safeException = Objects.requireNonNull(exception, "exception");
        String detailMessage = Strings.isNullOrEmpty(safeException.getMessage())
                ? fallbackMessage
                : safeException.getMessage();
        return new AuthenticationFailureSummary(safeException.getFailureType(), detailMessage);
    }

    public AuthenticationFailureType getFailureType() {
        return failureType;
    }

    public String getDetailMessage() {
        return detailMessage;
    }

    public boolean isSensitiveToClient() {
        return failureType == AuthenticationFailureType.BAD_CREDENTIAL
                || failureType == AuthenticationFailureType.USER_NOT_FOUND
                || failureType == AuthenticationFailureType.ACCESS_DENIED;
    }

    public boolean isOperationalFailure() {
        return failureType == AuthenticationFailureType.SOURCE_UNAVAILABLE
                || failureType == AuthenticationFailureType.MISCONFIGURED
                || failureType == AuthenticationFailureType.INTERNAL_ERROR;
    }

    @Override
    public String toString() {
        return "AuthenticationFailureSummary{"
                + "failureType=" + failureType
                + ", detailMessage='" + detailMessage + '\''
                + '}';
    }
}
