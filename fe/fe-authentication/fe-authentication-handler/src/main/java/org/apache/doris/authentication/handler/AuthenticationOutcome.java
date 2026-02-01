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

package org.apache.doris.authentication.handler;

import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.Identity;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authentication.Subject;
import org.apache.doris.authentication.spi.AuthenticationResult;

import java.util.Objects;
import java.util.Optional;

/**
 * Authentication outcome returned by the handler.
 */
public final class AuthenticationOutcome {

    private final AuthenticationProfile profile;
    private final AuthenticationResult authResult;
    private final Subject subject;
    private final Object resolvedUser;

    private AuthenticationOutcome(AuthenticationProfile profile, AuthenticationResult authResult,
            Subject subject, Object resolvedUser) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.authResult = Objects.requireNonNull(authResult, "authResult");
        this.subject = subject;
        this.resolvedUser = resolvedUser;
    }

    public static AuthenticationOutcome of(AuthenticationProfile profile, AuthenticationResult authResult,
            Subject subject) {
        return new AuthenticationOutcome(profile, authResult, subject, null);
    }

    public static AuthenticationOutcome of(AuthenticationProfile profile, AuthenticationResult authResult,
            Subject subject, Object resolvedUser) {
        return new AuthenticationOutcome(profile, authResult, subject, resolvedUser);
    }

    public AuthenticationProfile getProfile() {
        return profile;
    }

    public AuthenticationResult getAuthResult() {
        return authResult;
    }

    public Optional<Subject> getSubject() {
        return Optional.ofNullable(subject);
    }

    public Optional<Object> getResolvedUser() {
        return Optional.ofNullable(resolvedUser);
    }

    public Optional<Principal> getPrincipal() {
        return authResult.principal();
    }

    public Optional<Identity> getIdentity() {
        return Optional.ofNullable(authResult.getIdentity());
    }

    public boolean isSuccess() {
        return authResult.isSuccess();
    }

    public boolean isContinue() {
        return authResult.isContinue();
    }

    public boolean isFailure() {
        return authResult.isFailure();
    }
}
