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

import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.Identity;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authentication.Subject;

import java.util.Objects;
import java.util.Optional;

/**
 * Authentication outcome returned by the handler.
 *
 * <p>Contains all information about a completed authentication attempt:
 * <ul>
 *   <li>The integration that was used</li>
 *   <li>The authentication result from the plugin</li>
 *   <li>The built Subject (if successful)</li>
 *   <li>The resolved user object (if any)</li>
 * </ul>
 */
public final class AuthenticationOutcome {

    private final AuthenticationIntegration integration;
    private final AuthenticationResult authResult;
    private final Subject subject;
    private final Object resolvedUser;

    private AuthenticationOutcome(AuthenticationIntegration integration, AuthenticationResult authResult,
            Subject subject, Object resolvedUser) {
        this.integration = Objects.requireNonNull(integration, "integration");
        this.authResult = Objects.requireNonNull(authResult, "authResult");
        this.subject = subject;
        this.resolvedUser = resolvedUser;
    }

    /**
     * Create an outcome with integration, result, and subject.
     *
     * @param integration the integration used
     * @param authResult the authentication result
     * @param subject the built subject (may be null)
     * @return new outcome
     */
    public static AuthenticationOutcome of(AuthenticationIntegration integration, AuthenticationResult authResult,
            Subject subject) {
        return new AuthenticationOutcome(integration, authResult, subject, null);
    }

    /**
     * Create an outcome with all fields.
     *
     * @param integration the integration used
     * @param authResult the authentication result
     * @param subject the built subject (may be null)
     * @param resolvedUser the resolved user object (may be null)
     * @return new outcome
     */
    public static AuthenticationOutcome of(AuthenticationIntegration integration, AuthenticationResult authResult,
            Subject subject, Object resolvedUser) {
        return new AuthenticationOutcome(integration, authResult, subject, resolvedUser);
    }

    /**
     * Get the integration that was used for authentication.
     *
     * @return the integration
     */
    public AuthenticationIntegration getIntegration() {
        return integration;
    }

    /**
     * Get the authentication result from the plugin.
     *
     * @return the result
     */
    public AuthenticationResult getAuthResult() {
        return authResult;
    }

    /**
     * Get the built subject if authentication was successful.
     *
     * @return optional subject
     */
    public Optional<Subject> getSubject() {
        return Optional.ofNullable(subject);
    }

    /**
     * Get the resolved user object if available.
     *
     * @return optional user object
     */
    public Optional<Object> getResolvedUser() {
        return Optional.ofNullable(resolvedUser);
    }

    /**
     * Get the principal from the authentication result.
     *
     * @return optional principal
     */
    public Optional<Principal> getPrincipal() {
        return authResult.principal();
    }

    /**
     * Get the identity from the authentication result.
     *
     * @return optional identity
     */
    public Optional<Identity> getIdentity() {
        return Optional.ofNullable(authResult.getIdentity());
    }

    /**
     * Check if authentication was successful.
     *
     * @return true if success
     */
    public boolean isSuccess() {
        return authResult.isSuccess();
    }

    /**
     * Check if authentication needs to continue (multi-step).
     *
     * @return true if continue needed
     */
    public boolean isContinue() {
        return authResult.isContinue();
    }

    /**
     * Check if authentication failed.
     *
     * @return true if failure
     */
    public boolean isFailure() {
        return authResult.isFailure();
    }

    @Override
    public String toString() {
        return "AuthenticationOutcome{"
                + "integration=" + integration.getName()
                + ", success=" + isSuccess()
                + ", identity=" + getIdentity().orElse(null)
                + '}';
    }
}
