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

package org.apache.doris.authentication;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Authentication result returned by authenticator plugins.
 *
 * <p>Authentication can result in one of three states:
 * <ul>
 *   <li>{@link Status#SUCCESS} - Authentication succeeded, principal is available</li>
 *   <li>{@link Status#CONTINUE} - Multi-step auth, more data needed from client</li>
 *   <li>{@link Status#FAILURE} - Authentication failed, exception explains why</li>
 * </ul>
 *
 * <p>{@link Status} drives control flow. {@link AuthenticationException} is only a failure reason
 * when {@link Status#FAILURE} and should not be used for normal control flow.
 * For expected authentication failures (e.g. invalid credentials), return
 * {@link #failure(AuthenticationException)}. Reserve thrown exceptions for internal/plugin errors
 * (e.g. misconfiguration, dependency outage).
 *
 * <p>Use the static factory methods to create instances:
 * <pre>{@code
 * // Success
 * return AuthenticationResult.success(principal);
 *
 * // Need more data (multi-step)
 * return AuthenticationResult.continueWith(state, challengeData);
 *
 * // Failure
 * return AuthenticationResult.failure(new AuthenticationException("Invalid password"));
 * }</pre>
 */
public final class AuthenticationResult {

    /**
     * Authentication result status.
     */
    public enum Status {
        /**
         * Authentication successful.
         * The principal is available via {@link AuthenticationResult#getPrincipal()}.
         */
        SUCCESS,

        /**
         * Authentication needs to continue (multi-step authentication).
         * Send challenge data to client and wait for response.
         */
        CONTINUE,

        /**
         * Authentication failed.
         * The exception is available via {@link AuthenticationResult#getException()} as a failure reason.
         */
        FAILURE
    }

    private final Status status;
    private final Principal principal;
    private final Object nextState;
    private final byte[] challengeData;
    private final AuthenticationException exception;
    private final Set<String> grantedRoles;

    private AuthenticationResult(
            Status status,
            Principal principal,
            Object nextState,
            byte[] challengeData,
            AuthenticationException exception,
            Set<String> grantedRoles) {
        this.status = Objects.requireNonNull(status, "status is required");
        this.principal = principal;
        this.nextState = nextState;
        this.challengeData = challengeData;
        this.exception = exception;
        this.grantedRoles = immutableGrantedRoles(grantedRoles);
    }

    /**
     * Creates a successful authentication result.
     *
     * @param principal the authenticated principal
     * @return success result
     * @throws NullPointerException if principal is null
     */
    public static AuthenticationResult success(Principal principal) {
        return success(principal, Collections.emptySet());
    }

    /**
     * Creates a successful authentication result with granted roles.
     *
     * @param principal the authenticated principal
     * @param grantedRoles the roles granted during authentication handling
     * @return success result
     * @throws NullPointerException if principal or grantedRoles is null
     */
    public static AuthenticationResult success(Principal principal, Set<String> grantedRoles) {
        Objects.requireNonNull(principal, "principal is required for success");
        return new AuthenticationResult(Status.SUCCESS, principal, null, null, null, grantedRoles);
    }

    /**
     * Creates a continue result for multi-step authentication.
     *
     * @param state the authentication state to preserve between steps
     * @param challenge the challenge data to send to client
     * @return continue result
     */
    public static AuthenticationResult continueWith(Object state, byte[] challenge) {
        return new AuthenticationResult(Status.CONTINUE, null, state, challenge, null, Collections.emptySet());
    }

    /**
     * Creates a failure result.
     *
     * @param exception the exception describing the failure
     * @return failure result
     * @throws NullPointerException if exception is null
     */
    public static AuthenticationResult failure(AuthenticationException exception) {
        Objects.requireNonNull(exception, "exception is required for failure");
        return new AuthenticationResult(Status.FAILURE, null, null, null, exception, Collections.emptySet());
    }

    /**
     * Creates a failure result with a message.
     *
     * @param message the error message
     * @return failure result
     */
    public static AuthenticationResult failure(String message) {
        return failure(new AuthenticationException(message));
    }

    /**
     * Creates a failure result with a failure category and message.
     *
     * @param failureType the failure category
     * @param message the error message
     * @return failure result
     */
    public static AuthenticationResult failure(AuthenticationFailureType failureType, String message) {
        return failure(new AuthenticationException(message, failureType));
    }

    /**
     * Returns the authentication status.
     *
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Returns the authenticated principal if successful.
     *
     * @return the principal, or null if not successful
     */
    public Principal getPrincipal() {
        return principal;
    }

    /**
     * Returns the principal as an Optional.
     *
     * @return optional principal
     */
    public Optional<Principal> principal() {
        return Optional.ofNullable(principal);
    }

    /**
     * Returns the next authentication state for multi-step auth.
     *
     * @return the state, or null if not CONTINUE
     */
    public Object getNextState() {
        return nextState;
    }

    /**
     * Returns the challenge data for multi-step auth.
     *
     * @return challenge bytes, or null if not CONTINUE
     */
    public byte[] getChallengeData() {
        return challengeData;
    }

    /**
     * Returns the roles granted during authentication handling.
     *
     * @return immutable granted roles set
     */
    public Set<String> getGrantedRoles() {
        return grantedRoles;
    }

    /**
     * Returns the exception if authentication failed.
     *
     * @return the exception, or null if not FAILURE
     */
    public AuthenticationException getException() {
        return exception;
    }

    /**
     * Returns the exception as an Optional.
     *
     * @return optional exception
     */
    public Optional<AuthenticationException> exception() {
        return Optional.ofNullable(exception);
    }

    /**
     * Checks if authentication was successful.
     *
     * @return true if SUCCESS
     */
    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }

    /**
     * Checks if authentication should continue (multi-step).
     *
     * @return true if CONTINUE
     */
    public boolean isContinue() {
        return status == Status.CONTINUE;
    }

    /**
     * Checks if authentication failed.
     *
     * @return true if FAILURE
     */
    public boolean isFailure() {
        return status == Status.FAILURE;
    }

    private static Set<String> immutableGrantedRoles(Set<String> grantedRoles) {
        Objects.requireNonNull(grantedRoles, "grantedRoles is required");
        return Collections.unmodifiableSet(new LinkedHashSet<>(grantedRoles));
    }

    @Override
    public String toString() {
        switch (status) {
            case SUCCESS:
                return "AuthenticationResult{SUCCESS, principal=" + principal.getName() + "}";
            case CONTINUE:
                return "AuthenticationResult{CONTINUE}";
            case FAILURE:
                return "AuthenticationResult{FAILURE, error="
                        + (exception != null ? exception.getMessage() : "unknown")
                        + "}";
            default:
                return "AuthenticationResult{" + status + "}";
        }
    }
}
