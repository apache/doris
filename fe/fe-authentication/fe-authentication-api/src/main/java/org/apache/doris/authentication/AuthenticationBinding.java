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

import java.util.Objects;

/**
 * User binding configuration - specifies which Integration a user should use for authentication.
 *
 * <p>Supports two binding modes:
 * <ul>
 *   <li>User-level binding: specific user binds to specific Integration</li>
 *   <li>Global default: handled via AUTHENTICATION CHAIN, not separate DEFAULT binding</li>
 * </ul>
 *
 * <p>Example SQL:
 * <pre>{@code
 * CREATE USER 'alice'@'%' IDENTIFIED WITH corp_ldap;
 * }</pre>
 *
 * <p>Authentication selection logic:
 * <pre>
 * 1. User alice logs in
 *    → Check if AuthenticationBinding(username="alice") exists
 *    → If yes, use the bound Integration
 *    → If no, use AUTHENTICATION CHAIN
 *
 * 2. User bob logs in (no binding)
 *    → No AuthenticationBinding
 *    → Use AUTHENTICATION CHAIN to try each Integration in order
 * </pre>
 */
public class AuthenticationBinding {

    /** Username (null or empty means this is a default Integration) */
    private final String username;

    /** AuthenticationIntegration name */
    private final String integrationName;

    /**
     * Creates a user-level binding.
     *
     * @param username the username
     * @param integrationName the integration name
     */
    public AuthenticationBinding(String username, String integrationName) {
        this.username = username;
        this.integrationName = Objects.requireNonNull(integrationName, "integrationName is required");
    }

    /**
     * Creates a binding from username and integration name.
     *
     * @param username the username (can be null for default binding)
     * @param integrationName the integration name
     * @return new binding instance
     */
    public static AuthenticationBinding of(String username, String integrationName) {
        return new AuthenticationBinding(username, integrationName);
    }

    /**
     * Creates a user-specific binding.
     *
     * @param username the username
     * @param integrationName the integration name
     * @return new binding instance
     */
    public static AuthenticationBinding forUser(String username, String integrationName) {
        Objects.requireNonNull(username, "username is required for user binding");
        return new AuthenticationBinding(username, integrationName);
    }

    /**
     * Returns the username.
     *
     * @return username, may be null if this is a default binding
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the integration name.
     *
     * @return integration name
     */
    public String getIntegrationName() {
        return integrationName;
    }

    /**
     * Checks if this is a user-level binding.
     *
     * @return true if username is non-null and non-empty
     */
    public boolean isUserBinding() {
        return username != null && !username.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthenticationBinding that = (AuthenticationBinding) o;
        return Objects.equals(username, that.username)
                && Objects.equals(integrationName, that.integrationName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, integrationName);
    }

    @Override
    public String toString() {
        return "AuthenticationBinding{"
                + "username='" + username + '\''
                + ", integrationName='" + integrationName + '\''
                + '}';
    }
}
