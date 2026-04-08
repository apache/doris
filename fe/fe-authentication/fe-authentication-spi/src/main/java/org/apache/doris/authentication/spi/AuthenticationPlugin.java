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

package org.apache.doris.authentication.spi;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.extension.spi.Plugin;

/**
 * Authentication plugin interface.
 *
 * <p>All authentication plugins (Password/LDAP/OIDC/Kerberos etc.) must implement this interface.
 *
 * <p>Design reference: Trino's PasswordAuthenticator/CertificateAuthenticator
 * <p>Differences: supports dynamic configuration, multiple instances, lifecycle management
 *
 * <p>Key design principles:
 * <ul>
 *   <li>Plugin name is a string (not enum) for extensibility</li>
 *   <li>Plugins are protocol-agnostic and encryption-agnostic</li>
 *   <li>Configuration is passed via AuthenticationIntegration</li>
 * </ul>
 */
public interface AuthenticationPlugin extends Plugin {

    // ==================== Basic Information ====================

    /**
     * Plugin name (globally unique).
     * Example: "ldap", "oidc", "saml2", "kerberos", "password"
     *
     * @return plugin name
     */
    String name();

    /**
     * Plugin description.
     *
     * @return description
     */
    default String description() {
        return "Authentication plugin: " + name();
    }

    // ==================== Capability Check ====================

    /**
     * Whether this plugin supports the given authentication request.
     *
     * @param request authentication request
     * @return true if supported, false otherwise (will try next plugin)
     */
    boolean supports(AuthenticationRequest request);

    /**
     * Whether this plugin requires clear text password.
     * LDAP/Password plugins need clear text, Kerberos/Certificate plugins don't.
     *
     * @return true if clear password is required
     */
    default boolean requiresClearPassword() {
        return false;
    }

    /**
     * Whether this plugin supports multi-step authentication (challenge-response).
     * Kerberos/OIDC may need this, Password/LDAP don't.
     *
     * @return true if multi-step auth is supported
     */
    default boolean supportsMultiStep() {
        return false;
    }

    // ==================== Authentication Execution ====================

    /**
     * Execute authentication (core method).
     *
     * @param request authentication request (contains username, credential, source IP, etc.)
     * @param integration authentication configuration instance (contains plugin configuration)
     * @return authentication result (contains Principal or error message)
     * @throws AuthenticationException for internal/plugin errors (e.g. misconfiguration,
     *         dependency outages). Expected authentication failures (e.g. invalid credentials)
     *         should return {@link AuthenticationResult#failure(AuthenticationException)} instead.
     */
    AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationIntegration integration)
            throws AuthenticationException;

    // ==================== Lifecycle ====================

    /**
     * Validate configuration (called when Integration is created/updated).
     *
     * @param integration configuration to validate
     * @throws AuthenticationException if configuration is invalid
     */
    default void validate(AuthenticationIntegration integration) throws AuthenticationException {
        // Default: no validation
    }

    /**
     * Initialize plugin (called when Integration is first used or on startup).
     *
     * @param integration configuration instance
     * @throws AuthenticationException if initialization fails
     */
    default void initialize(AuthenticationIntegration integration) throws AuthenticationException {
        // Default: no initialization
    }

    /**
     * Reload configuration (called when Integration is updated via ALTER).
     *
     * @param integration new configuration instance
     * @throws AuthenticationException if reload fails
     */
    default void reload(AuthenticationIntegration integration) throws AuthenticationException {
        // Default: re-initialize
        initialize(integration);
    }

    /**
     * Close plugin, release resources (called on shutdown or when Integration is deleted).
     */
    default void close() {
        // Default: no cleanup needed
    }
}
