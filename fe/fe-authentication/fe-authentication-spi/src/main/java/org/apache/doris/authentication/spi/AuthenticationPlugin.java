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

import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.extension.spi.Plugin;

/**
 * Authentication plugin interface.
 * All authentication plugins must implement this interface.
 */
public interface AuthenticationPlugin extends Plugin {

    // ==================== Basic Information ====================

    /** Plugin name (unique identifier) */
    String name();

    /** Plugin type */
    AuthenticationPluginType type();

    /** Plugin description */
    default String description() {
        return "Authentication plugin: " + name();
    }

    // ==================== Capability Check ====================

    /** Whether this request is supported */
    boolean supports(AuthenticationRequest request);

    /** Whether clear password is required */
    default boolean requiresClearPassword() {
        return false;
    }

    /** Whether multi-step authentication is supported */
    default boolean supportsMultiStep() {
        return false;
    }

    // ==================== Authentication Execution ====================

    /**
     * Execute authentication.
     *
     * @param request authentication request
     * @param profile authentication profile
     * @return authentication result
     */
    AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationProfile profile)
            throws AuthenticationException;

    // ==================== Lifecycle ====================

    /**
     * Validate configuration is valid.
     * Called when Profile is created/updated.
     */
    default void validate(AuthenticationProfile profile) throws AuthenticationException {
        // Default: no validation
    }

    /**
     * Initialize plugin.
     * Called when Profile is first used.
     */
    default void initialize(AuthenticationProfile profile) throws AuthenticationException {
        // Default: no initialization
    }

    /**
     * Health check.
     * Called periodically to check if external dependencies are available.
     */
    default boolean healthCheck(AuthenticationProfile profile) {
        return true;
    }

    /**
     * Reload configuration.
     * Called when Profile is updated.
     */
    default void reload(AuthenticationProfile profile) throws AuthenticationException {
        // Default: re-initialize
        initialize(profile);
    }

    /**
     * Close plugin, release resources.
     */
    default void close() {
        // Default: no cleanup needed
    }
}
