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

import org.apache.doris.authentication.AuthenticationRequest;

import java.util.Map;
import java.util.Optional;

/**
 * Helper utilities for constructing {@link AuthenticationRequest}.
 *
 * <p>Design note: Per auth.md, the concept of "requestedProfile" has been replaced
 * by explicit user bindings to integrations. This class provides utilities for
 * resolving integration names from request properties when needed for backward
 * compatibility with legacy clients.
 */
public final class AuthenticationRequestBuilder {

    public static final String KEY_AUTH_INTEGRATION = "auth_integration";
    public static final String KEY_REQUESTED_INTEGRATION = "requested_integration";

    // Legacy keys for backward compatibility
    public static final String KEY_AUTH_PROFILE = "auth_profile";
    public static final String KEY_REQUESTED_PROFILE = "requested_profile";

    private AuthenticationRequestBuilder() {
    }

    /**
     * Resolve requested integration name from an AuthenticationRequest's properties.
     *
     * <p>This is used when legacy clients pass integration hints via properties.
     * The preferred approach is explicit user bindings via BindingRegistry.
     *
     * @param request the authentication request
     * @return the integration name if found in properties
     */
    public static Optional<String> resolveRequestedIntegration(AuthenticationRequest request) {
        if (request == null) {
            return Optional.empty();
        }
        return resolveFromProperties(request.getProperties());
    }

    /**
     * Resolve requested integration name from properties map.
     *
     * <p>Checks for both new (integration) and legacy (profile) property keys.
     *
     * @param properties the properties map
     * @return the integration name if found
     */
    private static Optional<String> resolveFromProperties(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return Optional.empty();
        }

        // Check new keys first
        String[] keysToCheck = {
            KEY_AUTH_INTEGRATION,
            KEY_REQUESTED_INTEGRATION,
            KEY_AUTH_PROFILE,           // Legacy
            KEY_REQUESTED_PROFILE       // Legacy
        };

        for (String key : keysToCheck) {
            Object value = getIgnoreCase(properties, key);
            if (value instanceof String) {
                String strValue = (String) value;
                if (!strValue.isEmpty()) {
                    return Optional.of(strValue);
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Get a value from the map with case-insensitive key matching.
     */
    private static Object getIgnoreCase(Map<String, Object> map, String key) {
        // Try exact match first
        Object value = map.get(key);
        if (value != null) {
            return value;
        }

        // Try case-insensitive match
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }

        return null;
    }
}
