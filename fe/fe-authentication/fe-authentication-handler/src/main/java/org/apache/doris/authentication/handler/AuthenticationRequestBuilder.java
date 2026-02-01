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
 */
public final class AuthenticationRequestBuilder {

    public static final String KEY_AUTH_PROFILE = "auth_profile";
    public static final String KEY_REQUESTED_PROFILE = "requested_profile";

    private AuthenticationRequestBuilder() {
    }

    /**
     * Resolve requested profile from an AuthenticationRequest.
     */
    public static Optional<String> resolveRequestedProfile(AuthenticationRequest request) {
        if (request == null) {
            return Optional.empty();
        }
        Optional<String> direct = request.getRequestedProfile();
        if (direct.isPresent() && !direct.get().isEmpty()) {
            return direct;
        }
        return resolveRequestedProfile(null, request.getProperties());
    }

    /**
     * Resolve requested profile from explicit value and properties map.
     */
    public static Optional<String> resolveRequestedProfile(String explicit, Map<String, String> properties) {
        if (explicit != null && !explicit.isEmpty()) {
            return Optional.of(explicit);
        }
        if (properties == null || properties.isEmpty()) {
            return Optional.empty();
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key == null) {
                continue;
            }
            if (key.equalsIgnoreCase(KEY_AUTH_PROFILE) || key.equalsIgnoreCase(KEY_REQUESTED_PROFILE)) {
                String value = entry.getValue();
                if (value != null && !value.isEmpty()) {
                    return Optional.of(value);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Apply resolved requested profile into an AuthenticationRequest builder.
     */
    public static AuthenticationRequest.Builder applyRequestedProfile(AuthenticationRequest.Builder builder,
                                                            String explicit,
                                                            Map<String, String> properties) {
        if (builder == null) {
            throw new IllegalArgumentException("builder is required");
        }
        Optional<String> resolved = resolveRequestedProfile(explicit, properties);
        if (resolved.isPresent()) {
            builder.requestedProfile(resolved.get());
        }
        return builder;
    }
}
