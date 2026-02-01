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

import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * In-memory registry for authentication profiles.
 *
 * <p>This keeps the handler module self-sufficient for integration tests and
 * makes the selection logic explicit (enabled first, then priority, then name
 * for stability).</p>
 */
public class ProfileRegistry {

    private final ConcurrentMap<String, AuthenticationProfile> profiles = new ConcurrentHashMap<>();
    private volatile String defaultPasswordProfileName;

    /**
     * Register an authentication profile.
     *
     * @param profile authentication profile
     */
    public void register(AuthenticationProfile profile) {
        Objects.requireNonNull(profile, "profile");
        profiles.put(profile.getName(), profile);
        if (defaultPasswordProfileName == null
                && profile.getPluginType() == AuthenticationPluginType.PASSWORD) {
            defaultPasswordProfileName = profile.getName();
        }
    }

    /**
     * Get authentication profile by name.
     *
     * @param name profile name
     * @return authentication profile, empty if not found
     */
    public Optional<AuthenticationProfile> get(String name) {
        if (name == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(profiles.get(name));
    }

    /**
     * Get all enabled authentication profiles.
     *
     * @return list of enabled profiles
     */
    public List<AuthenticationProfile> getAllEnabled() {
        return profiles.values().stream()
                .filter(AuthenticationProfile::isEnabled)
                .sorted(Comparator.comparingInt(AuthenticationProfile::getPriority)
                        .thenComparing(AuthenticationProfile::getName))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Get default password profile.
     *
     * @return default password profile
     */
    public AuthenticationProfile getDefaultPasswordProfile() {
        if (defaultPasswordProfileName != null) {
            AuthenticationProfile profile = profiles.get(defaultPasswordProfileName);
            if (profile != null && profile.isEnabled()) {
                return profile;
            }
        }
        return profiles.values().stream()
                .filter(p -> p.isEnabled()
                        && p.getPluginType() == AuthenticationPluginType.PASSWORD)
                .min(Comparator.comparingInt(AuthenticationProfile::getPriority)
                        .thenComparing(AuthenticationProfile::getName))
                .orElse(null);
    }

    /**
     * Set explicit default password profile name; useful when multiple PASSWORD profiles exist.
     */
    public void setDefaultPasswordProfileName(String profileName) {
        this.defaultPasswordProfileName = profileName;
    }
}
