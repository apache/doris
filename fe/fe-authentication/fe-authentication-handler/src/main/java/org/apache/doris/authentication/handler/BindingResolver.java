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

import org.apache.doris.authentication.AuthenticationBinding;
import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.spi.AuthenticationException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolver for authentication bindings.
 *
 * <p>Resolves which AuthenticationProfile should be used for a given user/request.
 * Follows the binding priority:
 * 1. User explicit binding
 * 2. Role binding
 * 3. Default binding
 * 4. System default
 */
public class BindingResolver {

    private final ProfileRegistry profileRegistry;
    private final BindingRegistry bindingRegistry;

    public BindingResolver(ProfileRegistry profileRegistry, BindingRegistry bindingRegistry) {
        this.profileRegistry = Objects.requireNonNull(profileRegistry, "profileRegistry");
        this.bindingRegistry = Objects.requireNonNull(bindingRegistry, "bindingRegistry");
    }

    /**
     * Resolve AuthenticationProfile for the given username and request.
     *
     * <p>This returns the first candidate from {@link #resolveCandidates(String, AuthenticationRequest)}.</p>
     *
     * @param username username
     * @param request authentication request
     * @return resolved AuthenticationProfile
     * @throws AuthenticationException if resolution fails
     */
    public AuthenticationProfile resolveProfile(String username,
            AuthenticationRequest request) throws AuthenticationException {
        List<AuthenticationProfile> candidates = resolveCandidates(username, request);
        if (candidates.isEmpty()) {
            throw new AuthenticationException(
                    "No authentication profile available for user: " + username);
        }
        return candidates.get(0);
    }

    /**
     * Resolve candidate profiles in priority order.
     *
     * <p>Order: user binding -> explicit requested -> default bindings -> system default.</p>
     */
    public List<AuthenticationProfile> resolveCandidates(String username,
            AuthenticationRequest request) throws AuthenticationException {
        Map<String, AuthenticationProfile> candidates = new LinkedHashMap<>();

        // 1. User binding
        Optional<AuthenticationBinding> userBinding = bindingRegistry.getUserBinding(username);
        if (userBinding.isPresent()) {
            addCandidate(candidates, resolveProfile(userBinding.get()));
        }

        // 2. Explicitly requested profile (client-provided)
        Optional<String> requested = AuthenticationRequestBuilder.resolveRequestedProfile(request);
        if (requested.isPresent()) {
            if (userBinding.isPresent()) {
                resolveRequestedProfileOptional(requested.get())
                        .ifPresent(profile -> addCandidate(candidates, profile));
            } else {
                AuthenticationProfile profile = resolveRequestedProfile(requested.get());
                addCandidate(candidates, profile);
            }
        }

        // 3. Default binding
        List<AuthenticationBinding> defaults = bindingRegistry.getDefaultBindings();
        for (AuthenticationBinding binding : defaults) {
            AuthenticationProfile profile = resolveProfile(binding);
            addCandidate(candidates, profile);
        }

        // 4. System default (Password plugin as baseline)
        AuthenticationProfile fallback = profileRegistry.getDefaultPasswordProfile();
        if (fallback != null) {
            addCandidate(candidates, fallback);
        }

        return new ArrayList<>(candidates.values());
    }

    private AuthenticationProfile resolveProfile(AuthenticationBinding binding) throws AuthenticationException {
        Optional<AuthenticationProfile> profile = profileRegistry.get(binding.getProfileName());
        if (!profile.isPresent()) {
            if (binding.isMandatory()) {
                throw new AuthenticationException(
                        "Mandatory authentication profile not found: " + binding.getProfileName());
            }
            // fall through to default selection by type to keep login possible
            return profileRegistry.getDefaultPasswordProfile();
        }
        AuthenticationProfile resolved = profile.get();
        if (!resolved.isEnabled()) {
            if (binding.isMandatory()) {
                throw new AuthenticationException("Authentication profile disabled: " + resolved.getName());
            }
            if (resolved.getPluginType() == AuthenticationPluginType.PASSWORD) {
                return profileRegistry.getDefaultPasswordProfile();
            }
        }
        return resolved;
    }

    private AuthenticationProfile resolveRequestedProfile(String profileName) throws AuthenticationException {
        Optional<AuthenticationProfile> profile = profileRegistry.get(profileName);
        if (!profile.isPresent()) {
            throw new AuthenticationException(
                    "Requested authentication profile not found: " + profileName);
        }
        AuthenticationProfile resolved = profile.get();
        if (!resolved.isEnabled()) {
            throw new AuthenticationException(
                    "Requested authentication profile is disabled: " + profileName);
        }
        return resolved;
    }

    private Optional<AuthenticationProfile> resolveRequestedProfileOptional(String profileName) {
        Optional<AuthenticationProfile> profile = profileRegistry.get(profileName);
        if (!profile.isPresent()) {
            return Optional.empty();
        }
        AuthenticationProfile resolved = profile.get();
        if (!resolved.isEnabled()) {
            return Optional.empty();
        }
        return Optional.of(resolved);
    }

    private void addCandidate(Map<String, AuthenticationProfile> candidates, AuthenticationProfile profile) {
        if (profile != null) {
            candidates.putIfAbsent(profile.getName(), profile);
        }
    }
}
