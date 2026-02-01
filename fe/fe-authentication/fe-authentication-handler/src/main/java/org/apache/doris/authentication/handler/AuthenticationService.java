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
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.spi.AuthenticationException;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationResult;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Authentication service - core authentication logic.
 *
 * <p>This class orchestrates the authentication flow:
 * 1. Resolve AuthenticationProfile for the user
 * 2. Get appropriate AuthenticationPlugin
 * 3. Execute authentication
 * 4. Resolve user and map roles
 */
public class AuthenticationService {

    private final ProfileRegistry profileRegistry;
    private final PluginManager pluginManager;
    private final BindingResolver bindingResolver;
    private final UserResolver userResolver;
    private final RoleMapper roleMapper;
    private final SubjectBuilder subjectBuilder;
    private final RoleResolutionStage roleResolutionStage;

    public AuthenticationService(ProfileRegistry profileRegistry, PluginManager pluginManager,
                       BindingResolver bindingResolver) {
        this(profileRegistry, pluginManager, bindingResolver, null, null, null, null);
    }

    public AuthenticationService(ProfileRegistry profileRegistry, PluginManager pluginManager,
                       BindingResolver bindingResolver, UserResolver userResolver,
                       RoleMapper roleMapper, SubjectBuilder subjectBuilder,
                       RoleResolutionStage roleResolutionStage) {
        this.profileRegistry = Objects.requireNonNull(profileRegistry, "profileRegistry");
        this.pluginManager = Objects.requireNonNull(pluginManager, "pluginManager");
        this.bindingResolver = Objects.requireNonNull(bindingResolver, "bindingResolver");
        this.userResolver = userResolver != null ? userResolver : new NoopUserResolver();
        this.roleMapper = roleMapper != null ? roleMapper : new RoleMapper();
        this.subjectBuilder = subjectBuilder != null ? subjectBuilder : new DefaultSubjectBuilder();
        this.roleResolutionStage = roleResolutionStage != null
                ? roleResolutionStage
                : new DefaultRoleResolutionStage(this.roleMapper);
    }

    public AuthenticationService(ProfileRegistry profileRegistry, PluginManager pluginManager,
                       BindingResolver bindingResolver, UserResolver userResolver,
                       RoleMapper roleMapper) {
        this(profileRegistry, pluginManager, bindingResolver, userResolver, roleMapper, null, null);
    }

    /**
     * Execute authentication flow.
     *
     * @param request authentication request
     * @return authentication result
     */
    public AuthenticationResult authenticate(AuthenticationRequest request) throws AuthenticationException {
        return authenticateWithOutcome(request).getAuthResult();
    }

    /**
     * Execute authentication flow and return the full outcome.
     *
     * @param request authentication request
     * @return authentication outcome
     */
    public AuthenticationOutcome authenticateWithOutcome(AuthenticationRequest request) throws AuthenticationException {
        if (request == null) {
            throw new AuthenticationException("AuthenticationRequest must not be null");
        }
        // 1. Resolve candidate profiles
        AuthenticationProfile profile = null;
        AuthenticationPlugin plugin = null;
        for (AuthenticationProfile candidate : bindingResolver.resolveCandidates(
                request.getUsername(), request)) {
            AuthenticationPlugin candidatePlugin = pluginManager.getPlugin(candidate);
            if (candidatePlugin.supports(request)) {
                profile = candidate;
                plugin = candidatePlugin;
                break;
            }
        }
        if (profile == null || plugin == null) {
            throw new AuthenticationException(
                    "No authentication profile supports request for user: " + request.getUsername());
        }

        // 2. Execute authentication
        AuthenticationResult result = plugin.authenticate(request, profile);

        if (!result.isSuccess()) {
            return AuthenticationOutcome.of(profile, result, null, null);
        }

        Object resolvedUser = null;
        if (result.getIdentity() != null) {
            resolvedUser = userResolver.resolveUser(result.getIdentity(), profile);
        }

        Set<String> mappedRoles = Collections.emptySet();
        if (roleResolutionStage != null) {
            mappedRoles = roleResolutionStage.resolveRoles(result.getIdentity(), profile);
        }

        return AuthenticationOutcome.of(profile, result,
                subjectBuilder.build(result.getPrincipal(), mappedRoles, request),
                resolvedUser);
    }
}
