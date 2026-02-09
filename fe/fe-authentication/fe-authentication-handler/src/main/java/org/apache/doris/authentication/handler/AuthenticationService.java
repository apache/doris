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
 * <ol>
 *   <li>Resolve candidate AuthenticationIntegrations for the user</li>
 *   <li>Get appropriate AuthenticationPlugin for each integration</li>
 *   <li>Execute authentication until success or all fail</li>
 *   <li>Resolve user and map roles</li>
 * </ol>
 *
 * <p>Design per auth.md: uses IntegrationRegistry and simplified BindingRegistry.
 */
public class AuthenticationService {

    private final IntegrationRegistry integrationRegistry;
    private final PluginManager pluginManager;
    private final BindingResolver bindingResolver;
    private final UserResolver userResolver;
    private final RoleMapper roleMapper;
    private final SubjectBuilder subjectBuilder;
    private final RoleResolutionStage roleResolutionStage;

    /**
     * Create a minimal authentication service.
     *
     * @param integrationRegistry the integration registry
     * @param pluginManager the plugin manager
     * @param bindingResolver the binding resolver
     */
    public AuthenticationService(IntegrationRegistry integrationRegistry, PluginManager pluginManager,
                       BindingResolver bindingResolver) {
        this(integrationRegistry, pluginManager, bindingResolver, null, null, null, null);
    }

    /**
     * Create a full-featured authentication service.
     *
     * @param integrationRegistry the integration registry
     * @param pluginManager the plugin manager
     * @param bindingResolver the binding resolver
     * @param userResolver user resolver (optional)
     * @param roleMapper role mapper (optional)
     * @param subjectBuilder subject builder (optional)
     * @param roleResolutionStage role resolution stage (optional)
     */
    public AuthenticationService(IntegrationRegistry integrationRegistry, PluginManager pluginManager,
                       BindingResolver bindingResolver, UserResolver userResolver,
                       RoleMapper roleMapper, SubjectBuilder subjectBuilder,
                       RoleResolutionStage roleResolutionStage) {
        this.integrationRegistry = Objects.requireNonNull(integrationRegistry, "integrationRegistry");
        this.pluginManager = Objects.requireNonNull(pluginManager, "pluginManager");
        this.bindingResolver = Objects.requireNonNull(bindingResolver, "bindingResolver");
        this.userResolver = userResolver != null ? userResolver : new NoopUserResolver();
        this.roleMapper = roleMapper != null ? roleMapper : new RoleMapper();
        this.subjectBuilder = subjectBuilder != null ? subjectBuilder : new DefaultSubjectBuilder();
        this.roleResolutionStage = roleResolutionStage != null
                ? roleResolutionStage
                : new DefaultRoleResolutionStage(this.roleMapper);
    }

    /**
     * Create an authentication service with common components.
     *
     * @param integrationRegistry the integration registry
     * @param pluginManager the plugin manager
     * @param bindingResolver the binding resolver
     * @param userResolver user resolver
     * @param roleMapper role mapper
     */
    public AuthenticationService(IntegrationRegistry integrationRegistry, PluginManager pluginManager,
                       BindingResolver bindingResolver, UserResolver userResolver,
                       RoleMapper roleMapper) {
        this(integrationRegistry, pluginManager, bindingResolver, userResolver, roleMapper, null, null);
    }

    /**
     * Execute authentication flow.
     *
     * @param request authentication request
     * @return authentication result
     * @throws AuthenticationException if authentication fails
     */
    public AuthenticationResult authenticate(AuthenticationRequest request) throws AuthenticationException {
        return authenticateWithOutcome(request).getAuthResult();
    }

    /**
     * Execute authentication flow and return the full outcome.
     *
     * @param request authentication request
     * @return authentication outcome with all details
     * @throws AuthenticationException if authentication process fails
     */
    public AuthenticationOutcome authenticateWithOutcome(AuthenticationRequest request) throws AuthenticationException {
        if (request == null) {
            throw new AuthenticationException("AuthenticationRequest must not be null");
        }

        // 1. Resolve candidate integrations
        AuthenticationIntegration integration = null;
        AuthenticationPlugin plugin = null;

        for (AuthenticationIntegration candidate : bindingResolver.resolveCandidates(
                request.getUsername(), request)) {
            AuthenticationPlugin candidatePlugin = pluginManager.getPlugin(candidate);
            if (candidatePlugin.supports(request)) {
                integration = candidate;
                plugin = candidatePlugin;
                break;
            }
        }

        if (integration == null || plugin == null) {
            throw new AuthenticationException(
                    "No authentication integration supports request for user: " + request.getUsername());
        }

        // 2. Execute authentication
        AuthenticationResult result = plugin.authenticate(request, integration);

        if (!result.isSuccess()) {
            return AuthenticationOutcome.of(integration, result, null, null);
        }

        // 3. Resolve user
        Object resolvedUser = null;
        if (result.getIdentity() != null) {
            resolvedUser = userResolver.resolveUser(result.getIdentity(), integration);
        }

        // 4. Map roles
        Set<String> mappedRoles = Collections.emptySet();
        if (roleResolutionStage != null && result.getIdentity() != null) {
            mappedRoles = roleResolutionStage.resolveRoles(result.getIdentity(), integration);
        }

        // 5. Build subject
        return AuthenticationOutcome.of(integration, result,
                subjectBuilder.build(result.getPrincipal(), mappedRoles, request),
                resolvedUser);
    }

    /**
     * Get the integration registry.
     *
     * @return the integration registry
     */
    public IntegrationRegistry getIntegrationRegistry() {
        return integrationRegistry;
    }

    /**
     * Get the plugin manager.
     *
     * @return the plugin manager
     */
    public PluginManager getPluginManager() {
        return pluginManager;
    }

    /**
     * Get the binding resolver.
     *
     * @return the binding resolver
     */
    public BindingResolver getBindingResolver() {
        return bindingResolver;
    }
}
