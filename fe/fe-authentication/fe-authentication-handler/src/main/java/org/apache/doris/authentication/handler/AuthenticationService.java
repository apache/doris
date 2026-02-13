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

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.spi.AuthenticationPlugin;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Authentication service - core authentication orchestration.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Resolve candidate {@link AuthenticationIntegration} for the user</li>
 *   <li>Ask {@link AuthenticationPluginManager} for a plugin instance</li>
 *   <li>Execute authentication until success or all candidates fail</li>
 *   <li>Return authentication result (principal)</li>
 * </ol>
 *
 * <p>Dependency relationship:
 * <ul>
 *   <li>Service depends on {@link AuthenticationPluginManager}</li>
 *   <li>Not a peer: the manager provides plugin lifecycle and caching</li>
 *   <li>Service remains the orchestrator and does not manage plugins directly</li>
 * </ul>
 *
 * <p>Design per auth.md: uses IntegrationRegistry and simplified BindingRegistry.
 */
public class AuthenticationService {

    private final IntegrationRegistry integrationRegistry;
    private final AuthenticationPluginManager pluginManager;
    private final BindingResolver bindingResolver;

    /**
     * Create a minimal authentication service.
     *
     * @param integrationRegistry the integration registry
     * @param pluginManager the plugin manager
     * @param bindingResolver the binding resolver
     */
    public AuthenticationService(IntegrationRegistry integrationRegistry, AuthenticationPluginManager pluginManager,
                       BindingResolver bindingResolver) {
        this.integrationRegistry = Objects.requireNonNull(integrationRegistry, "integrationRegistry");
        this.pluginManager = Objects.requireNonNull(pluginManager, "pluginManager");
        this.bindingResolver = Objects.requireNonNull(bindingResolver, "bindingResolver");
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
        List<AuthenticationIntegration> candidates = bindingResolver.resolveCandidates(
                request.getUsername(), request);
        if (candidates.isEmpty()) {
            throw new AuthenticationException(
                    "No authentication integration available for user: " + request.getUsername());
        }

        boolean hasUserBinding = bindingResolver.hasUserBinding(request.getUsername());

        // 2. User binding is mandatory: only try the bound integration
        if (hasUserBinding) {
            AuthenticationIntegration integration = candidates.get(0);
            AuthenticationPlugin plugin = pluginManager.getPlugin(integration);
            if (!plugin.supports(request)) {
                throw new AuthenticationException(
                        "No authentication integration supports request for user: " + request.getUsername());
            }
            AuthenticationResult result = plugin.authenticate(request, integration);
            return AuthenticationOutcome.of(integration, result);
        }

        // 3. No binding: try candidates in order until success or all fail
        AuthenticationIntegration lastFailureIntegration = null;
        AuthenticationResult lastFailureResult = null;
        boolean anySupported = false;

        for (AuthenticationIntegration candidate : candidates) {
            AuthenticationPlugin candidatePlugin;
            try {
                candidatePlugin = pluginManager.getPlugin(candidate);
            } catch (AuthenticationException e) {
                lastFailureIntegration = candidate;
                lastFailureResult = AuthenticationResult.failure(new AuthenticationException(
                        "Failed to prepare integration '" + candidate.getName() + "': " + e.getMessage(), e));
                continue;
            }
            if (!candidatePlugin.supports(request)) {
                continue;
            }
            anySupported = true;

            AuthenticationResult result;
            try {
                result = candidatePlugin.authenticate(request, candidate);
            } catch (AuthenticationException e) {
                lastFailureIntegration = candidate;
                lastFailureResult = AuthenticationResult.failure(e);
                continue;
            }
            if (result.isSuccess() || result.isContinue()) {
                return AuthenticationOutcome.of(candidate, result);
            }
            lastFailureIntegration = candidate;
            lastFailureResult = result;
        }

        if (lastFailureIntegration != null && lastFailureResult != null) {
            return AuthenticationOutcome.of(lastFailureIntegration, lastFailureResult);
        }
        if (!anySupported) {
            throw new AuthenticationException(
                    "No authentication integration supports request for user: " + request.getUsername());
        }

        throw new AuthenticationException("Authentication failed for user: " + request.getUsername());
    }

    /**
     * Load external plugins from plugin roots.
     *
     * @param pluginRoots plugin root directories
     * @param parent parent classloader
     * @throws AuthenticationException if loading fails
     */
    public void loadExternalPlugins(List<Path> pluginRoots, ClassLoader parent) throws AuthenticationException {
        pluginManager.loadAll(pluginRoots, parent);
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
    public AuthenticationPluginManager getPluginManager() {
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
