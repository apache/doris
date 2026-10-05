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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolver for authentication integrations.
 *
 * <p>Resolves which AuthenticationIntegration should be used for a given user/request.
 * Follows the resolution priority:
 * <ol>
 *   <li>User explicit binding (from BindingRegistry)</li>
 *   <li>Authentication chain (from IntegrationRegistry)</li>
 *   <li>Default password integration (fallback)</li>
 * </ol>
 *
 * <p>If a user binding exists but the referenced integration is missing,
 * resolution fails with an {@link AuthenticationException}.
 *
 * <p>Design per auth.md: simplified model without role bindings and priority.
 */
public class BindingResolver {

    private final IntegrationRegistry integrationRegistry;
    private final BindingRegistry bindingRegistry;

    public BindingResolver(IntegrationRegistry integrationRegistry, BindingRegistry bindingRegistry) {
        this.integrationRegistry = Objects.requireNonNull(integrationRegistry, "integrationRegistry");
        this.bindingRegistry = Objects.requireNonNull(bindingRegistry, "bindingRegistry");
    }

    /**
     * Resolve AuthenticationIntegration for the given username and request.
     *
     * <p>This returns the first candidate from {@link #resolveCandidates(String, AuthenticationRequest)}.</p>
     *
     * @param username username
     * @param request authentication request
     * @return resolved AuthenticationIntegration
     * @throws AuthenticationException if resolution fails
     */
    public AuthenticationIntegration resolveIntegration(String username,
            AuthenticationRequest request) throws AuthenticationException {
        List<AuthenticationIntegration> candidates = resolveCandidates(username, request);
        if (candidates.isEmpty()) {
            throw new AuthenticationException(
                    "No authentication integration available for user: " + username);
        }
        return candidates.get(0);
    }

    /**
     * Resolve candidate integrations in priority order.
     *
     * <p>Order:
     * <ol>
     *   <li>User binding - explicit integration bound to user</li>
     *   <li>Authentication chain - ordered list from IntegrationRegistry</li>
     *   <li>Default password integration - fallback</li>
     * </ol>
     *
     * @param username the username
     * @param request the authentication request
     * @return list of candidate integrations in priority order
     * @throws AuthenticationException if a user binding points to a missing integration
     */
    public List<AuthenticationIntegration> resolveCandidates(String username,
            AuthenticationRequest request) throws AuthenticationException {
        Map<String, AuthenticationIntegration> candidates = new LinkedHashMap<>();

        // 1. User binding - check if user has explicit binding
        Optional<String> boundIntegration = bindingRegistry.getIntegrationName(username);
        if (boundIntegration.isPresent()) {
            String integrationName = boundIntegration.get();
            Optional<AuthenticationIntegration> integration = integrationRegistry.get(integrationName);
            if (integration.isPresent()) {
                addCandidate(candidates, integration.get());
            } else {
                throw new AuthenticationException("Bound integration not found: " + integrationName
                        + " for user: " + username);
            }
        }

        // 2. Authentication chain - try integrations in defined order
        for (AuthenticationIntegration integration : integrationRegistry.getAuthenticationChain()) {
            addCandidate(candidates, integration);
        }

        // 3. Default password integration - fallback
        AuthenticationIntegration defaultIntegration = integrationRegistry.getDefaultPasswordIntegration();
        if (defaultIntegration != null) {
            addCandidate(candidates, defaultIntegration);
        }

        return new ArrayList<>(candidates.values());
    }

    /**
     * Check if a user has an explicit binding.
     *
     * @param username the username
     * @return true if user has binding
     */
    public boolean hasUserBinding(String username) {
        return bindingRegistry.hasBinding(username);
    }

    /**
     * Get the integration bound to a user, if any.
     *
     * @param username the username
     * @return the bound integration, or empty
     */
    public Optional<AuthenticationIntegration> getUserBoundIntegration(String username) {
        return bindingRegistry.getIntegrationName(username)
                .flatMap(integrationRegistry::get);
    }

    private void addCandidate(Map<String, AuthenticationIntegration> candidates,
            AuthenticationIntegration integration) {
        if (integration != null) {
            candidates.putIfAbsent(integration.getName(), integration);
        }
    }
}
