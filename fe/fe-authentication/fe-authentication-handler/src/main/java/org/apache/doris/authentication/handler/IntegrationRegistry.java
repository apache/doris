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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for AuthenticationIntegration instances.
 *
 * <p>Manages:
 * <ul>
 *   <li>Registered integrations by name</li>
 *   <li>Authentication chain (ordered list of integrations)</li>
 *   <li>Default fallback integration (usually password)</li>
 * </ul>
 */
public class IntegrationRegistry {

    private final Map<String, AuthenticationIntegration> integrations = new ConcurrentHashMap<>();
    private final List<AuthenticationIntegration> chain = new CopyOnWriteArrayList<>();
    private volatile AuthenticationIntegration defaultIntegration;

    /**
     * Register an integration.
     *
     * @param integration the integration to register
     */
    public void register(AuthenticationIntegration integration) {
        if (integration != null) {
            integrations.put(integration.getName(), integration);
        }
    }

    /**
     * Unregister an integration by name.
     *
     * @param name the integration name
     */
    public void unregister(String name) {
        if (name != null) {
            integrations.remove(name);
            chain.removeIf(i -> i.getName().equals(name));
            if (defaultIntegration != null && defaultIntegration.getName().equals(name)) {
                defaultIntegration = null;
            }
        }
    }

    /**
     * Get an integration by name.
     *
     * @param name the integration name
     * @return the integration, or empty if not found
     */
    public Optional<AuthenticationIntegration> get(String name) {
        return Optional.ofNullable(integrations.get(name));
    }

    /**
     * Get all registered integrations.
     *
     * @return map of name to integration
     */
    public Map<String, AuthenticationIntegration> getAll() {
        return Collections.unmodifiableMap(integrations);
    }

    /**
     * Get the authentication chain.
     *
     * @return list of integrations in the chain
     */
    public List<AuthenticationIntegration> getAuthenticationChain() {
        return Collections.unmodifiableList(chain);
    }

    /**
     * Set the authentication chain.
     *
     * @param chain the new chain of integrations
     */
    public void setAuthenticationChain(List<AuthenticationIntegration> chain) {
        this.chain.clear();
        if (chain != null) {
            this.chain.addAll(chain);
        }
    }

    /**
     * Get the default fallback integration (usually password).
     *
     * @return the default integration, or null
     */
    public AuthenticationIntegration getDefaultPasswordIntegration() {
        return defaultIntegration;
    }

    /**
     * Set the default fallback integration.
     *
     * @param integration the default integration
     */
    public void setDefaultPasswordIntegration(AuthenticationIntegration integration) {
        this.defaultIntegration = integration;
    }

    /**
     * Clear all integrations.
     */
    public void clear() {
        integrations.clear();
        chain.clear();
        defaultIntegration = null;
    }
}
