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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory registry for authentication bindings.
 *
 * <p>Stores user-level bindings that map usernames to specific integrations.
 * This is intentionally simple so it can be replaced by a persistent
 * implementation in fe-core without changing handler logic.</p>
 *
 * <p>Design per auth.md:
 * <ul>
 *   <li>User-level binding: specific user binds to specific Integration</li>
 *   <li>Global default: handled via AUTHENTICATION CHAIN in IntegrationRegistry</li>
 *   <li>No priority/type/role - simplified model</li>
 * </ul>
 */
public class BindingRegistry {

    /** User bindings: username -> binding */
    private final ConcurrentMap<String, AuthenticationBinding> userBindings = new ConcurrentHashMap<>();

    /**
     * Register a user binding.
     * If a binding already exists for the user, it will be replaced.
     *
     * @param binding the binding to register
     * @throws IllegalArgumentException if binding is not a user binding
     */
    public void register(AuthenticationBinding binding) {
        Objects.requireNonNull(binding, "binding");
        if (!binding.isUserBinding()) {
            throw new IllegalArgumentException("Only user bindings can be registered");
        }
        userBindings.put(binding.getUsername(), binding);
    }

    /**
     * Register a user binding with explicit username.
     *
     * @param username the username
     * @param integrationName the integration name
     */
    public void register(String username, String integrationName) {
        register(AuthenticationBinding.forUser(username, integrationName));
    }

    /**
     * Remove a user binding.
     *
     * @param username the username
     * @return the removed binding, or empty if not found
     */
    public Optional<AuthenticationBinding> unregister(String username) {
        if (username == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(userBindings.remove(username));
    }

    /**
     * Get user binding.
     *
     * @param username the username
     * @return the binding if exists
     */
    public Optional<AuthenticationBinding> getBinding(String username) {
        if (username == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(userBindings.get(username));
    }

    /**
     * Get the integration name bound to a user.
     *
     * @param username the username
     * @return the integration name if bound
     */
    public Optional<String> getIntegrationName(String username) {
        return getBinding(username).map(AuthenticationBinding::getIntegrationName);
    }

    /**
     * Check if a user has a binding.
     *
     * @param username the username
     * @return true if bound
     */
    public boolean hasBinding(String username) {
        return username != null && userBindings.containsKey(username);
    }

    /**
     * Get all registered bindings.
     *
     * @return collection of all bindings
     */
    public Collection<AuthenticationBinding> getAllBindings() {
        return new ArrayList<>(userBindings.values());
    }

    /**
     * Get all usernames that have bindings.
     *
     * @return list of bound usernames
     */
    public List<String> getBoundUsernames() {
        return new ArrayList<>(userBindings.keySet());
    }

    /**
     * Get the number of registered bindings.
     *
     * @return count of bindings
     */
    public int size() {
        return userBindings.size();
    }

    /**
     * Clear all bindings.
     */
    public void clear() {
        userBindings.clear();
    }
}
