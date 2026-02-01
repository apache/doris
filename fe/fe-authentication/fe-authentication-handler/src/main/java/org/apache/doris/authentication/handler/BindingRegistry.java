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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * In-memory registry for authentication bindings.
 *
 * <p>Supports user, role, and default bindings with priority ordering. This is
 * intentionally simple so it can be replaced by a persistent implementation in
 * fe-core without changing handler logic.</p>
 */
public class BindingRegistry {

    private final ConcurrentMap<String, List<AuthenticationBinding>> userBindings = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<AuthenticationBinding>> roleBindings = new ConcurrentHashMap<>();
    private final List<AuthenticationBinding> defaultBindings = new ArrayList<>();

    /** Register or replace a user binding. */
    public void putUserBinding(String username, AuthenticationBinding binding) {
        userBindings.compute(username, (k, v) -> mergeBinding(v, binding));
    }

    /** Register or replace a role binding. */
    public void putRoleBinding(String role, AuthenticationBinding binding) {
        roleBindings.compute(role, (k, v) -> mergeBinding(v, binding));
    }

    /** Register a default binding. */
    public void addDefaultBinding(AuthenticationBinding binding) {
        synchronized (defaultBindings) {
            List<AuthenticationBinding> merged = mergeBinding(defaultBindings, binding);
            defaultBindings.clear();
            defaultBindings.addAll(merged);
        }
    }

    /**
     * Get user binding.
     *
     * @param username username
     * @return binding if exists
     */
    public Optional<AuthenticationBinding> getUserBinding(String username) {
        return Optional.ofNullable(userBindings.get(username))
            .flatMap(list -> list.stream()
                .sorted(bindingOrder())
                .findFirst());
    }

    /**
     * Get role bindings.
     *
     * @param roles set of role names
     * @return list of bindings
     */
    public List<AuthenticationBinding> getRoleBindings(Set<String> roles) {
        return roles.stream()
                .map(roleBindings::get)
                .filter(list -> list != null && !list.isEmpty())
                .flatMap(List::stream)
                .sorted(bindingOrder())
                .collect(Collectors.toList());
    }

    /**
     * Get default bindings.
     *
     * @return list of default bindings
     */
    public List<AuthenticationBinding> getDefaultBindings() {
        synchronized (defaultBindings) {
            return defaultBindings.stream()
                    .sorted(bindingOrder())
                    .collect(Collectors.toList());
        }
    }

    private Comparator<AuthenticationBinding> bindingOrder() {
        return Comparator.comparingInt(AuthenticationBinding::getPriority)
                .thenComparing(AuthenticationBinding::getProfileName);
    }

    private List<AuthenticationBinding> mergeBinding(
            List<AuthenticationBinding> current,
            AuthenticationBinding binding) {
        List<AuthenticationBinding> next = current == null ? new ArrayList<>() : new ArrayList<>(current);
        next.removeIf(b -> b.getType() == binding.getType()
                && b.getTargetName().equals(binding.getTargetName()));
        next.add(binding);
        return next;
    }
}
