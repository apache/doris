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

package org.apache.doris.authentication.rolemapping;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.Principal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Evaluates inline role-mapping rules stored on integration properties.
 */
public final class IntegrationPropertyRoleMappingEvaluator implements RoleMappingEvaluator {

    static final String RULE_PREFIX = "role_mapping.rule.";
    private static final String CONDITION_SUFFIX = ".condition";
    private static final String ROLES_SUFFIX = ".roles";
    private static final Set<String> KNOWN_MULTI_VALUE_KEYS = Set.of("roles", "scope", "scp", "permissions");

    private final ConcurrentMap<String, CachedMapping> cache = new ConcurrentHashMap<>();

    @Override
    public Set<String> evaluate(AuthenticationIntegration integration, Principal principal)
            throws AuthenticationException {
        Objects.requireNonNull(integration, "integration");
        Objects.requireNonNull(principal, "principal");

        CachedMapping cachedMapping;
        try {
            cachedMapping = cache.compute(integration.getName(), (ignored, existing) -> {
                if (existing != null && existing.matches(integration)) {
                    return existing;
                }
                try {
                    return compile(integration);
                } catch (AuthenticationException e) {
                    throw new CachedMappingBuildException(e);
                }
            });
        } catch (CachedMappingBuildException e) {
            throw e.getAuthenticationException();
        }
        if (!cachedMapping.hasRules()) {
            return Collections.emptySet();
        }

        UnifiedRoleMappingCelEngine.EvaluationContext context = UnifiedRoleMappingCelEngine.fromPrincipal(principal);
        return cachedMapping.evaluate(context);
    }

    private static CachedMapping compile(AuthenticationIntegration integration) throws AuthenticationException {
        try {
            TreeMap<String, RuleDefinition> definitions = new TreeMap<>();
            for (Map.Entry<String, String> entry : integration.getProperties().entrySet()) {
                String key = entry.getKey();
                if (!key.startsWith(RULE_PREFIX)) {
                    continue;
                }
                String suffix = key.substring(RULE_PREFIX.length());
                if (suffix.endsWith(CONDITION_SUFFIX)) {
                    String ruleId = suffix.substring(0, suffix.length() - CONDITION_SUFFIX.length());
                    definitions.computeIfAbsent(ruleId, ignored -> new RuleDefinition())
                            .condition = requireNonBlank(entry.getValue(), key);
                    continue;
                }
                if (suffix.endsWith(ROLES_SUFFIX)) {
                    String ruleId = suffix.substring(0, suffix.length() - ROLES_SUFFIX.length());
                    definitions.computeIfAbsent(ruleId, ignored -> new RuleDefinition())
                            .grantedRoles = parseGrantedRoles(entry.getValue(), key);
                }
            }
            if (definitions.isEmpty()) {
                return CachedMapping.noRules(integration);
            }

            List<UnifiedRoleMappingCelEngine.Rule> rules = new ArrayList<>(definitions.size());
            for (Map.Entry<String, RuleDefinition> entry : definitions.entrySet()) {
                String ruleId = entry.getKey();
                RuleDefinition definition = entry.getValue();
                if (definition.condition == null) {
                    throw new IllegalArgumentException("Missing condition for role mapping rule '" + ruleId + "'");
                }
                if (definition.grantedRoles == null || definition.grantedRoles.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Missing granted roles for role mapping rule '" + ruleId + "'");
                }
                rules.add(UnifiedRoleMappingCelEngine.Rule.of(definition.condition, definition.grantedRoles));
            }
            return CachedMapping.withRules(integration, new UnifiedRoleMappingCelEngine(rules));
        } catch (IllegalArgumentException e) {
            throw new AuthenticationException(
                    "Invalid role mapping configuration for integration '" + integration.getName() + "': "
                            + e.getMessage(),
                    e,
                    AuthenticationFailureType.MISCONFIGURED
            );
        }
    }

    private static String requireNonBlank(String value, String key) {
        String normalized = value == null ? "" : value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Property '" + key + "' must not be empty");
        }
        return normalized;
    }

    private static Set<String> parseGrantedRoles(String value, String key) {
        String normalized = requireNonBlank(value, key);
        LinkedHashSet<String> grantedRoles = new LinkedHashSet<>();
        for (String token : normalized.split(",")) {
            String roleName = token.trim();
            if (!roleName.isEmpty()) {
                grantedRoles.add(roleName);
            }
        }
        if (grantedRoles.isEmpty()) {
            throw new IllegalArgumentException("Property '" + key + "' must declare at least one role");
        }
        return Collections.unmodifiableSet(grantedRoles);
    }



    private static final class CachedMapping {
        private final Map<String, String> properties;
        private final UnifiedRoleMappingCelEngine engine;

        private CachedMapping(Map<String, String> properties, UnifiedRoleMappingCelEngine engine) {
            this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
            this.engine = engine;
        }

        private static CachedMapping noRules(AuthenticationIntegration integration) {
            return new CachedMapping(integration.getProperties(), null);
        }

        private static CachedMapping withRules(
                AuthenticationIntegration integration, UnifiedRoleMappingCelEngine engine) {
            return new CachedMapping(integration.getProperties(), engine);
        }

        private boolean matches(AuthenticationIntegration integration) {
            return properties.equals(integration.getProperties());
        }

        private boolean hasRules() {
            return engine != null;
        }

        private Set<String> evaluate(UnifiedRoleMappingCelEngine.EvaluationContext context) {
            return engine.evaluate(context);
        }
    }

    private static final class CachedMappingBuildException extends RuntimeException {
        private final AuthenticationException authenticationException;

        private CachedMappingBuildException(AuthenticationException authenticationException) {
            super(authenticationException);
            this.authenticationException = authenticationException;
        }

        private AuthenticationException getAuthenticationException() {
            return authenticationException;
        }
    }

    private static final class RuleDefinition {
        private String condition;
        private Set<String> grantedRoles;
    }
}
