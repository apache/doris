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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Evaluates role-mapping definitions resolved from FE metadata.
 */
public final class DefinitionBackedRoleMappingEvaluator implements RoleMappingEvaluator {
    private final RoleMappingDefinitionProvider provider;
    private final ConcurrentMap<String, CachedMapping> cache = new ConcurrentHashMap<>();

    public DefinitionBackedRoleMappingEvaluator(RoleMappingDefinitionProvider provider) {
        this.provider = Objects.requireNonNull(provider, "provider");
    }

    @Override
    public Set<String> evaluate(AuthenticationIntegration integration, Principal principal)
            throws AuthenticationException {
        Objects.requireNonNull(integration, "integration");
        Objects.requireNonNull(principal, "principal");

        RoleMappingDefinition definition = provider.getRoleMappingByIntegration(integration.getName());
        if (definition == null) {
            cache.remove(integration.getName());
            return Collections.emptySet();
        }

        CachedMapping cached;
        try {
            cached = cache.compute(integration.getName(), (ignored, existing) -> {
                if (existing != null && existing.matches(definition)) {
                    return existing;
                }
                try {
                    return compile(definition);
                } catch (AuthenticationException e) {
                    throw new CachedMappingBuildException(e);
                }
            });
        } catch (CachedMappingBuildException e) {
            throw e.getAuthenticationException();
        }
        return cached.evaluate(UnifiedRoleMappingCelEngine.fromPrincipal(principal));
    }

    private static CachedMapping compile(RoleMappingDefinition definition) throws AuthenticationException {
        try {
            return new CachedMapping(definition, new UnifiedRoleMappingCelEngine(definition.toEngineRules()));
        } catch (IllegalArgumentException e) {
            throw new AuthenticationException(
                    "Invalid role mapping '" + definition.getName() + "': " + e.getMessage(),
                    e,
                    AuthenticationFailureType.MISCONFIGURED);
        }
    }

    private static final class CachedMapping {
        private final RoleMappingDefinition definition;
        private final UnifiedRoleMappingCelEngine engine;

        private CachedMapping(RoleMappingDefinition definition, UnifiedRoleMappingCelEngine engine) {
            this.definition = definition;
            this.engine = engine;
        }

        private boolean matches(RoleMappingDefinition candidate) {
            return definition.equals(candidate);
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
}
