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

package org.apache.doris.authentication;

import org.apache.doris.authentication.handler.AuthenticationOutcome;
import org.apache.doris.authentication.handler.AuthenticationPluginManager;
import org.apache.doris.authentication.rolemapping.DefinitionBackedRoleMappingEvaluator;
import org.apache.doris.authentication.rolemapping.RoleMappingDefinition;
import org.apache.doris.authentication.rolemapping.RoleMappingEvaluator;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.ClassLoaderUtils;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime manager for AUTHENTICATION INTEGRATION.
 *
 * <p>Runtime state is intentionally lazier than metadata state:
 * <ul>
 *   <li>CREATE/ALTER only update metadata and mark runtime state dirty when needed.</li>
 *   <li>The next authentication request reloads the plugin from the latest metadata before authenticating.</li>
 *   <li>Replay/restart only clear caches and states; plugin instances are recreated on first real use.</li>
 * </ul>
 */
public class AuthenticationIntegrationRuntime {
    private static final Logger LOG = LogManager.getLogger(AuthenticationIntegrationRuntime.class);

    private static final class ResolvedAuthenticationPlugin {
        private final AuthenticationIntegration integration;
        private final AuthenticationPlugin plugin;

        private ResolvedAuthenticationPlugin(AuthenticationIntegration integration, AuthenticationPlugin plugin) {
            this.integration = integration;
            this.plugin = plugin;
        }
    }

    public enum RuntimeState {
        AVAILABLE,
        BROKEN
    }

    public static final class PreparedAuthenticationIntegration implements Closeable {
        private final AuthenticationIntegration integration;
        private final AuthenticationPlugin plugin;

        private PreparedAuthenticationIntegration(AuthenticationIntegration integration, AuthenticationPlugin plugin) {
            this.integration = Objects.requireNonNull(integration, "integration");
            this.plugin = Objects.requireNonNull(plugin, "plugin");
        }

        public AuthenticationIntegration getIntegration() {
            return integration;
        }

        public AuthenticationPlugin getPlugin() {
            return plugin;
        }

        @Override
        public void close() throws IOException {
            plugin.close();
        }
    }

    private final AuthenticationPluginManager pluginManager;
    private final RoleMappingEvaluator roleMappingEvaluator;
    private final Map<String, RuntimeState> runtimeStates = new ConcurrentHashMap<>();
    private final Map<String, String> brokenReasons = new ConcurrentHashMap<>();
    private final Set<String> dirtyIntegrations = ConcurrentHashMap.newKeySet();

    public AuthenticationIntegrationRuntime() {
        this(new AuthenticationPluginManager(), createDefaultRoleMappingEvaluator());
    }

    public AuthenticationIntegrationRuntime(AuthenticationPluginManager pluginManager) {
        this(pluginManager, createDefaultRoleMappingEvaluator());
    }

    AuthenticationIntegrationRuntime(AuthenticationPluginManager pluginManager,
            RoleMappingEvaluator roleMappingEvaluator) {
        this.pluginManager = Objects.requireNonNull(pluginManager, "pluginManager");
        this.roleMappingEvaluator = Objects.requireNonNull(roleMappingEvaluator, "roleMappingEvaluator");
    }

    public PreparedAuthenticationIntegration prepareAuthenticationIntegration(AuthenticationIntegrationMeta meta)
            throws AuthenticationException {
        AuthenticationIntegration integration = toIntegration(meta);
        ensurePluginFactoryLoaded(integration.getType());
        AuthenticationPlugin plugin = pluginManager.createPlugin(integration);
        return new PreparedAuthenticationIntegration(integration, plugin);
    }

    public void activatePreparedAuthenticationIntegration(PreparedAuthenticationIntegration prepared) {
        pluginManager.installPlugin(prepared.getIntegration(), prepared.getPlugin());
        runtimeStates.put(prepared.getIntegration().getName(), RuntimeState.AVAILABLE);
        brokenReasons.remove(prepared.getIntegration().getName());
        dirtyIntegrations.remove(prepared.getIntegration().getName());
    }

    public void discardPreparedAuthenticationIntegration(PreparedAuthenticationIntegration prepared) {
        if (prepared == null) {
            return;
        }
        try {
            prepared.close();
        } catch (IOException ignored) {
            // AuthenticationPlugin.close() does not throw. This is only to satisfy Closeable.
        }
    }

    public void removeAuthenticationIntegration(String integrationName) {
        pluginManager.removePlugin(integrationName);
        runtimeStates.remove(integrationName);
        brokenReasons.remove(integrationName);
        dirtyIntegrations.remove(integrationName);
    }

    public void markAuthenticationIntegrationDirty(String integrationName) {
        dirtyIntegrations.add(integrationName);
        runtimeStates.remove(integrationName);
        brokenReasons.remove(integrationName);
    }

    public void replayUpsertAuthenticationIntegration(AuthenticationIntegrationMeta meta) {
        pluginManager.removePlugin(meta.getName());
        runtimeStates.remove(meta.getName());
        brokenReasons.remove(meta.getName());
        dirtyIntegrations.remove(meta.getName());
    }

    public void rebuildAuthenticationIntegrations(Map<String, AuthenticationIntegrationMeta> snapshot) {
        pluginManager.clearCache();
        runtimeStates.clear();
        brokenReasons.clear();
        dirtyIntegrations.clear();
    }

    public AuthenticationOutcome authenticate(List<AuthenticationIntegrationMeta> chain, AuthenticationRequest request)
            throws AuthenticationException {
        Objects.requireNonNull(chain, "chain");
        Objects.requireNonNull(request, "request");
        if (chain.isEmpty()) {
            throw new AuthenticationException(
                    "authentication chain is empty",
                    AuthenticationFailureType.MISCONFIGURED);
        }

        AuthenticationOutcome lastFailure = null;
        boolean anySupported = false;
        for (AuthenticationIntegrationMeta meta : chain) {
            ResolvedAuthenticationPlugin resolved;
            try {
                resolved = resolvePluginForAuthentication(meta);
            } catch (AuthenticationException e) {
                AuthenticationIntegration currentIntegration =
                        toIntegration(resolveCurrentAuthenticationIntegration(meta));
                markBroken(currentIntegration.getName(), e);
                AuthenticationResult result = AuthenticationResult.failure(e);
                AuthenticationOutcome outcome = AuthenticationOutcome.of(currentIntegration, result);
                lastFailure = outcome;
                if (!shouldContinueInChain(result)) {
                    return outcome;
                }
                continue;
            }
            AuthenticationIntegration integration = resolved.integration;
            AuthenticationPlugin plugin = resolved.plugin;
            if (!plugin.supports(request)) {
                continue;
            }
            anySupported = true;

            AuthenticationOutcome outcome;
            try {
                outcome = toOutcome(integration, plugin.authenticate(request, integration));
            } catch (AuthenticationException e) {
                outcome = AuthenticationOutcome.of(integration, AuthenticationResult.failure(e));
            }
            if (!outcome.isFailure()) {
                return outcome;
            }
            lastFailure = outcome;
            if (!shouldContinueInChain(outcome.getAuthResult())) {
                return outcome;
            }
        }

        if (lastFailure != null) {
            return lastFailure;
        }
        if (!anySupported) {
            throw new AuthenticationException(
                    "No authentication integration supports request for user: " + request.getUsername(),
                    AuthenticationFailureType.MISCONFIGURED);
        }
        throw new AuthenticationException(
                "Authentication failed for user: " + request.getUsername(),
                AuthenticationFailureType.ACCESS_DENIED);
    }

    public RuntimeState getRuntimeState(String integrationName) {
        return runtimeStates.get(integrationName);
    }

    public String getBrokenReason(String integrationName) {
        return brokenReasons.get(integrationName);
    }

    private AuthenticationOutcome toOutcome(AuthenticationIntegration integration, AuthenticationResult result)
            throws AuthenticationException {
        Objects.requireNonNull(integration, "integration");
        Objects.requireNonNull(result, "result");
        if (!result.isSuccess()) {
            return AuthenticationOutcome.of(integration, result);
        }

        Principal principal = Objects.requireNonNull(result.getPrincipal(), "principal is required for success");
        Set<String> mappedRoles = roleMappingEvaluator.evaluate(integration, principal);
        if (mappedRoles.isEmpty()) {
            return AuthenticationOutcome.of(integration, result);
        }

        LinkedHashSet<String> grantedRoles = new LinkedHashSet<>(result.getGrantedRoles());
        grantedRoles.addAll(mappedRoles);
        return AuthenticationOutcome.of(integration, AuthenticationResult.success(principal, grantedRoles));
    }

    private ResolvedAuthenticationPlugin resolvePluginForAuthentication(AuthenticationIntegrationMeta requestedMeta)
            throws AuthenticationException {
        AuthenticationIntegrationMeta currentMeta = resolveCurrentAuthenticationIntegration(requestedMeta);
        String integrationName = currentMeta.getName();
        if (dirtyIntegrations.contains(integrationName)) {
            // DDL updated metadata without eager init. Refresh the cached plugin from the latest metadata before
            // serving the first request after that ALTER.
            currentMeta = resolveCurrentAuthenticationIntegration(requestedMeta);
            AuthenticationIntegration refreshedIntegration = toIntegration(currentMeta);
            ensurePluginFactoryLoaded(refreshedIntegration.getType());
            pluginManager.reloadPlugin(refreshedIntegration);
            dirtyIntegrations.remove(integrationName);
        }
        AuthenticationIntegration integration = toIntegration(currentMeta);
        ensurePluginFactoryLoaded(integration.getType());
        AuthenticationPlugin plugin = pluginManager.getPlugin(integration);
        runtimeStates.put(integrationName, RuntimeState.AVAILABLE);
        brokenReasons.remove(integrationName);
        return new ResolvedAuthenticationPlugin(integration, plugin);
    }

    private AuthenticationIntegrationMeta resolveCurrentAuthenticationIntegration(AuthenticationIntegrationMeta meta) {
        Env env = Env.getCurrentEnv();
        if (env == null || env.getAuthenticationIntegrationMgr() == null) {
            return meta;
        }
        AuthenticationIntegrationMeta current = env.getAuthenticationIntegrationMgr().getAuthenticationIntegration(
                meta.getName());
        return current != null ? current : meta;
    }

    private void ensurePluginFactoryLoaded(String pluginType) throws AuthenticationException {
        if (pluginManager.hasFactory(pluginType)) {
            return;
        }

        try {
            pluginManager.loadAll(
                    ClassLoaderUtils.parsePluginRootDirectories(Config.authentication_plugins_dir),
                    getClass().getClassLoader());
        } catch (AuthenticationException e) {
            throw new AuthenticationException(
                    "Failed to load authentication plugins for type '" + pluginType + "': " + e.getMessage(),
                    e,
                    AuthenticationFailureType.MISCONFIGURED);
        }

        if (!pluginManager.hasFactory(pluginType)) {
            throw new AuthenticationException(
                    "No authentication plugin factory found for type: " + pluginType,
                    AuthenticationFailureType.MISCONFIGURED);
        }
    }

    private void markBroken(String integrationName, AuthenticationException exception) {
        runtimeStates.put(integrationName, RuntimeState.BROKEN);
        brokenReasons.put(integrationName, Strings.nullToEmpty(exception.getMessage()));
        LOG.warn("Authentication integration '{}' is broken: {}", integrationName, exception.getMessage(), exception);
    }

    private static boolean shouldContinueInChain(AuthenticationResult result) {
        if (!result.isFailure()) {
            return false;
        }
        AuthenticationException exception = result.getException();
        return exception != null && exception.getFailureType().shouldContinueInChain();
    }

    private static RoleMappingEvaluator createDefaultRoleMappingEvaluator() {
        return new DefinitionBackedRoleMappingEvaluator(AuthenticationIntegrationRuntime::resolveRoleMappingDefinition);
    }

    private static RoleMappingDefinition resolveRoleMappingDefinition(String integrationName) {
        Env env = Env.getCurrentEnv();
        if (env == null || env.getRoleMappingMgr() == null) {
            return null;
        }
        RoleMappingMeta meta = env.getRoleMappingMgr().getRoleMappingByIntegration(integrationName);
        return meta == null ? null : meta.toDefinition();
    }

    private static AuthenticationIntegration toIntegration(AuthenticationIntegrationMeta meta) {
        return AuthenticationIntegration.builder()
                .name(meta.getName())
                .type(meta.getType())
                .properties(meta.getProperties())
                .comment(meta.getComment())
                .build();
    }
}
