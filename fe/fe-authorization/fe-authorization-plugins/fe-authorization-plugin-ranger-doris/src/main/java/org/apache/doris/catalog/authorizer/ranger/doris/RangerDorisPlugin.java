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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.doris.catalog.authorizer.ranger.LoadedRangerPlugin;
import org.apache.doris.catalog.authorizer.ranger.RangerUserStoreGroups;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Map;

/**
 * The plugin over a Ranger service of type {@code doris}: built with the service's policies or not at all, so
 * that an FE whose instance scope it governs starts with them or does not start; see
 * {@link LoadedRangerPlugin}.
 */
public class RangerDorisPlugin extends LoadedRangerPlugin {
    private static final Logger LOG = LogManager.getLogger(RangerDorisPlugin.class);
    private volatile ReadScopes readScopes;

    private static final class ReadScopes {
        private final RangerPolicyEngine engine;
        private final Map<String, String> config;
        private final RangerReadScopeSummary summary;
        private final RangerReadResourceIndex resources;
        private final long version;

        private ReadScopes(RangerPolicyEngine engine, Map<String, String> config, RangerReadScopeSummary summary,
                RangerReadResourceIndex resources) {
            this.engine = engine;
            this.config = config;
            this.summary = summary;
            this.resources = resources;
            this.version = engine.getPolicyVersion();
        }
    }

    // Starts no refresher: the caller installs snapshots directly, as an embedded-engine test does.
    RangerDorisPlugin(RangerPluginConfig config) {
        super(config);
    }

    public RangerDorisPlugin(String serviceName) {
        this(serviceName, null);
    }

    public RangerDorisPlugin(String serviceName, RangerAuthContextListener rangerAuthContextListener) {
        super(serviceName, null, null);
        // Registered before the load, so that the listener hears of the engine the load installs.
        registerAuthContextEventListener(rangerAuthContextListener);
        init();
    }

    @Override
    public synchronized void setPolicies(ServicePolicies policies) {
        ReadScopes previous = readScopes;
        RangerPolicyEngine oldEngine = getPolicyEngine();
        long oldVersion = getPoliciesVersion();
        boolean synchronousRefresh = Boolean.parseBoolean(getServiceConfigs().get(
                getConfig().getPropertyPrefix() + ".policy.refresh.synchronous"));
        readScopes = null;
        super.setPolicies(policies);
        if (synchronousRefresh || policies == null || policies.getPolicyVersion() == null
                || getPoliciesVersion() != policies.getPolicyVersion()) {
            return;
        }
        RangerPolicyEngine installedEngine = getPolicyEngine();
        if (installedEngine == oldEngine && getPoliciesVersion() == oldVersion) {
            // Ranger catches a failed installation and can keep an engine with the payload's version.
            // Compile only after a verifiable installation, including an in-place delta version change.
            return;
        }
        if (!RangerReadResourceIndex.isEnabled(getConfig())) {
            return;
        }
        Map<String, String> installedConfig = getServiceConfigs();
        RangerReadScopeSummary summary = RangerReadScopeSummary.build(policies, getConfig(), installedConfig,
                previous == null ? null : previous.summary);
        if (summary != null) {
            RangerReadResourceIndex resources = RangerReadResourceIndex.build(
                    installedEngine, getConfig(), installedConfig);
            readScopes = new ReadScopes(installedEngine, installedConfig, summary, resources);
            if (resources != null) {
                LOG.info("Ranger read resource index initialized: version={}, tables={}, users={}, bindings={},"
                                + " nativeUsers={}",
                        getPoliciesVersion(), resources.tableCount(), resources.userCount(), resources.bindingCount(),
                        resources.nativeUserCount());
            }
        }
    }

    @Override
    public void cleanup() {
        readScopes = null;
        try {
            // Do not hold the policy-install monitor while Ranger joins its refresher thread.
            super.cleanup();
        } finally {
            // A refresh already in flight may have published a summary while cleanup stopped it.
            readScopes = null;
        }
    }

    Boolean indexedReadPrivilege(String user, DorisAccessType action, RangerDorisResource resource) {
        if (action != DorisAccessType.SELECT && action != DorisAccessType.ADMIN) {
            return null;
        }
        ReadScopes current = readScopesOf(user);
        if (current == null) {
            return null;
        }
        if (!current.summary.mayGrant(user, resource)) {
            return false;
        }
        if (current.resources == null) {
            return null;
        }
        return current.resources.allows(user, action, resource);
    }

    private ReadScopes readScopesOf(String user) {
        ReadScopes current = readScopes;
        if (current == null || getResultProcessor() != null || current.engine != getPolicyEngine()
                || current.version != getPoliciesVersion() || current.config != getServiceConfigs()) {
            return null;
        }
        if (getConfig().isSuperUser(user)
                || getConfig().hasSuperGroup(RangerUserStoreGroups.groupsOf(this, user))) {
            return null;
        }
        return current;
    }
}
