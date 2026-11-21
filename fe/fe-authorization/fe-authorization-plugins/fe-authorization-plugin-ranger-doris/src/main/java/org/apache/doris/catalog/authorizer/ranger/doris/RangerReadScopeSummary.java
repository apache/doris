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

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.contextenricher.RangerUserStoreEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An over-approximation of the scopes where a static, direct-user ADMIN or SELECT policy could grant access.
 * It never grants anything: it only lets the Doris controller omit an impossible SELF check before
 * it asks Ranger about the table's columns. Visibility questions and column decisions still reach Ranger.
 */
final class RangerReadScopeSummary {
    private static final List<String> HIERARCHY = Arrays.asList("catalog", "database", "table", "column");
    private static final int GLOBAL = 1 << 4;
    private static final int MAX_USER_BINDINGS = 65_536;

    private final Map<Long, Map<String, Integer>> policies;
    private final Map<String, Integer> users;
    private final String definition;

    private RangerReadScopeSummary(Map<Long, Map<String, Integer>> policies, Map<String, Integer> users,
            String definition) {
        this.policies = ImmutableMap.copyOf(policies);
        this.users = ImmutableMap.copyOf(users);
        this.definition = definition;
    }

    static RangerReadScopeSummary build(ServicePolicies downloaded, RangerPluginConfig config,
            Map<String, String> effectiveConfig, RangerReadScopeSummary previous) {
        if (!supportedService(downloaded, config, effectiveConfig)) {
            return null;
        }
        Map<Long, Map<String, Integer>> policies = new HashMap<>();
        String definition = JsonUtils.objectToJson(downloaded.getServiceDef());
        if (definition == null) {
            return null;
        }
        Map<String, RangerResourceDef> resources = new HashMap<>();
        for (RangerResourceDef resource : downloaded.getServiceDef().getResources()) {
            resources.put(resource.getName(), resource);
        }
        if (downloaded.getPolicyDeltas() != null) {
            if (previous == null || !previous.definition.equals(definition)) {
                return null;
            }
            policies.putAll(previous.policies);
            for (RangerPolicyDelta delta : downloaded.getPolicyDeltas()) {
                if (delta.getPolicy() == null || delta.getPolicy().getId() == null || delta.getChangeType() == null) {
                    return null;
                }
                if (!StringUtils.equals(delta.getServiceType(), downloaded.getServiceDef().getName())) {
                    return null;
                }
                if (delta.getChangeType() == RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE) {
                    policies.remove(delta.getPolicy().getId());
                    continue;
                }
                if (delta.getChangeType() != RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE
                        && delta.getChangeType() != RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE) {
                    return null;
                }
                if (!addPolicy(policies, delta.getPolicy(), resources)) {
                    return null;
                }
            }
        } else {
            if (downloaded.getPolicies() == null) {
                return null;
            }
            for (RangerPolicy policy : downloaded.getPolicies()) {
                if (policies.containsKey(policy.getId())) {
                    return null;
                }
                if (!addPolicy(policies, policy, resources)) {
                    return null;
                }
            }
        }
        Map<String, Integer> users = new HashMap<>();
        int bindings = 0;
        for (Map<String, Integer> policy : policies.values()) {
            bindings += policy.size();
            if (bindings > MAX_USER_BINDINGS) {
                return null;
            }
            policy.forEach((user, scopes) -> users.merge(user, scopes, (first, second) -> first | second));
        }
        return new RangerReadScopeSummary(policies, users, definition);
    }

    boolean mayGrant(String user, RangerDorisResource resource) {
        int scope = scopeOf(resource);
        if (scope == 0) {
            return true;
        }
        return (users.getOrDefault(user, 0) & scope) != 0;
    }

    private static int scopeOf(RangerDorisResource resource) {
        if (resource.getValue("global") != null) {
            return GLOBAL;
        }
        if (resource.getValue("column") != null || resource.getValue("catalog") == null) {
            return 0;
        }
        if (resource.getValue("table") != null) {
            return 1 << 2;
        }
        if (resource.getValue("database") != null) {
            return 1 << 1;
        }
        return 1;
    }

    private static boolean supportedService(ServicePolicies policies, RangerPluginConfig config,
            Map<String, String> effectiveConfig) {
        if (policies == null || policies.getServiceDef() == null || policies.getTagPolicies() != null
                || (policies.getSecurityZones() != null && !policies.getSecurityZones().isEmpty())) {
            return false;
        }
        String prefix = config.getPropertyPrefix();
        if (config.getBoolean(prefix + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_NAME_TRANSFORMATION, false)
                || config.isConvertEmailToUsername()
                || StringUtils.isNotBlank(config.get(prefix + ".super.users"))
                || StringUtils.isNotBlank(config.get(prefix + ".super.groups"))
                || StringUtils.isNotBlank(config.get(prefix + ".service.admins"))
                || StringUtils.isNotBlank(config.get(prefix + ".chained.services"))
                || config.getBoolean(prefix + ".policy.refresh.synchronous", false)
                || Boolean.parseBoolean(effectiveConfig.get(prefix + ".policy.refresh.synchronous"))) {
            return false;
        }
        for (String key : Arrays.asList(RangerPolicyEngine.PLUGIN_SUPER_USERS, RangerPolicyEngine.PLUGIN_SUPER_GROUPS,
                RangerPolicyEngine.PLUGIN_SERVICE_ADMINS, RangerPolicyEngine.PLUGIN_AUDIT_FILTER)) {
            if (StringUtils.isNotBlank(effectiveConfig.get(key))) {
                return false;
            }
        }
        RangerServiceDef definition = policies.getServiceDef();
        for (RangerServiceDef.RangerAccessTypeDef access : definition.getAccessTypes()) {
            if (access.getImpliedGrants() != null && !access.getImpliedGrants().isEmpty()) {
                return false;
            }
        }
        for (RangerResourceDef resource : definition.getResources()) {
            if (!RangerDefaultResourceMatcher.class.getName().equals(resource.getMatcher())) {
                return false;
            }
            for (String option : resource.getMatcherOptions().keySet()) {
                if (!"wildCard".equals(option) && !"ignoreCase".equals(option)) {
                    return false;
                }
            }
        }
        Map<String, RangerResourceDef> hierarchy = new HashMap<>();
        for (RangerResourceDef resource : definition.getResources()) {
            hierarchy.put(resource.getName(), resource);
        }
        int previousLevel = Integer.MIN_VALUE;
        for (int level = 0; level < HIERARCHY.size(); level++) {
            RangerResourceDef resource = hierarchy.get(HIERARCHY.get(level));
            if (resource == null || resource.getLevel() == null || resource.getLevel() <= previousLevel) {
                return false;
            }
            String expectedParent = level == 0 ? null : HIERARCHY.get(level - 1);
            if (!StringUtils.equals(StringUtils.defaultIfBlank(resource.getParent(), null), expectedParent)) {
                return false;
            }
            previousLevel = resource.getLevel();
        }
        if (definition.getContextEnrichers() != null) {
            for (RangerServiceDef.RangerContextEnricherDef enricher : definition.getContextEnrichers()) {
                if (!RangerUserStoreEnricher.class.getName().equals(enricher.getEnricher())) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean addPolicy(Map<Long, Map<String, Integer>> policies, RangerPolicy policy,
            Map<String, RangerResourceDef> definitions) {
        if (policy.getId() == null) {
            return false;
        }
        if (!Boolean.TRUE.equals(policy.getIsEnabled())) {
            policies.put(policy.getId(), Collections.emptyMap());
            return true;
        }
        if ((policy.getPolicyType() != null && policy.getPolicyType() != RangerPolicy.POLICY_TYPE_ACCESS)
                || !policy.getDenyPolicyItems().isEmpty() || !policy.getAllowExceptions().isEmpty()
                || !policy.getDenyExceptions().isEmpty() || Boolean.TRUE.equals(policy.getIsDenyAllElse())
                || (policy.getConditions() != null && !policy.getConditions().isEmpty())
                || !policy.getValiditySchedules().isEmpty() || StringUtils.isNotBlank(policy.getZoneName())) {
            return false;
        }
        Map<String, Integer> bindings = new HashMap<>();
        boolean read = false;
        for (RangerPolicyItem item : policy.getPolicyItems()) {
            if (!item.getConditions().isEmpty()) {
                return false;
            }
            for (RangerPolicyItemAccess access : item.getAccesses()) {
                if (("SELECT".equalsIgnoreCase(access.getType()) || "ADMIN".equalsIgnoreCase(access.getType()))
                        && !Boolean.FALSE.equals(access.getIsAllowed())) {
                    read = true;
                }
            }
        }
        if (!read) {
            policies.put(policy.getId(), Collections.emptyMap());
            return true;
        }
        int scopes = resourceScopes(policy.getResources(), definitions);
        if (scopes < 0) {
            return false;
        }
        if (policy.getAdditionalResources() != null) {
            for (Map<String, RangerPolicyResource> resource : policy.getAdditionalResources()) {
                int additionalScopes = resourceScopes(resource, definitions);
                if (additionalScopes < 0) {
                    return false;
                }
                scopes |= additionalScopes;
            }
        }
        for (RangerPolicyItem item : policy.getPolicyItems()) {
            if (!item.getGroups().isEmpty() || !item.getRoles().isEmpty()) {
                return false;
            }
            for (String user : item.getUsers()) {
                if (user.indexOf('{') >= 0 || user.indexOf('}') >= 0) {
                    return false;
                }
                bindings.put(user, scopes);
                if (bindings.size() > MAX_USER_BINDINGS) {
                    return false;
                }
            }
        }
        policies.put(policy.getId(), ImmutableMap.copyOf(bindings));
        return true;
    }

    private static int resourceScopes(Map<String, RangerPolicyResource> resource,
            Map<String, RangerResourceDef> definitions) {
        if (resource == null || resource.isEmpty()) {
            return -1;
        }
        if (resource.size() == 1 && resource.containsKey("global")) {
            RangerPolicyResource global = resource.get("global");
            if (global == null || global.getIsExcludes() || global.getIsRecursive()) {
                return -1;
            }
            for (String value : global.getValues()) {
                if (value == null || value.indexOf('{') >= 0 || value.indexOf('}') >= 0) {
                    return -1;
                }
            }
            return GLOBAL;
        }
        int scopes = 1 << 3;
        for (String key : resource.keySet()) {
            if (!HIERARCHY.contains(key) || !definitions.containsKey(key)) {
                return -1;
            }
        }
        boolean descendantsMatchAny = true;
        for (int level = HIERARCHY.size() - 1; level >= 0; level--) {
            if (descendantsMatchAny) {
                scopes |= 1 << level;
            }
            RangerPolicyResource value = resource.get(HIERARCHY.get(level));
            if (value == null) {
                continue;
            }
            if (value.getIsExcludes() || value.getIsRecursive()) {
                return -1;
            }
            boolean exact = !value.getValues().isEmpty();
            for (String text : value.getValues()) {
                if (text == null || text.indexOf('{') >= 0 || text.indexOf('}') >= 0) {
                    return -1;
                }
                if (text.isEmpty() || text.indexOf('*') >= 0 || text.indexOf('?') >= 0) {
                    exact = false;
                }
            }
            if (exact) {
                descendantsMatchAny = false;
                continue;
            }
            RangerDefaultResourceMatcher matcher = new RangerDefaultResourceMatcher();
            matcher.setResourceDef(definitions.get(HIERARCHY.get(level)));
            matcher.setPolicyResource(value);
            matcher.init();
            if (matcher.getNeedsDynamicEval()) {
                return -1;
            }
            descendantsMatchAny &= matcher.isMatchAny();
        }
        return scopes;
    }
}
