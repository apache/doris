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

package org.apache.doris.catalog.authorizer.ranger;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

class CompactedPolicyEngineTest {
    private static final class CompactedPlugin extends LoadedRangerPlugin {
        private CompactedPlugin(String type) {
            super(configuration(type));
        }
    }

    @Test
    void hiveQueriesAndDenyPoliciesAgreeWithTheOriginalEngine() throws Exception {
        RangerBasePlugin original = new RangerBasePlugin(configuration("hive"));
        CompactedPlugin compacted = new CompactedPlugin("hive");
        try {
            RangerPolicy allowed = policy(1, "alice", "select", resources("db", "t", "c1", "alice"));
            allowed.setAdditionalResources(List.of(resources("other", "other_table", "c2", "alice")));
            RangerPolicy denied = policy(2, "alice", "select", resources("db", "t", "secret"));
            denied.setDenyPolicyItems(denied.getPolicyItems());
            denied.setPolicyItems(Collections.emptyList());
            ServicePolicies snapshot = hive(1, allowed, denied);
            original.setPolicies(copy(snapshot));
            compacted.setPolicies(copy(snapshot));
            for (String user : Arrays.asList("alice", "bob")) {
                for (String db : Arrays.asList("db", "other")) {
                    for (String table : Arrays.asList("t", "other_table")) {
                        for (String column : Arrays.asList("c1", "c2", "secret", "alice", "unknown")) {
                            compare(original, compacted, user, db, table, column);
                        }
                    }
                }
            }
        } finally {
            original.cleanup();
            compacted.cleanup();
        }
    }

    @Test
    void exceptionsGroupsRolesAndExcludedResourcesKeepTheNativeDecisions() throws Exception {
        RangerBasePlugin original = new RangerBasePlugin(configuration("hive"));
        CompactedPlugin compacted = new CompactedPlugin("hive");
        try {
            RangerPolicy exceptedAllow = policy(1, "alice", "select", resources("db", "t", "c1"));
            exceptedAllow.getPolicyItems().get(0).setUsers(List.of("alice", "bob"));
            exceptedAllow.setAllowExceptions(List.of(item("alice")));
            original.setPolicies(copy(hive(1, exceptedAllow)));
            compacted.setPolicies(copy(hive(1, exceptedAllow)));
            compareExpected(original, compacted, request("alice", "db", "t", "c1"), false);
            compareExpected(original, compacted, request("bob", "db", "t", "c1"), true);

            RangerPolicy exceptedDeny = policy(2, "alice", "select", resources("db", "t", "c1"));
            exceptedDeny.getPolicyItems().get(0).setUsers(List.of("alice", "bob"));
            exceptedDeny.setDenyPolicyItems(List.of(item("alice", "bob")));
            exceptedDeny.setDenyExceptions(List.of(item("alice")));
            original.setPolicies(copy(hive(2, exceptedDeny)));
            compacted.setPolicies(copy(hive(2, exceptedDeny)));
            compareExpected(original, compacted, request("alice", "db", "t", "c1"), true);
            compareExpected(original, compacted, request("bob", "db", "t", "c1"), false);

            RangerPolicy group = policy(3, "alice", "select", resources("db", "t", "c1"));
            group.getPolicyItems().get(0).setUsers(Collections.emptyList());
            group.getPolicyItems().get(0).setGroups(List.of("readers"));
            original.setPolicies(copy(hive(3, group)));
            compacted.setPolicies(copy(hive(3, group)));
            RangerAccessRequest member = request("alice", "db", "t", "c1");
            ((RangerAccessRequestImpl) member).setUserGroups(Set.of("readers"));
            compareExpected(original, compacted, member, true);
            compareExpected(original, compacted, request("alice", "db", "t", "c1"), false);

            RangerRole role = new RangerRole();
            role.setName("read_role");
            role.setUsers(List.of(new RangerRole.RoleMember("alice", false)));
            RangerRoles roles = new RangerRoles();
            roles.setServiceName("test");
            roles.setRoleVersion(1L);
            roles.setRangerRoles(Set.of(role));
            original.setRoles(roles);
            compacted.setRoles(roles);
            RangerPolicy rolePolicy = policy(4, "alice", "select", resources("db", "t", "c1"));
            rolePolicy.getPolicyItems().get(0).setUsers(Collections.emptyList());
            rolePolicy.getPolicyItems().get(0).setRoles(List.of("read_role"));
            original.setPolicies(copy(hive(4, rolePolicy)));
            compacted.setPolicies(copy(hive(4, rolePolicy)));
            compareExpected(original, compacted, request("alice", "db", "t", "c1"), true);
            compareExpected(original, compacted, request("bob", "db", "t", "c1"), false);

            RangerPolicy excluded = policy(5, "alice", "select", resources("db", "t", "secret"));
            excluded.getResources().get("column").setIsExcludes(true);
            excluded.getResources().get("column").setIsRecursive(true);
            original.setPolicies(copy(hive(5, excluded)));
            compacted.setPolicies(copy(hive(5, excluded)));
            compareExpected(original, compacted, request("alice", "db", "t", "c1"), true);
            compareExpected(original, compacted, request("alice", "db", "t", "secret"), false);
        } finally {
            original.cleanup();
            compacted.cleanup();
        }
    }

    @Test
    void zoneAndTagRepositoriesCanBuildFromCompactedResources() throws Exception {
        RangerBasePlugin original = new RangerBasePlugin(configuration("hive"));
        CompactedPlugin compacted = new CompactedPlugin("hive");
        try {
            ServicePolicies snapshot = hive(1);
            ServicePolicies.SecurityZoneInfo zone = new ServicePolicies.SecurityZoneInfo();
            zone.setZoneName("zone");
            zone.setResources(List.of(new HashMap<>(Map.of("database", List.of("db"), "table", List.of("*"),
                    "column", List.of("*")))));
            RangerPolicy zonePolicy = policy(42, "alice", "select", resources("db", "t", "c1"));
            zonePolicy.setZoneName("zone");
            zone.setPolicies(List.of(zonePolicy));
            zone.setContainsAssociatedTagService(false);
            snapshot.setSecurityZones(Map.of("zone", zone));
            ServicePolicies.TagPolicies tags = new ServicePolicies.TagPolicies();
            tags.setServiceName("tag-service");
            tags.setServiceId(2L);
            tags.setPolicyVersion(1L);
            tags.setServiceDef(EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef("tag"));
            RangerPolicy tagPolicy = policy(43, "alice", "hive:select", Map.of("tag", new RangerPolicyResource("PII")));
            tagPolicy.setService("tag-service");
            tagPolicy.setServiceType("tag");
            tags.setPolicies(List.of(tagPolicy));
            snapshot.setTagPolicies(tags);
            original.setPolicies(copy(snapshot));
            compacted.setPolicies(copy(snapshot));
            Assertions.assertEquals(1, compacted.getPoliciesVersion());
            compare(original, compacted, "alice", "db", "t", "c1");
            compare(original, compacted, "bob", "db", "t", "c1");
        } finally {
            original.cleanup();
            compacted.cleanup();
        }
    }

    @Test
    void repeatedFullAndDeltaRefreshesRevokeOldColumns() throws Exception {
        RangerBasePlugin original = new RangerBasePlugin(configuration("hive"));
        CompactedPlugin compacted = new CompactedPlugin("hive");
        try {
            ServicePolicies initial = hive(1, policy(1, "alice", "select", resources("db", "t", "c1")));
            original.setPolicies(copy(initial));
            compacted.setPolicies(copy(initial));
            compare(original, compacted, "alice", "db", "t", "c1");
            Assertions.assertEquals(Long.valueOf(1), compacted.isAccessAllowed(
                    request("alice", "db", "t", "c1")).getPolicyVersion());
            for (int version = 2; version <= 12; version++) {
                String column = version % 2 == 0 ? "c2" : "c1";
                RangerPolicy replacement = policy(1, "alice", "select", resources("db", "t", column));
                replacement.setVersion((long) version);
                ServicePolicies update = hive(version, replacement);
                if (version % 3 != 0) {
                    RangerPolicyDelta delta = new RangerPolicyDelta();
                    delta.setId((long) version);
                    delta.setChangeType(RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE);
                    delta.setPolicy(replacement);
                    update.setPolicies(null);
                    update.setPolicyDeltas(List.of(delta));
                }
                original.setPolicies(copy(update));
                compacted.setPolicies(copy(update));
                Assertions.assertEquals(version, compacted.getPoliciesVersion());
                compare(original, compacted, "alice", "db", "t", "c1");
                compare(original, compacted, "alice", "db", "t", "c2");
                RangerAccessResult allowed = compacted.isAccessAllowed(request("alice", "db", "t", column));
                Assertions.assertTrue(allowed.getIsAllowed());
                Assertions.assertEquals(Long.valueOf(version), allowed.getPolicyVersion());
                String revoked = "c1".equals(column) ? "c2" : "c1";
                Assertions.assertFalse(compacted.isAccessAllowed(request("alice", "db", "t", revoked)).getIsAllowed());
            }
        } finally {
            original.cleanup();
            compacted.cleanup();
        }
    }

    private static void compare(RangerBasePlugin original, RangerBasePlugin compacted,
            String user, String db, String table, String column) {
        compareResults(original.isAccessAllowed(request(user, db, table, column)),
                compacted.isAccessAllowed(request(user, db, table, column)));
    }

    private static void compareExpected(RangerBasePlugin original, RangerBasePlugin compacted,
            RangerAccessRequest request, boolean allowed) {
        RangerAccessResult expected = original.isAccessAllowed(request);
        Assertions.assertEquals(allowed, expected.getIsAllowed());
        compareResults(expected, compacted.isAccessAllowed(request));
    }

    private static void compareResults(RangerAccessResult expected, RangerAccessResult actual) {
        Assertions.assertNotNull(expected);
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(expected.getIsAllowed(), actual.getIsAllowed());
        Assertions.assertEquals(expected.getPolicyId(), actual.getPolicyId());
        Assertions.assertEquals(expected.getPolicyVersion(), actual.getPolicyVersion());
        Assertions.assertEquals(expected.getPolicyPriority(), actual.getPolicyPriority());
        Assertions.assertEquals(expected.getIsAudited(), actual.getIsAudited());
        Assertions.assertEquals(expected.getAuditPolicyId(), actual.getAuditPolicyId());
    }

    private static RangerPolicyItem item(String... users) {
        RangerPolicyItem result = new RangerPolicyItem();
        result.setUsers(Arrays.asList(users));
        result.setAccesses(List.of(new RangerPolicyItemAccess("select", true)));
        return result;
    }

    private static ServicePolicies copy(ServicePolicies source) {
        return JsonUtils.jsonToObject(JsonUtils.objectToJson(source), ServicePolicies.class);
    }

    private static RangerAccessRequest request(String user, String db, String table, String column) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue("database", db);
        resource.setValue("table", table);
        resource.setValue("column", column);
        return new RangerAccessRequestImpl(resource, "select", user, Collections.emptySet(), Collections.emptySet());
    }

    private static RangerPluginConfig configuration(String type) {
        Properties properties = new Properties();
        properties.setProperty("ranger.plugin." + type + ".use.rangerGroups", "false");
        properties.setProperty("ranger.plugin." + type + ".policyengine.option.disable.context.enrichers", "true");
        return new RangerPluginConfig(type, "test", "test", properties);
    }

    private static ServicePolicies hive(long version, RangerPolicy... policies) throws Exception {
        ServicePolicies result = new ServicePolicies();
        result.setServiceName("test");
        result.setServiceId(1L);
        result.setPolicyVersion(version);
        result.setServiceDef(EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef("hive"));
        result.setPolicies(new ArrayList<>(Arrays.asList(policies)));
        return result;
    }

    private static RangerPolicy policy(long id, String user, String access, Map<String, RangerPolicyResource> resource) {
        RangerPolicy result = new RangerPolicy();
        result.setId(id);
        result.setVersion(1L);
        result.setService("test");
        result.setServiceType("hive");
        result.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        result.setIsAuditEnabled(true);
        result.setResources(resource);
        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(List.of(user));
        item.setAccesses(List.of(new RangerPolicyItemAccess(access, true)));
        result.setPolicyItems(List.of(item));
        return result;
    }

    private static Map<String, RangerPolicyResource> resources(String database, String table, String... columns) {
        Map<String, RangerPolicyResource> result = new HashMap<>();
        result.put("database", new RangerPolicyResource(database));
        result.put("table", new RangerPolicyResource(table));
        result.put("column", new RangerPolicyResource(new ArrayList<>(Arrays.asList(columns)), false, false));
        return result;
    }
}
