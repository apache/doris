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

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class RangerReadScopeSummaryTest {
    private static final AuthorizationContext CONTEXT = new AuthorizationContext() {
        @Override
        public Set<String> rolesOf(AuthorizedSubject subject) {
            return Collections.emptySet();
        }

        @Override
        public boolean grantedByGlobalScopeAuthority(AuthorizedSubject subject, AccessRequirement requirement) {
            return false;
        }
    };
    private static final AuthorizedSubject ALICE = AuthorizedSubject.of("alice", "%");

    private static final class CountingPlugin extends RangerDorisPlugin {
        private final AtomicInteger requests = new AtomicInteger();

        private CountingPlugin() {
            this(config());
        }

        private CountingPlugin(RangerPluginConfig configuration) {
            super(configuration);
        }

        private RangerPolicyEngine engine() {
            return getPolicyEngine();
        }

        @Override
        public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
            requests.incrementAndGet();
            return super.isAccessAllowed(request);
        }
    }

    private static final class StockPlugin extends RangerBasePlugin {
        private StockPlugin(RangerPluginConfig configuration) {
            super(configuration);
        }

        private RangerPolicyEngine engine() {
            return getPolicyEngine();
        }
    }

    @Test
    void parentChecksAreSkippedButColumnsAndVisibilityStillReachRanger() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1", "alice"))));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            controller.checkPrivilege(ALICE, AuthorizedResource.columns("internal", "db", "t", Set.of("c1")),
                    AccessRequirements.SELECT, AccessContext.NONE);
            Assertions.assertEquals(2, plugin.requests.get(), "only the column ADMIN and SELECT should reach Ranger");
            int before = plugin.requests.get();
            controller.checkPrivilege(ALICE, AuthorizedResource.table("internal", "db", "t"),
                    AccessRequirements.VISIBILITY, AccessContext.NONE);
            Assertions.assertTrue(plugin.requests.get() > before, "visibility's descendant lookup must reach Ranger");
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void auditProcessorKeepsEveryOriginalCheckAndItsAuditPolicy() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        List<Long> audited = new ArrayList<>();
        try {
            plugin.setPolicies(snapshot(1, policy(7, "alice", "SELECT", resource("db", "t", "c1"))));
            plugin.setResultProcessor(new RangerAccessResultProcessor() {
                @Override
                public void processResult(RangerAccessResult result) {
                    audited.add(result.getAuditPolicyId());
                }

                @Override
                public void processResults(Collection<RangerAccessResult> results) {
                    results.forEach(this::processResult);
                }
            });
            new RangerDorisAccessController(plugin, CONTEXT).checkPrivilege(ALICE,
                    AuthorizedResource.columns("internal", "db", "t", Set.of("c1")),
                    AccessRequirements.SELECT, AccessContext.NONE);
            Assertions.assertEquals(10, plugin.requests.get());
            Assertions.assertEquals(10, audited.size());
            Assertions.assertEquals(7L, audited.get(audited.size() - 1));
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void wildcardEmptyAndExactColumnsAgreeWithTheUnprunedEngine() {
        for (List<String> columns : Arrays.asList(Collections.<String>emptyList(), List.of("*"), List.of("**"),
                List.of("*", "alice"), List.of("c?"), List.of("c*"), List.of("c1", "alice"))) {
            CountingPlugin plugin = new CountingPlugin();
            try {
                Map<String, RangerPolicyResource> resources = resource("db", "t");
                resources.put("column", new RangerPolicyResource(columns, false, false));
                ServicePolicies policies = snapshot(1, policy(1, "alice", "SELECT", resources));
                plugin.setPolicies(policies);
                RangerReadScopeSummary summary = RangerReadScopeSummary.build(policies, plugin.getConfig(),
                        plugin.getServiceConfigs(), null);
                Assertions.assertNotNull(summary);
                for (DorisObjectType kind : Arrays.asList(DorisObjectType.CATALOG, DorisObjectType.DATABASE,
                        DorisObjectType.TABLE)) {
                    RangerDorisResource resource = scoped(kind);
                    RangerAccessResult original = plugin.isAccessAllowed(request("alice", "SELECT", resource));
                    if (!summary.mayGrant("alice", resource)) {
                        Assertions.assertFalse(original.getIsAllowed(), "unsafe pruning for " + columns + " / " + kind);
                    }
                }
            } finally {
                plugin.cleanup();
            }
        }
    }

    @Test
    void parentAndAdminGrantsRemainAvailableAndOrBlocksAreNotCrossed() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        try {
            RangerPolicy columns = policy(1, "alice", "SELECT", resource("db", "t", "c1"));
            columns.setAdditionalResources(List.of(resource("other", "other_table", "c2")));
            RangerPolicy admin = policy(2, "admin_user", "ADMIN", Map.of("global", new RangerPolicyResource("*")));
            plugin.setPolicies(snapshot(1, columns, admin));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            controller.checkPrivilege(AuthorizedSubject.of("admin_user", "%"),
                    AuthorizedResource.table("internal", "db", "anything"), AccessRequirements.SELECT,
                    AccessContext.NONE);
            Assertions.assertThrows(org.apache.doris.authorization.AccessDeniedException.class,
                    () -> controller.checkPrivilege(ALICE,
                            AuthorizedResource.columns("internal", "db", "other_table", Set.of("c2")),
                            AccessRequirements.SELECT, AccessContext.NONE));
            plugin.setPolicies(snapshot(2, policy(1, "alice", "SELECT", resource("db", "t", "*"))));
            controller.checkPrivilege(ALICE, AuthorizedResource.table("internal", "db", "t"),
                    AccessRequirements.SELECT, AccessContext.NONE);
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void unsupportedSnapshotsFallBackAsAWhole() {
        RangerPolicy deny = policy(1, "alice", "SELECT", resource("db", "t", "c1"));
        deny.setDenyPolicyItems(deny.getPolicyItems());
        RangerPolicy group = policy(2, "alice", "SELECT", resource("db", "t", "c1"));
        group.getPolicyItems().get(0).setGroups(List.of("readers"));
        RangerPolicy role = policy(3, "alice", "SELECT", resource("db", "t", "c1"));
        role.getPolicyItems().get(0).setRoles(List.of("readers"));
        RangerPolicy excluded = policy(4, "alice", "SELECT", resource("db", "t", "*"));
        excluded.getResources().get("column").setIsExcludes(true);
        RangerPolicy macro = policy(5, "alice", "SELECT", resource("db", "t", "{USER}"));
        for (RangerPolicy policy : Arrays.asList(deny, group, role, excluded, macro)) {
            Assertions.assertNull(RangerReadScopeSummary.build(snapshot(1, policy), config(), Map.of(), null));
        }
        ServicePolicies tagged = snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1")));
        tagged.setTagPolicies(new ServicePolicies.TagPolicies());
        Assertions.assertNull(RangerReadScopeSummary.build(tagged, config(), Map.of(), null));
        ServicePolicies plain = snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1")));
        Assertions.assertNull(RangerReadScopeSummary.build(plain, config(),
                Map.of(RangerPolicyEngine.PLUGIN_SUPER_USERS, "alice"), null));
        RangerPluginConfig transformed = config();
        transformed.set("ranger.plugin.doris.supports.name.transformation", "true");
        Assertions.assertNull(RangerReadScopeSummary.build(plain, transformed, Map.of(), null));
    }

    @Test
    void conditionsValidityExceptionsAndServiceOverridesDisableTheWholeSummary() {
        RangerPolicy conditioned = policy(1, "alice", "SELECT", resource("db", "t", "c1"));
        conditioned.getPolicyItems().get(0).setConditions(List.of(
                new RangerPolicyItemCondition("ip-range", List.of("10.0.0.0/8"))));
        RangerPolicy scheduled = policy(2, "alice", "SELECT", resource("db", "t", "c1"));
        scheduled.setValiditySchedules(List.of(new RangerValiditySchedule()));
        RangerPolicy denyAllElse = policy(3, "alice", "SELECT", resource("db", "t", "c1"));
        denyAllElse.setIsDenyAllElse(true);
        RangerPolicy allowException = policy(4, "alice", "SELECT", resource("db", "t", "c1"));
        allowException.setAllowExceptions(allowException.getPolicyItems());
        RangerPolicy denyException = policy(5, "alice", "SELECT", resource("db", "t", "c1"));
        denyException.setDenyExceptions(denyException.getPolicyItems());
        for (RangerPolicy complex : Arrays.asList(conditioned, scheduled, denyAllElse,
                allowException, denyException)) {
            CountingPlugin plugin = new CountingPlugin();
            try {
                // A separate simple policy must not leave a partially compiled authorization path.
                plugin.setPolicies(snapshot(1, policy(9, "bob", "SELECT", resource("db", "t", "c1")), complex));
                Assertions.assertTrue(plugin.mayGrantReadPrivilege("bob", scoped(DorisObjectType.TABLE)));
                Assertions.assertNull(RangerReadScopeSummary.build(snapshot(1, complex), config(), Map.of(), null));
            } finally {
                plugin.cleanup();
            }
        }
        ServicePolicies plain = snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1")));
        Assertions.assertNull(RangerReadScopeSummary.build(plain, config(),
                Map.of(RangerPolicyEngine.PLUGIN_SUPER_GROUPS, "administrators"), null));
        Assertions.assertNull(RangerReadScopeSummary.build(plain, config(),
                Map.of("ranger.plugin.doris.policy.refresh.synchronous", "true"), null));
        RangerPolicy disabled = policy(1, "alice", "SELECT", resource("db", "t", "*"));
        disabled.setIsEnabled(false);
        RangerReadScopeSummary summary = RangerReadScopeSummary.build(snapshot(1, disabled), config(), Map.of(), null);
        Assertions.assertNotNull(summary);
        Assertions.assertFalse(summary.mayGrant("alice", scoped(DorisObjectType.TABLE)));
    }

    @Test
    void deltaAddsAndRevokesParentPermissionsWithoutKeepingStaleScopeBits() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
            ServicePolicies added = snapshot(2);
            added.setPolicies(null);
            added.setPolicyDeltas(List.of(delta(RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE,
                    policy(2, "alice", "SELECT", resource("db", "t", "*")))));
            plugin.setPolicies(added);
            Assertions.assertEquals(2, plugin.getPoliciesVersion());
            Assertions.assertTrue(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
            ServicePolicies removed = snapshot(3);
            removed.setPolicies(null);
            removed.setPolicyDeltas(List.of(delta(RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE,
                    policy(2, "alice", "SELECT", resource("db", "t", "*")))));
            plugin.setPolicies(removed);
            Assertions.assertEquals(3, plugin.getPoliciesVersion());
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void inPlaceDeltasFallBackWhenNativeServiceVersionDoesNotAdvance() throws Exception {
        RangerPluginConfig configuration = config();
        RangerPluginConfig nativeConfiguration = config();
        for (RangerPluginConfig configured : Arrays.asList(configuration, nativeConfiguration)) {
            configured.set("ranger.plugin.doris.supports.policy.deltas", "true");
            configured.set("ranger.plugin.doris.supports.in.place.policy.updates", "true");
        }
        CountingPlugin plugin = new CountingPlugin(configuration);
        StockPlugin stock = new StockPlugin(nativeConfiguration);
        Field summary = RangerDorisPlugin.class.getDeclaredField("readScopes");
        summary.setAccessible(true);
        try {
            ServicePolicies initial = snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1")));
            stock.setPolicies(JsonUtils.jsonToObject(JsonUtils.objectToJson(initial), ServicePolicies.class));
            plugin.setPolicies(initial);
            RangerPolicyEngine engine = plugin.engine();
            RangerPolicyEngine nativeEngine = stock.engine();
            RangerDorisResource table = scoped(DorisObjectType.TABLE);
            RangerDorisResource column = new RangerDorisResource(DorisObjectType.COLUMN, "internal", "db", "t", "c1");
            Assertions.assertNotNull(engine);
            Assertions.assertNotNull(nativeEngine);
            Assertions.assertEquals(1, plugin.getPoliciesVersion());
            Assertions.assertEquals(1, stock.getPoliciesVersion());
            Assertions.assertNotNull(summary.get(plugin));
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", table));
            Assertions.assertFalse(plugin.isAccessAllowed(request("alice", "SELECT", table)).getIsAllowed());
            Assertions.assertFalse(stock.isAccessAllowed(request("alice", "SELECT", table)).getIsAllowed());
            Assertions.assertTrue(plugin.isAccessAllowed(request("alice", "SELECT", column)).getIsAllowed());
            Assertions.assertTrue(stock.isAccessAllowed(request("alice", "SELECT", column)).getIsAllowed());

            ServicePolicies added = snapshot(2);
            added.setPolicies(null);
            RangerPolicy parent = policy(2, "alice", "SELECT", resource("db", "t", "*"));
            parent.setVersion(2L);
            added.setPolicyDeltas(List.of(delta(RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE,
                    parent)));
            stock.setPolicies(JsonUtils.jsonToObject(JsonUtils.objectToJson(added), ServicePolicies.class));
            plugin.setPolicies(added);
            Assertions.assertSame(engine, plugin.engine(), "both flags enable native in-place policy updates");
            Assertions.assertSame(nativeEngine, stock.engine());
            // Ranger 2.9 reinitializes its repository in place without advancing the service version.
            Assertions.assertEquals(1, plugin.getPoliciesVersion());
            Assertions.assertEquals(1, stock.getPoliciesVersion());
            Assertions.assertNull(summary.get(plugin), "an unverified service version must disable pruning");
            Assertions.assertTrue(plugin.mayGrantReadPrivilege("alice", table));
            RangerAccessResult granted = plugin.isAccessAllowed(request("alice", "SELECT", table));
            RangerAccessResult nativeGrant = stock.isAccessAllowed(request("alice", "SELECT", table));
            Assertions.assertTrue(granted.getIsAllowed());
            Assertions.assertTrue(nativeGrant.getIsAllowed());
            Assertions.assertEquals(Long.valueOf(2), granted.getPolicyVersion());
            Assertions.assertEquals(nativeGrant.getPolicyVersion(), granted.getPolicyVersion());
            Assertions.assertTrue(plugin.isAccessAllowed(request("alice", "SELECT", column)).getIsAllowed());
            Assertions.assertTrue(stock.isAccessAllowed(request("alice", "SELECT", column)).getIsAllowed());

            ServicePolicies removed = snapshot(3);
            removed.setPolicies(null);
            removed.setPolicyDeltas(List.of(delta(RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE,
                    parent)));
            stock.setPolicies(JsonUtils.jsonToObject(JsonUtils.objectToJson(removed), ServicePolicies.class));
            plugin.setPolicies(removed);
            Assertions.assertSame(engine, plugin.engine());
            Assertions.assertSame(nativeEngine, stock.engine());
            Assertions.assertEquals(1, plugin.getPoliciesVersion());
            Assertions.assertEquals(1, stock.getPoliciesVersion());
            Assertions.assertNull(summary.get(plugin));
            Assertions.assertTrue(plugin.mayGrantReadPrivilege("alice", table), "fallback delegates the refusal");
            Assertions.assertFalse(plugin.isAccessAllowed(request("alice", "SELECT", table)).getIsAllowed());
            Assertions.assertFalse(stock.isAccessAllowed(request("alice", "SELECT", table)).getIsAllowed());
            Assertions.assertTrue(plugin.isAccessAllowed(request("alice", "SELECT", column)).getIsAllowed());
            Assertions.assertTrue(stock.isAccessAllowed(request("alice", "SELECT", column)).getIsAllowed());
        } finally {
            plugin.cleanup();
            stock.cleanup();
        }
    }

    @Test
    void changedServiceDefinitionOrCustomMatcherOptionsDisableDeltaReuse() {
        ServicePolicies initial = snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "*")));
        initial.getServiceDef().getResources().get(3).setMatcherOptions(Map.of("wildCard", "false"));
        RangerReadScopeSummary old = RangerReadScopeSummary.build(initial, config(), Map.of(), null);
        Assertions.assertNotNull(old);
        Assertions.assertFalse(old.mayGrant("alice", scoped(DorisObjectType.TABLE)));
        ServicePolicies update = snapshot(2);
        update.setPolicies(null);
        update.setPolicyDeltas(List.of(delta(RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE,
                policy(2, "other_user", "SELECT", resource("db", "other", "c")))));
        Assertions.assertNull(RangerReadScopeSummary.build(update, config(), Map.of(), old),
                "unchanged policies must be recompiled when wildcard semantics change");
        ServicePolicies custom = snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c")));
        custom.getServiceDef().getResources().get(3).setMatcherOptions(Map.of("tokenDelimiterStart", "$"));
        Assertions.assertNull(RangerReadScopeSummary.build(custom, config(), Map.of(), null));
        ServicePolicies reparented = snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c")));
        reparented.getServiceDef().getResources().get(3).setParent("database");
        Assertions.assertNull(RangerReadScopeSummary.build(reparented, config(), Map.of(), null));
    }

    @Test
    void runtimeSuperUserOverrideStillReachesTheOriginalEngine() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
            plugin.setSuperUsersAndGroups(Set.of("alice"), Set.of());
            Assertions.assertTrue(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
            new RangerDorisAccessController(plugin, CONTEXT).checkPrivilege(ALICE,
                    AuthorizedResource.table("internal", "ungranted", "ungranted"), AccessRequirements.SELECT,
                    AccessContext.NONE);
            Assertions.assertTrue(plugin.requests.get() > 0);
            plugin.setSuperUsersAndGroups(Set.of(), Set.of());
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void concurrentSnapshotReplacementKeepsColumnChecksAndFinalRevocationCorrect() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        ExecutorService readers = Executors.newFixedThreadPool(3);
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            List<Future<?>> futures = new ArrayList<>();
            CountDownLatch ready = new CountDownLatch(3);
            CountDownLatch start = new CountDownLatch(1);
            for (int reader = 0; reader < 3; reader++) {
                futures.add(readers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(error);
                    }
                    for (int check = 0; check < 50; check++) {
                        try {
                            controller.checkPrivilege(ALICE,
                                    AuthorizedResource.columns("internal", "db", "t", Set.of("c1")),
                                    AccessRequirements.SELECT, AccessContext.NONE);
                            Assertions.assertThrows(org.apache.doris.authorization.AccessDeniedException.class,
                                    () -> controller.checkPrivilege(ALICE,
                                            AuthorizedResource.columns("internal", "db", "t", Set.of("never_granted")),
                                            AccessRequirements.SELECT, AccessContext.NONE));
                        } catch (Exception error) {
                            throw new AssertionError(error);
                        }
                    }
                }));
            }
            ready.await();
            start.countDown();
            for (int version = 2; version <= 10; version++) {
                plugin.setPolicies(snapshot(version,
                        policy(1, "alice", "SELECT", resource("db", "t", version % 2 == 0
                                ? new String[] {"c1", "c2"} : new String[] {"c1"}))));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            plugin.setPolicies(snapshot(11, policy(1, "alice", "SELECT", resource("db", "t", "c2"))));
            for (int check = 0; check < 50; check++) {
                Assertions.assertThrows(org.apache.doris.authorization.AccessDeniedException.class,
                        () -> controller.checkPrivilege(ALICE,
                                AuthorizedResource.columns("internal", "db", "t", Set.of("c1")),
                                AccessRequirements.SELECT, AccessContext.NONE));
            }
        } finally {
            readers.shutdownNow();
            plugin.cleanup();
        }
    }

    @Test
    void failedInstallWithTheCurrentVersionDisablesTheSummary() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            RangerPolicyEngine original = plugin.engine();
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
            ServicePolicies invalid = snapshot(1, policy(2, "alice", "SELECT", resource("db", "t", "*")), null);
            Assertions.assertDoesNotThrow(() -> plugin.setPolicies(invalid));
            Assertions.assertSame(original, plugin.engine());
            Assertions.assertTrue(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)),
                    "a failed installation must fall back even when its version equals the active version");
            Assertions.assertTrue(plugin.isAccessAllowed(request("alice", "SELECT",
                    new RangerDorisResource(DorisObjectType.COLUMN, "internal", "db", "t", "c1"))).getIsAllowed());
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void cleanupDropsTheSummaryEngineReferenceAndPermitsASeparateInstallation() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        Field summary = RangerDorisPlugin.class.getDeclaredField("readScopes");
        summary.setAccessible(true);
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            Assertions.assertNotNull(summary.get(plugin));
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));

            plugin.cleanup();
            Assertions.assertNull(plugin.engine());
            Assertions.assertNull(summary.get(plugin), "a retained closed plugin must not retain the old engine");
            Assertions.assertTrue(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));

            plugin.setPolicies(snapshot(2, policy(2, "alice", "SELECT", resource("db", "t", "c2"))));
            Assertions.assertEquals(2, plugin.getPoliciesVersion());
            Assertions.assertNotNull(summary.get(plugin));
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice", scoped(DorisObjectType.TABLE)));
            Assertions.assertTrue(plugin.isAccessAllowed(request("alice", "SELECT",
                    new RangerDorisResource(DorisObjectType.COLUMN, "internal", "db", "t", "c2"))).getIsAllowed());
            Assertions.assertFalse(plugin.isAccessAllowed(request("alice", "SELECT",
                    new RangerDorisResource(DorisObjectType.COLUMN, "internal", "db", "t", "c1"))).getIsAllowed());
        } finally {
            plugin.cleanup();
        }
        Assertions.assertNull(summary.get(plugin));
    }

    private static RangerDorisResource scoped(DorisObjectType type) {
        if (type == DorisObjectType.CATALOG) {
            return new RangerDorisResource(type, "internal");
        }
        if (type == DorisObjectType.DATABASE) {
            return new RangerDorisResource(type, "internal", "db");
        }
        return new RangerDorisResource(type, "internal", "db", "t");
    }

    private static RangerAccessRequest request(String user, String access, RangerAccessResourceImpl resource) {
        return new RangerAccessRequestImpl(resource, access, user, Collections.emptySet(), Collections.emptySet());
    }

    static RangerPluginConfig config() {
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty("ranger.plugin.doris.use.rangerGroups", "false");
        properties.setProperty("ranger.plugin.doris.policyengine.option.disable.context.enrichers", "true");
        return new RangerPluginConfig("doris", "scope-test", "scope-test", properties);
    }

    static ServicePolicies snapshot(long version, RangerPolicy... policies) {
        ServicePolicies result = new ServicePolicies();
        result.setServiceName("scope-test");
        result.setServiceId(1L);
        result.setPolicyVersion(version);
        result.setPolicies(new ArrayList<>(Arrays.asList(policies)));
        result.setServiceDef(definition());
        return result;
    }

    private static RangerPolicyDelta delta(int type, RangerPolicy policy) {
        RangerPolicyDelta result = new RangerPolicyDelta();
        result.setId(1L);
        result.setChangeType(type);
        result.setPolicy(policy);
        return result;
    }

    static RangerPolicy policy(long id, String user, String access, Map<String, RangerPolicyResource> resource) {
        RangerPolicy result = new RangerPolicy();
        result.setId(id);
        result.setVersion(1L);
        result.setName("p" + id);
        result.setService("scope-test");
        result.setServiceType("doris");
        result.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        result.setIsAuditEnabled(true);
        result.setResources(resource);
        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(List.of(user));
        item.setAccesses(List.of(new RangerPolicyItemAccess(access, true)));
        result.setPolicyItems(List.of(item));
        return result;
    }

    static Map<String, RangerPolicyResource> resource(String db, String table, String... columns) {
        Map<String, RangerPolicyResource> result = new HashMap<>();
        result.put("catalog", new RangerPolicyResource("internal"));
        result.put("database", new RangerPolicyResource(db));
        result.put("table", new RangerPolicyResource(table));
        result.put("column", new RangerPolicyResource(new ArrayList<>(Arrays.asList(columns)), false, false));
        return result;
    }

    private static RangerServiceDef definition() {
        RangerServiceDef result = new RangerServiceDef();
        result.setName("doris");
        List<RangerResourceDef> resources = new ArrayList<>();
        String[] hierarchy = {"catalog", "database", "table", "column"};
        for (int level = 0; level < hierarchy.length; level++) {
            RangerResourceDef resource = new RangerResourceDef();
            resource.setItemId(level + 1L);
            resource.setName(hierarchy[level]);
            resource.setType("string");
            resource.setLevel(level + 1);
            resource.setParent(level == 0 ? null : hierarchy[level - 1]);
            resource.setMatcher(RangerDefaultResourceMatcher.class.getName());
            resource.setMatcherOptions(Map.of("wildCard", "true", "ignoreCase", "true"));
            resources.add(resource);
        }
        RangerResourceDef global = new RangerResourceDef();
        global.setItemId(5L);
        global.setName("global");
        global.setType("string");
        global.setLevel(1);
        global.setMatcher(RangerDefaultResourceMatcher.class.getName());
        resources.add(global);
        result.setResources(resources);
        List<RangerServiceDef.RangerAccessTypeDef> accesses = new ArrayList<>();
        for (String name : Arrays.asList("ADMIN", "SELECT", "LOAD")) {
            RangerServiceDef.RangerAccessTypeDef access = new RangerServiceDef.RangerAccessTypeDef();
            access.setItemId((long) accesses.size() + 1);
            access.setName(name);
            accesses.add(access);
        }
        result.setAccessTypes(accesses);
        result.setOptions(Map.of("enableDenyAndExceptionsInPolicies", "true"));
        return result;
    }
}
