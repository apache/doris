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
import org.apache.doris.authorization.AccessDeniedException;
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
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class RangerReadResourceIndexCorrectnessTest {
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
    private static final List<String> USERS = List.of("alice", "Alice", "bob", "domain\\user", "员工");

    private static final class CountingPlugin extends RangerDorisPlugin {
        private final AtomicInteger requests = new AtomicInteger();

        private CountingPlugin() {
            this(config());
        }

        private CountingPlugin(RangerPluginConfig configuration) {
            super(configuration);
        }

        @Override
        public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
            requests.incrementAndGet();
            return super.isAccessAllowed(request);
        }

        private RangerPolicyEngine engine() {
            return getPolicyEngine();
        }
    }

    @Test
    void fullRefreshUsesNewNativeGrantsBeforePublishingTheNewIndex() throws Exception {
        assertRefreshGap(false);
    }

    @Test
    void copyOnUpdateDeltaUsesNewNativeGrantsBeforePublishingTheNewIndex() throws Exception {
        assertRefreshGap(true);
    }

    private static void assertRefreshGap(boolean delta) throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch installed = new CountDownLatch(1);
        CountDownLatch publish = new CountDownLatch(1);
        try {
            plugin.setPolicies(snapshot(1, grant("c1")));
            Assertions.assertTrue(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c1")));
            // Ranger invokes this after installing its engine, before the Doris override can build its index.
            plugin.registerAuthContextEventListener(() -> {
                if (plugin.getPoliciesVersion() != 2) {
                    return;
                }
                installed.countDown();
                await(publish);
            });
            ServicePolicies replacement = snapshot(2, grant("c2"));
            if (delta) {
                replacement = delta(2, grant("c2"));
            }
            ServicePolicies next = replacement;
            Future<?> refresh = executor.submit(() -> plugin.setPolicies(next));
            await(installed);
            Assertions.assertEquals(2, plugin.getPoliciesVersion());
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c1")));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("c1")));
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
            Assertions.assertTrue(plugin.requests.get() > 0, "queries in the publication gap must reach native Ranger");
            publish.countDown();
            refresh.get(15, TimeUnit.SECONDS);
            plugin.requests.set(0);
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("c1")));
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
            Assertions.assertEquals(0, plugin.requests.get(), "the completed replacement must use the new index");
        } finally {
            publish.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
            plugin.cleanup();
        }
    }

    @Test
    void closeCanFinishWhileRefreshIsPausedAndCannotRepublishGrants() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch installed = new CountDownLatch(1);
        CountDownLatch publish = new CountDownLatch(1);
        try {
            plugin.setPolicies(snapshot(1, grant("c1")));
            plugin.registerAuthContextEventListener(() -> {
                installed.countDown();
                await(publish);
            });
            Future<?> refresh = executor.submit(() -> plugin.setPolicies(snapshot(2, grant("c2"))));
            await(installed);
            // Closing must not wait for the policy-install monitor held by the paused refresher.
            Future<?> close = executor.submit(controller::close);
            close.get(15, TimeUnit.SECONDS);
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
            publish.countDown();
            refresh.get(15, TimeUnit.SECONDS);
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c2")));
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("c1", "c2")));
        } finally {
            publish.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
            plugin.cleanup();
        }
    }

    @Test
    void randomizedSnapshotsMatchAnIndependentNativeControllerAndFreshRawRequests() {
        Random random = new Random(22550);
        int indexedComparisons = 0;
        int allowedQueries = 0;
        int deniedQueries = 0;
        List<String> queryUsers = List.of("alice", "Alice", "bob", "domain\\user", "员工", "operator", "unknown");
        for (int version = 1; version <= 24; version++) {
            CountingPlugin optimized = new CountingPlugin();
            RangerBasePlugin nativePlugin = new RangerBasePlugin(config());
            try {
                ServicePolicies input = randomSnapshot(random, version);
                nativePlugin.setPolicies(copy(input));
                optimized.setPolicies(copy(input));
                Assertions.assertNotNull(optimized.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c1")),
                        "random static snapshots must exercise the index, not just native fallback");
                RangerDorisAccessController expected = new RangerDorisAccessController(nativePlugin, CONTEXT);
                RangerDorisAccessController actual = new RangerDorisAccessController(optimized, CONTEXT);
                for (int sample = 0; sample < 160; sample++) {
                    String user = choose(random, queryUsers);
                    String catalog = choose(random, List.of("internal", "INTERNAL", "external", "unknown"));
                    String database = choose(random, List.of("db", "DB", "analytics", "销售", "i", "unknown"));
                    String table = choose(random, List.of("t", "T", "orders", "other", "unknown"));
                    Set<String> columns = new LinkedHashSet<>();
                    columns.add(choose(random, List.of("c1", "C1", "c2", "prefix_value", "列", "unlisted")));
                    columns.add(choose(random, List.of("c1", "c2", "alice", "domain\\user", "unlisted")));
                    String diagnostic = "version=" + version + " sample=" + sample + " user=" + user
                            + " resource=" + catalog + "/" + database + "/" + table + "/" + columns;
                    boolean verdict = allowed(expected, user, catalog, database, table, columns);
                    Assertions.assertEquals(verdict, allowed(actual, user, catalog, database, table, columns),
                            () -> diagnostic + " policies=" + JsonUtils.objectToJson(input));
                    if (verdict) {
                        allowedQueries++;
                    } else {
                        deniedQueries++;
                    }
                    for (DorisAccessType action : List.of(DorisAccessType.ADMIN, DorisAccessType.SELECT)) {
                        String col = columns.iterator().next();
                        Boolean indexed = optimized.indexedReadPrivilege(user, action,
                                column(catalog, database, table, col));
                        if (indexed == null) {
                            continue;
                        }
                        // Native preprocessing never touches the resource passed to the indexed decision.
                        boolean nativeVerdict = nativePlugin.isAccessAllowed(new RangerAccessRequestImpl(
                                column(catalog, database, table, col), action.name(), user, Set.of(), Set.of()))
                                .getIsAllowed();
                        Assertions.assertEquals(Boolean.valueOf(nativeVerdict), indexed,
                                () -> diagnostic + " action=" + action + " policies=" + JsonUtils.objectToJson(input));
                        indexedComparisons++;
                    }
                }
            } finally {
                optimized.cleanup();
                nativePlugin.cleanup();
            }
        }
        Assertions.assertTrue(indexedComparisons > 2000);
        Assertions.assertTrue(allowedQueries > 0);
        Assertions.assertTrue(deniedQueries > 0);
    }

    @Test
    void unsupportedRefreshesDiscardPreviouslyIndexedGrants() {
        for (String variant : List.of("deny", "publicGroup", "impliedGrant", "excluded", "lowercase")) {
            CountingPlugin optimized = new CountingPlugin();
            RangerBasePlugin nativePlugin = new RangerBasePlugin(config());
            try {
                optimized.setPolicies(snapshot(1, grant("c1")));
                Assertions.assertTrue(optimized.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c1")));
                RangerPolicy replacement = grant("c2");
                ServicePolicies next = snapshot(2, replacement);
                if (variant.equals("deny")) {
                    replacement.setDenyPolicyItems(replacement.getPolicyItems());
                }
                if (variant.equals("publicGroup")) {
                    replacement.getPolicyItems().get(0).setUsers(List.of());
                    replacement.getPolicyItems().get(0).setGroups(List.of("public"));
                }
                if (variant.equals("impliedGrant")) {
                    replacement.getPolicyItems().get(0).setAccesses(List.of(new RangerPolicyItemAccess("LOAD", true)));
                    next.getServiceDef().getAccessTypes().get(2).setImpliedGrants(List.of("SELECT"));
                }
                if (variant.equals("excluded")) {
                    replacement.getResources().put("column", new RangerPolicyResource(List.of("c1"), true, false));
                }
                if (variant.equals("lowercase")) {
                    replacement.getPolicyItems().get(0).setAccesses(List.of(new RangerPolicyItemAccess("select", true)));
                }
                nativePlugin.setPolicies(copy(next));
                optimized.setPolicies(copy(next));
                Assertions.assertNull(optimized.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c1")),
                        variant);
                RangerDorisAccessController expected = new RangerDorisAccessController(nativePlugin, CONTEXT);
                RangerDorisAccessController actual = new RangerDorisAccessController(optimized, CONTEXT);
                for (String user : List.of("alice", "bob")) {
                    for (String col : List.of("c1", "c2", "unlisted")) {
                        Assertions.assertEquals(allowed(expected, user, "internal", "db", "t", Set.of(col)),
                                allowed(actual, user, "internal", "db", "t", Set.of(col)), variant + "/" + user + col);
                    }
                }
                Assertions.assertFalse(allowed(actual, "alice", "internal", "db", "t", Set.of("c1")), variant);
            } finally {
                optimized.cleanup();
                nativePlugin.cleanup();
            }
        }
    }

    @Test
    void randomizedDeltaSequencesMatchIndependentFullSnapshotReplacements() {
        Random random = new Random(22551);
        CountingPlugin optimized = new CountingPlugin();
        RangerBasePlugin nativePlugin = new RangerBasePlugin(config());
        try {
            RangerDorisAccessController expected = new RangerDorisAccessController(nativePlugin, CONTEXT);
            RangerDorisAccessController actual = new RangerDorisAccessController(optimized, CONTEXT);
            ServicePolicies previous = null;
            for (int version = 1; version <= 32; version++) {
                ServicePolicies next = randomSnapshot(random, version);
                // Deltas retain a stable service definition, while users, resources, actions and policy IDs change.
                next.setServiceDef(snapshot(version).getServiceDef());
                int omitted = version % 4;
                next.getPolicies().removeIf(policy -> policy.getId() != 100 && policy.getId() % 4 == omitted);
                for (RangerPolicy policy : next.getPolicies()) {
                    policy.setVersion((long) version);
                }
                nativePlugin.setPolicies(copy(next));
                if (previous == null || version % 4 == 0) {
                    optimized.setPolicies(copy(next));
                } else {
                    optimized.setPolicies(changes(previous, next));
                }
                Assertions.assertEquals(version, optimized.getPoliciesVersion());
                Assertions.assertNotNull(optimized.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c1")),
                        "eligible delta sequences must rebuild the index, not hide errors behind fallback");
                for (int sample = 0; sample < 80; sample++) {
                    String user = choose(random, List.of("alice", "Alice", "bob", "员工", "operator", "unknown"));
                    String catalog = choose(random, List.of("internal", "INTERNAL", "external"));
                    String database = choose(random, List.of("db", "DB", "analytics", "销售", "i"));
                    String table = choose(random, List.of("t", "T", "orders", "other", "unknown"));
                    Set<String> columns = Set.of(choose(random, List.of("c1", "C1", "c2", "prefix_value", "列")));
                    String diagnostic = "delta version=" + version + " sample=" + sample + " user=" + user
                            + " resource=" + catalog + "/" + database + "/" + table + "/" + columns;
                    Assertions.assertEquals(allowed(expected, user, catalog, database, table, columns),
                            allowed(actual, user, catalog, database, table, columns),
                            () -> diagnostic + " policies=" + JsonUtils.objectToJson(next));
                }
                previous = next;
            }
        } finally {
            optimized.cleanup();
            nativePlugin.cleanup();
        }
    }

    @Test
    void auditFallbackPreservesTheNativeResultSequenceAndCanBeRemoved() {
        CountingPlugin optimized = new CountingPlugin();
        RangerBasePlugin nativePlugin = new RangerBasePlugin(config());
        try {
            ServicePolicies policies = snapshot(1, grant("c1"));
            optimized.setPolicies(copy(policies));
            nativePlugin.setPolicies(copy(policies));
            List<String> actualAudits = new ArrayList<>();
            List<String> expectedAudits = new ArrayList<>();
            optimized.setResultProcessor(audits(actualAudits));
            nativePlugin.setResultProcessor(audits(expectedAudits));
            RangerDorisAccessController expected = new RangerDorisAccessController(nativePlugin, CONTEXT);
            RangerDorisAccessController actual = new RangerDorisAccessController(optimized, CONTEXT);
            for (String col : List.of("c1", "unlisted")) {
                Assertions.assertEquals(allowed(expected, "alice", "internal", "db", "t", Set.of(col)),
                        allowed(actual, "alice", "internal", "db", "t", Set.of(col)));
            }
            Assertions.assertFalse(actualAudits.isEmpty());
            Assertions.assertEquals(expectedAudits, actualAudits);
            Assertions.assertEquals(expectedAudits.size(), optimized.requests.get());
            optimized.setResultProcessor(null);
            optimized.requests.set(0);
            Assertions.assertTrue(allowed(actual, "alice", "internal", "db", "t", Set.of("c1")));
            Assertions.assertFalse(allowed(actual, "alice", "internal", "db", "t", Set.of("unlisted")));
            Assertions.assertEquals(0, optimized.requests.get());
        } finally {
            optimized.cleanup();
            nativePlugin.cleanup();
        }
    }

    @Test
    void inPlaceDeltaRevocationRemainsNative() {
        RangerPluginConfig configuration = config();
        configuration.set("ranger.plugin.doris.supports.policy.deltas", "true");
        configuration.set("ranger.plugin.doris.supports.in.place.policy.updates", "true");
        CountingPlugin plugin = new CountingPlugin(configuration);
        try {
            plugin.setPolicies(snapshot(1, grant("c1")));
            RangerPolicyEngine original = plugin.engine();
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c1")));
            plugin.setPolicies(delta(2, grant("c2")));
            Assertions.assertSame(original, plugin.engine(), "fixture must exercise a real in-place delta");
            // Ranger 2.9 keeps the repository's final version field in this mode; check the actual grants.
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("c1")));
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c2")));
            Assertions.assertTrue(plugin.requests.get() > 0);
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void bindingLimitFallsBackWithoutPublishingAPartialIndex() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, grant("old")));
            RangerPolicy many = grant("c2");
            List<String> users = new ArrayList<>();
            users.add("alice");
            for (int user = 1; user < 65_536; user++) {
                users.add("user_" + user);
            }
            many.getPolicyItems().get(0).setUsers(users);
            List<Map<String, RangerPolicyResource>> additional = new ArrayList<>();
            for (int table = 1; table < 62; table++) {
                additional.add(RangerReadScopeSummaryTest.resource("db", "t" + table, "c2"));
            }
            many.setAdditionalResources(additional);
            plugin.setPolicies(snapshot(2, many));
            // 65536 users * 62 blocks exceeds the index cap; the final block must still be authorized natively.
            Assertions.assertFalse(plugin.mayGrantReadPrivilege("alice",
                    new RangerDorisResource(DorisObjectType.GLOBAL, "*")),
                    "the eligible scope summary must survive, so this specifically tests the resource-index cap");
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("old")));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t61", Set.of("c2")));
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("old")));
            Assertions.assertTrue(plugin.requests.get() > 0);
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void failedInstallAtTheSameVersionCannotBuildAnIndexFromTheRejectedPayload() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, grant("c1")));
            RangerPolicyEngine original = plugin.engine();
            Assertions.assertDoesNotThrow(() -> plugin.setPolicies(snapshot(1, grant("c2"), null)));
            Assertions.assertSame(original, plugin.engine());
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c2")));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t", Set.of("c1")));
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void unicodeCaseFoldingMatchesNativeCandidateSelectionAsWellAsTheResourceMatcher() {
        // policy database, policy column, request database, request column
        for (List<String> sample : List.of(
                List.of("ſales", "c1", "sales", "c1"),
                List.of("ı", "c1", "i", "c1"),
                List.of("db", "ſecret", "db", "secret"),
                List.of("db", "id", "db", "ıd"),
                List.of("db", "i*", "db", "ıd"),
                List.of("db", "ſ*", "db", "secret"),
                List.of("销售", "列", "销售", "列"),
                List.of("db", "ſecret", "db", "ſecret"))) {
            CountingPlugin optimized = new CountingPlugin();
            RangerBasePlugin nativePlugin = new RangerBasePlugin(config());
            try {
                RangerPolicy policy = RangerReadScopeSummaryTest.policy(1, "alice", "SELECT",
                        RangerReadScopeSummaryTest.resource(sample.get(0), "t", sample.get(1)));
                ServicePolicies input = snapshot(1, policy);
                optimized.setPolicies(copy(input));
                nativePlugin.setPolicies(copy(input));
                RangerDorisAccessController expected = new RangerDorisAccessController(nativePlugin, CONTEXT);
                RangerDorisAccessController actual = new RangerDorisAccessController(optimized, CONTEXT);
                boolean nativeVerdict = allowed(expected, "alice", "internal", sample.get(2), "t", Set.of(sample.get(3)));
                Assertions.assertEquals(nativeVerdict,
                        allowed(actual, "alice", "internal", sample.get(2), "t", Set.of(sample.get(3))), sample.toString());
                if (sample.get(0).equals(sample.get(2)) && sample.get(1).equals(sample.get(3))) {
                    Assertions.assertTrue(nativeVerdict, "exact Unicode grants must remain usable through fallback");
                }
            } finally {
                optimized.cleanup();
                nativePlugin.cleanup();
            }
        }
    }

    @Test
    void unicodePolicyFallbackIsPerUserAndDisappearsWhenThePolicyIsRemoved() {
        assertPolicyFallbackIsPerUser("ſecret");
    }

    @Test
    void escapedPolicyFallbackIsPerUserAndDisappearsWhenThePolicyIsRemoved() {
        assertPolicyFallbackIsPerUser("se\\\\cret");
    }

    private static void assertPolicyFallbackIsPerUser(String unsafeColumn) {
        CountingPlugin plugin = new CountingPlugin();
        try {
            RangerPolicy unsafe = RangerReadScopeSummaryTest.policy(1, "alice", "SELECT",
                    RangerReadScopeSummaryTest.resource("db", "t", unsafeColumn, "c3"));
            unsafe.getPolicyItems().get(0).setUsers(List.of("alice", "carol"));
            RangerPolicy bob = RangerReadScopeSummaryTest.policy(2, "bob", "SELECT",
                    RangerReadScopeSummaryTest.resource("db", "t", "c1"));
            RangerPolicy ascii = RangerReadScopeSummaryTest.policy(3, "alice", "SELECT",
                    RangerReadScopeSummaryTest.resource("db", "t", "c2"));
            ServicePolicies initial = snapshot(1, unsafe, bob, ascii);
            plugin.setPolicies(copy(initial));
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c2")));
            Assertions.assertNull(plugin.indexedReadPrivilege("carol", DorisAccessType.SELECT, column("c3")),
                    "a native-only principal may have no entries in the indexed-user dictionary");
            Assertions.assertTrue(plugin.indexedReadPrivilege("bob", DorisAccessType.SELECT, column("c1")));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            plugin.requests.set(0);
            Assertions.assertTrue(allowed(controller, "bob", "internal", "db", "t", Set.of("c1")));
            Assertions.assertFalse(allowed(controller, "bob", "internal", "db", "t", Set.of("secret")));
            Assertions.assertEquals(0, plugin.requests.get(), "unaffected users must retain the index");
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("secret")));
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
            Assertions.assertTrue(allowed(controller, "carol", "internal", "db", "t", Set.of("c3")));
            Assertions.assertTrue(plugin.requests.get() > 0);
            ServicePolicies removed = snapshot(2, bob, ascii);
            plugin.setPolicies(copy(removed));
            plugin.requests.set(0);
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
            Assertions.assertFalse(allowed(controller, "alice", "internal", "db", "t", Set.of("secret")));
            Assertions.assertEquals(0, plugin.requests.get(), "native-only user state must not outlive its policy");
            ServicePolicies restored = snapshot(3, unsafe, bob, ascii);
            plugin.setPolicies(changes(removed, restored));
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT, column("c2")));
            Assertions.assertTrue(allowed(controller, "carol", "internal", "db", "t", Set.of("c3")));
            plugin.setPolicies(changes(restored, snapshot(4, bob, ascii)));
            plugin.requests.set(0);
            Assertions.assertTrue(allowed(controller, "alice", "internal", "db", "t", Set.of("c2")));
            Assertions.assertEquals(0, plugin.requests.get(), "delta deletion must clear native-only user state too");
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void unicodeFallbackPreservesNativeDecisionsAtEveryResourceLevel() {
        List<String> fields = List.of("catalog", "database", "table", "column");
        for (int level = 0; level < fields.size(); level++) {
            for (List<String> values : List.of(List.of("ſcope", "scope"), List.of("scope", "ſcope"),
                    List.of("ſcope", "ſcope"))) {
                CountingPlugin optimized = new CountingPlugin();
                RangerBasePlugin nativePlugin = new RangerBasePlugin(config());
                try {
                    RangerPolicy policy = grant("c1");
                    policy.getResources().put(fields.get(level), new RangerPolicyResource(values.get(0)));
                    List<String> query = new ArrayList<>(List.of("internal", "db", "t", "c1"));
                    query.set(level, values.get(1));
                    optimized.setPolicies(copy(snapshot(1, policy)));
                    nativePlugin.setPolicies(copy(snapshot(1, policy)));
                    boolean expected = allowed(new RangerDorisAccessController(nativePlugin, CONTEXT),
                            "alice", query.get(0), query.get(1), query.get(2), Set.of(query.get(3)));
                    boolean actual = allowed(new RangerDorisAccessController(optimized, CONTEXT),
                            "alice", query.get(0), query.get(1), query.get(2), Set.of(query.get(3)));
                    Assertions.assertEquals(expected, actual, fields.get(level) + values);
                    Assertions.assertEquals(values.get(0).equals(values.get(1)), expected,
                            "the fixture must cover native denial of aliases and native acceptance of exact names");
                } finally {
                    optimized.cleanup();
                    nativePlugin.cleanup();
                }
            }
        }
    }

    @Test
    void escapedResourcesPreserveNativeExpansionAtEveryResourceLevel() {
        List<String> fields = List.of("catalog", "database", "table", "column");
        for (int level = 0; level < fields.size(); level++) {
            for (List<String> values : List.of(List.of("sc\\\\ope", "sc\\\\ope"),
                    List.of("sc\\\\ope", "sc\\ope"), List.of("sc\\ope", "sc\\ope"))) {
                CountingPlugin optimized = new CountingPlugin();
                RangerBasePlugin nativePlugin = new RangerBasePlugin(config());
                try {
                    RangerPolicy policy = grant("c1");
                    policy.getResources().put(fields.get(level), new RangerPolicyResource(values.get(0)));
                    List<String> query = new ArrayList<>(List.of("internal", "db", "t", "c1"));
                    query.set(level, values.get(1));
                    optimized.setPolicies(copy(snapshot(1, policy)));
                    nativePlugin.setPolicies(copy(snapshot(1, policy)));
                    boolean expected = allowed(new RangerDorisAccessController(nativePlugin, CONTEXT),
                            "alice", query.get(0), query.get(1), query.get(2), Set.of(query.get(3)));
                    boolean actual = allowed(new RangerDorisAccessController(optimized, CONTEXT),
                            "alice", query.get(0), query.get(1), query.get(2), Set.of(query.get(3)));
                    Assertions.assertEquals("sc\\ope".equals(values.get(1)), expected,
                            "native context must unescape paired backslashes and preserve single backslashes");
                    Assertions.assertEquals(expected, actual, fields.get(level) + values);
                    Assertions.assertNull(optimized.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                            column(query.get(0), query.get(1), query.get(2), query.get(3))));
                } finally {
                    optimized.cleanup();
                    nativePlugin.cleanup();
                }
            }
        }
    }

    @Test
    void indexUsesNativeDelegateAdminPruningInsteadOfTheUnprunedPayload() {
        RangerPluginConfig indexedConfig = config();
        indexedConfig.getPolicyEngineOptions().evaluateDelegateAdminOnly = true;
        RangerPluginConfig nativeConfig = config();
        nativeConfig.getPolicyEngineOptions().evaluateDelegateAdminOnly = true;
        CountingPlugin optimized = new CountingPlugin(indexedConfig);
        RangerBasePlugin nativePlugin = new RangerBasePlugin(nativeConfig);
        try {
            RangerPolicy mixed = grant("c1");
            RangerPolicyItem delegated = new RangerPolicyItem();
            delegated.setUsers(List.of("bob"));
            delegated.setAccesses(List.of(new RangerPolicyItemAccess("SELECT", true)));
            delegated.setDelegateAdmin(true);
            mixed.setPolicyItems(List.of(mixed.getPolicyItems().get(0), delegated));
            ServicePolicies input = snapshot(1, mixed, RangerReadScopeSummaryTest.policy(2, "carol", "ADMIN",
                    Map.of("global", new RangerPolicyResource("*"))));
            optimized.setPolicies(copy(input));
            nativePlugin.setPolicies(copy(input));
            for (String user : List.of("alice", "bob", "carol")) {
                boolean expected = allowed(new RangerDorisAccessController(nativePlugin, CONTEXT),
                        user, "internal", "db", "t", Set.of("c1"));
                boolean actual = allowed(new RangerDorisAccessController(optimized, CONTEXT),
                        user, "internal", "db", "t", Set.of("c1"));
                Assertions.assertEquals("bob".equals(user), expected, "native fixture must actually prune other users");
                Assertions.assertEquals(expected, actual);
                Assertions.assertEquals(Boolean.valueOf(expected),
                        optimized.indexedReadPrivilege(user, DorisAccessType.SELECT, column("c1")));
            }
        } finally {
            optimized.cleanup();
            nativePlugin.cleanup();
        }
    }

    private static ServicePolicies randomSnapshot(Random random, long version) {
        List<RangerPolicy> policies = new ArrayList<>();
        policies.add(RangerReadScopeSummaryTest.policy(100, "operator", "ADMIN",
                Map.of("global", new RangerPolicyResource("*"))));
        for (int index = 0; index < 8; index++) {
            RangerPolicy policy = RangerReadScopeSummaryTest.policy(index + 1, "alice", "SELECT", randomResource(random));
            policy.setIsEnabled(random.nextInt(5) != 0);
            policy.setPolicyPriority(random.nextInt(2));
            List<Map<String, RangerPolicyResource>> extra = new ArrayList<>();
            for (int block = 0; block < 4; block++) {
                extra.add(randomResource(random));
            }
            policy.setAdditionalResources(extra);
            List<RangerPolicyItem> items = new ArrayList<>();
            for (int item = 0; item < 3; item++) {
                RangerPolicyItem entry = new RangerPolicyItem();
                Set<String> users = new LinkedHashSet<>();
                users.add(choose(random, USERS));
                users.add(choose(random, USERS));
                entry.setUsers(new ArrayList<>(users));
                entry.setAccesses(List.of(new RangerPolicyItemAccess("ADMIN", random.nextBoolean()),
                        new RangerPolicyItemAccess("SELECT", random.nextBoolean()),
                        new RangerPolicyItemAccess("LOAD", true)));
                items.add(entry);
            }
            policy.setPolicyItems(items);
            policies.add(policy);
        }
        ServicePolicies result = snapshot(version, policies.toArray(new RangerPolicy[0]));
        for (int level = 0; level < 4; level++) {
            result.getServiceDef().getResources().get(level).setMatcherOptions(Map.of(
                    "wildCard", Boolean.toString(random.nextBoolean()),
                    "ignoreCase", Boolean.toString(random.nextBoolean())));
        }
        return result;
    }

    private static Map<String, RangerPolicyResource> randomResource(Random random) {
        if (random.nextInt(12) == 0) {
            return Map.of("global", new RangerPolicyResource("*"));
        }
        Map<String, RangerPolicyResource> result = new HashMap<>();
        List<String> names = List.of("catalog", "database", "table", "column");
        List<List<String>> values = List.of(List.of("internal", "external", "*", "int*"),
                List.of("db", "DB", "analytics", "*", "d?"),
                List.of("t", "T", "orders", "other", "*", "ord*"),
                List.of("c1", "C1", "c2", "prefix*", "c?", "*", "alice", ""));
        int depth = 1 + random.nextInt(4);
        for (int level = 0; level < depth; level++) {
            List<String> selected = new ArrayList<>();
            selected.add(choose(random, values.get(level)));
            if (random.nextBoolean()) {
                selected.add(choose(random, values.get(level)));
            }
            result.put(names.get(level), new RangerPolicyResource(selected, false, false));
        }
        return result;
    }

    private static RangerAccessResultProcessor audits(List<String> output) {
        return new RangerAccessResultProcessor() {
            @Override
            public void processResult(RangerAccessResult result) {
                output.add(result.getPolicyId() + "/" + result.getAuditPolicyId() + "/" + result.getIsAllowed()
                        + "/" + result.getIsAudited() + "/" + result.getIsAccessDetermined()
                        + "/" + result.getIsAuditedDetermined() + "/" + result.getPolicyVersion()
                        + "/" + result.getPolicyPriority() + "/" + result.getPolicyType() + "/" + result.getZoneName()
                        + "/" + result.getReason()
                        + "/" + result.getAccessRequest().getAccessType() + "/"
                        + result.getAccessRequest().getResource().getAsString());
            }

            @Override
            public void processResults(Collection<RangerAccessResult> results) {
                results.forEach(this::processResult);
            }
        };
    }

    private static boolean allowed(RangerDorisAccessController controller, String user, String catalog,
            String database, String table, Set<String> columns) {
        try {
            controller.checkPrivilege(AuthorizedSubject.of(user, "%"),
                    AuthorizedResource.columns(catalog, database, table, columns),
                    AccessRequirements.SELECT, AccessContext.NONE);
            return true;
        } catch (AccessDeniedException denied) {
            return false;
        }
    }

    private static <T> T choose(Random random, List<T> choices) {
        return choices.get(random.nextInt(choices.size()));
    }

    private static ServicePolicies copy(ServicePolicies policies) {
        ServicePolicies result = JsonUtils.jsonToObject(JsonUtils.objectToJson(policies), ServicePolicies.class);
        Assertions.assertNotNull(result);
        return result;
    }

    private static ServicePolicies delta(long version, RangerPolicy policy) {
        ServicePolicies result = snapshot(version);
        result.setPolicies(null);
        RangerPolicyDelta update = new RangerPolicyDelta();
        update.setId(version);
        update.setChangeType(RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE);
        update.setPolicy(policy);
        result.setPolicyDeltas(List.of(update));
        return result;
    }

    private static ServicePolicies changes(ServicePolicies before, ServicePolicies after) {
        Map<Long, RangerPolicy> oldPolicies = new HashMap<>();
        Map<Long, RangerPolicy> newPolicies = new HashMap<>();
        before.getPolicies().forEach(policy -> oldPolicies.put(policy.getId(), policy));
        after.getPolicies().forEach(policy -> newPolicies.put(policy.getId(), policy));
        List<RangerPolicyDelta> updates = new ArrayList<>();
        for (RangerPolicy policy : before.getPolicies()) {
            if (newPolicies.containsKey(policy.getId())) {
                continue;
            }
            updates.add(change(after.getPolicyVersion(), updates.size(), RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE,
                    policy));
        }
        for (RangerPolicy policy : after.getPolicies()) {
            int type = RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE;
            if (oldPolicies.containsKey(policy.getId())) {
                type = RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE;
            }
            updates.add(change(after.getPolicyVersion(), updates.size(), type, policy));
        }
        ServicePolicies result = copy(after);
        result.setPolicies(null);
        result.setPolicyDeltas(updates);
        return result;
    }

    private static RangerPolicyDelta change(long version, int index, int type, RangerPolicy policy) {
        RangerPolicyDelta result = new RangerPolicyDelta();
        result.setId(version * 100 + index);
        result.setChangeType(type);
        result.setPolicy(policy);
        return result;
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(15, TimeUnit.SECONDS), "the controlled refresh phase was not reached");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static RangerPluginConfig config() {
        return RangerReadScopeSummaryTest.config();
    }

    private static ServicePolicies snapshot(long version, RangerPolicy... policies) {
        return RangerReadScopeSummaryTest.snapshot(version, policies);
    }

    private static RangerPolicy grant(String column) {
        return RangerReadScopeSummaryTest.policy(1, "alice", "SELECT",
                RangerReadScopeSummaryTest.resource("db", "t", column));
    }

    private static RangerDorisResource column(String name) {
        return column("internal", "db", "t", name);
    }

    private static RangerDorisResource column(String catalog, String database, String table, String name) {
        return new RangerDorisResource(DorisObjectType.COLUMN, catalog, database, table, name);
    }
}
