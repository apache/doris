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
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class RangerReadResourceIndexTest {
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

    private static final class CountingPlugin extends RangerDorisPlugin {
        private final AtomicInteger requests = new AtomicInteger();

        private CountingPlugin() {
            this(config());
        }

        private CountingPlugin(RangerPluginConfig configuration) {
            super(configuration);
        }

        @Override
        public RangerAccessResult isAccessAllowed(org.apache.ranger.plugin.policyengine.RangerAccessRequest request) {
            requests.incrementAndGet();
            return super.isAccessAllowed(request);
        }
    }

    @Test
    void controllerUsesIndexedColumnsAndPreservesTheFakeColumn() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        try {
            RangerPolicy alice = policy(1, "alice", "SELECT", resource("db", "t", "c1", "alice"));
            alice.setAdditionalResources(List.of(resource("db", "other", "c2", "alice")));
            plugin.setPolicies(snapshot(1, alice, policy(2, "bob", "SELECT", resource("db", "t", "c2", "bob"))));
            RangerDorisAccessController controller = new RangerDorisAccessController(plugin, CONTEXT);
            controller.checkPrivilege(AuthorizedSubject.of("alice", "%"),
                    AuthorizedResource.columns("internal", "db", "t", Set.of("c1", "alice")),
                    AccessRequirements.SELECT, AccessContext.NONE);
            Assertions.assertThrows(AccessDeniedException.class, () -> controller.checkPrivilege(
                    AuthorizedSubject.of("alice", "%"),
                    AuthorizedResource.columns("internal", "db", "t", Set.of("c2")),
                    AccessRequirements.SELECT, AccessContext.NONE));
            Assertions.assertEquals(0, plugin.requests.get(), "eligible Boolean checks must bypass raw Ranger calls");
            Assertions.assertFalse(plugin.indexedReadPrivilege("bob", DorisAccessType.SELECT,
                    column("db", "t", "alice")));
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.LOAD, column("db", "t", "c1")));
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void nativeMatcherParityIncludesMultipleItemsUsersBlocksAndColumnPatterns() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            List<RangerPolicy> policies = new ArrayList<>();
            for (int user = 0; user < 8; user++) {
                String name = "u" + user;
                RangerPolicy entry = policy(user + 1, name, "SELECT",
                        resource("db" + (user % 2), "t0", "c" + (user % 3), name));
                entry.setAdditionalResources(List.of(resource("db0", "t1", "prefix*", name),
                        resource("db1", "t2", "c?", name)));
                RangerPolicyItem admin = new RangerPolicyItem();
                admin.setUsers(List.of("shared", name));
                admin.setAccesses(List.of(new RangerPolicyItemAccess("ADMIN", true),
                        new RangerPolicyItemAccess("SELECT", false)));
                List<RangerPolicyItem> items = new ArrayList<>(entry.getPolicyItems());
                items.add(admin);
                entry.setPolicyItems(items);
                policies.add(entry);
            }
            RangerPolicy disabled = policy(99, "shared", "SELECT", resource("db0", "t0", "forbidden"));
            disabled.setIsEnabled(false);
            policies.add(disabled);
            ServicePolicies installed = snapshot(1, policies.toArray(new RangerPolicy[0]));
            installed.getServiceDef().getResources().get(3).setAccessTypeRestrictions(Set.of("SELECT"));
            plugin.setPolicies(installed);
            for (String user : List.of("u0", "u1", "u7", "shared", "absent")) {
                for (String database : List.of("db0", "db1", "DB0")) {
                    for (String table : List.of("t0", "t1", "t2", "absent")) {
                        for (String col : List.of("c0", "c1", "c3", "prefix_value", "u0", "forbidden")) {
                            for (DorisAccessType access : List.of(DorisAccessType.SELECT, DorisAccessType.ADMIN)) {
                                assertParity(plugin, user, access, column(database, table, col));
                            }
                        }
                    }
                }
            }
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void rawApiAndAuditProcessorsKeepUsingTheNativeEngine() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            Assertions.assertTrue(plugin.isAccessAllowed(new RangerAccessRequestImpl(column("db", "t", "c1"),
                    "SELECT", "alice", Set.of(), Set.of())).getIsAllowed());
            Assertions.assertEquals(1, plugin.requests.get());
            List<RangerAccessResult> audits = new ArrayList<>();
            plugin.setResultProcessor(new RangerAccessResultProcessor() {
                @Override
                public void processResult(RangerAccessResult result) {
                    audits.add(result);
                }

                @Override
                public void processResults(Collection<RangerAccessResult> results) {
                    audits.addAll(results);
                }
            });
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c1")));
            new RangerDorisAccessController(plugin, CONTEXT).checkPrivilege(AuthorizedSubject.of("alice", "%"),
                    AuthorizedResource.columns("internal", "db", "t", Set.of("c1")),
                    AccessRequirements.SELECT, AccessContext.NONE);
            Assertions.assertFalse(audits.isEmpty());
            Assertions.assertTrue(audits.get(audits.size() - 1).getIsAllowed());
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void fullRefreshAndDeltaRevokeOldIndexedGrants() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            Assertions.assertTrue(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c1")));
            plugin.setPolicies(snapshot(2, policy(2, "alice", "SELECT", resource("db", "t", "c2"))));
            Assertions.assertFalse(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c1")));
            Assertions.assertTrue(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c2")));
            ServicePolicies delta = snapshot(3);
            delta.setPolicies(null);
            RangerPolicyDelta remove = new RangerPolicyDelta();
            remove.setId(1L);
            remove.setChangeType(RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE);
            remove.setPolicy(policy(2, "alice", "SELECT", resource("db", "t", "c2")));
            delta.setPolicyDeltas(List.of(remove));
            plugin.setPolicies(delta);
            assertParity(plugin, "alice", DorisAccessType.SELECT, column("db", "t", "c2"));
            Assertions.assertFalse(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c2")));
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void unsupportedSnapshotsAndInPlaceUpdatesUseNativeChecks() {
        for (String variant : List.of("group", "deny", "inPlace", "disabled")) {
            RangerPluginConfig configuration = config();
            RangerPolicy entry = policy(1, "alice", "SELECT", resource("db", "t", "c1"));
            if (variant.equals("group")) {
                entry.getPolicyItems().get(0).setGroups(List.of("employees"));
            }
            if (variant.equals("deny")) {
                entry.setDenyPolicyItems(entry.getPolicyItems());
            }
            if (variant.equals("inPlace")) {
                configuration.set("ranger.plugin.doris.supports.in.place.policy.updates", "true");
            }
            if (variant.equals("disabled")) {
                configuration.set(configuration.getPropertyPrefix() + RangerReadResourceIndex.ENABLED, "false");
            }
            CountingPlugin plugin = new CountingPlugin(configuration);
            try {
                plugin.setPolicies(snapshot(1, entry));
                Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                        column("db", "t", "c1")), variant);
            } finally {
                plugin.cleanup();
            }
        }
    }

    @Test
    void globalAdministrationAndBroadPrefixesCoexistWithIndexedColumns() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        try {
            RangerPolicy global = policy(1, "operator", "ADMIN", Map.of("global", new RangerPolicyResource("*")));
            RangerPolicy alice = policy(2, "alice", "SELECT", resource("db", "t", "c1"));
            RangerPolicy wildcard = policy(3, "bob", "SELECT", resource("db", "*", "c2"));
            RangerPolicy multiple = policy(4, "alice", "SELECT", resource("db", "t", "c3"));
            multiple.getResources().put("table", new RangerPolicyResource(List.of("t", "t2"), false, false));
            RangerPolicy parent = policy(5, "carol", "SELECT", Map.of(
                    "catalog", new RangerPolicyResource("internal"), "database", new RangerPolicyResource("db")));
            plugin.setPolicies(snapshot(1, global, alice, wildcard, multiple, parent));
            for (String user : List.of("operator", "alice", "bob", "carol", "unknown")) {
                for (String table : List.of("t", "t2", "other")) {
                    for (String col : List.of("c1", "c2", "c3", "unlisted")) {
                        assertParity(plugin, user, DorisAccessType.SELECT, column("db", table, col));
                        assertParity(plugin, user, DorisAccessType.ADMIN, column("db", table, col));
                    }
                }
            }
            plugin.requests.set(0);
            new RangerDorisAccessController(plugin, CONTEXT).checkPrivilege(AuthorizedSubject.of("alice", "%"),
                    AuthorizedResource.columns("internal", "db", "t", Set.of("c1", "c3")),
                    AccessRequirements.SELECT, AccessContext.NONE);
            Assertions.assertEquals(0, plugin.requests.get(), "a global administrator must not disable employee indexing");
            new RangerDorisAccessController(plugin, CONTEXT).checkPrivilege(AuthorizedSubject.of("operator", "%"),
                    AuthorizedResource.columns("internal", "db", "absent", Set.of("anything")),
                    AccessRequirements.SELECT, AccessContext.NONE);
            Assertions.assertTrue(plugin.requests.get() > 0, "global ADMIN must retain the original upper-scope grant");
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void caseSensitiveKeysAndRuntimeSuperUsersRemainCorrect() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            ServicePolicies policies = snapshot(1, policy(1, "alice", "SELECT", resource("db", "Table", "c1")));
            policies.getServiceDef().getResources().get(2).setMatcherOptions(
                    Map.of("wildCard", "true", "ignoreCase", "false"));
            plugin.setPolicies(policies);
            assertParity(plugin, "alice", DorisAccessType.SELECT, column("db", "Table", "c1"));
            assertParity(plugin, "alice", DorisAccessType.SELECT, column("db", "table", "c1"));
            plugin.setSuperUsersAndGroups(Set.of("alice"), Set.of());
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "absent", "c1")));
            plugin.setSuperUsersAndGroups(Set.of(), Set.of());
            Assertions.assertFalse(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "absent", "c1")));
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void sameTableNamesInDifferentCatalogsAndDatabasesDoNotShareGrants() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            Map<String, RangerPolicyResource> external = resource("db", "t", "external_only");
            external.put("catalog", new RangerPolicyResource("external"));
            plugin.setPolicies(snapshot(1,
                    policy(1, "alice", "SELECT", resource("db", "t", "internal_only")),
                    policy(2, "alice", "SELECT", external),
                    policy(3, "alice", "SELECT", resource("other_db", "t", "other_db_only"))));
            for (String catalog : List.of("internal", "external", "INTERNAL")) {
                for (String database : List.of("db", "other_db")) {
                    for (String col : List.of("internal_only", "external_only", "other_db_only")) {
                        assertParity(plugin, "alice", DorisAccessType.SELECT,
                                new RangerDorisResource(DorisObjectType.COLUMN, catalog, database, "t", col));
                    }
                }
            }
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void invalidIndexConfigurationDoesNotRetainAnOldGrant() {
        CountingPlugin plugin = new CountingPlugin();
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            plugin.getConfig().set(plugin.getConfig().getPropertyPrefix() + RangerReadResourceIndex.ENABLED, "typo");
            Assertions.assertThrows(IllegalArgumentException.class, () -> plugin.setPolicies(
                    snapshot(2, policy(2, "alice", "SELECT", resource("db", "t", "c2")))));
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c1")));
            Assertions.assertFalse(plugin.isAccessAllowed(new RangerAccessRequestImpl(column("db", "t", "c1"),
                    "SELECT", "alice", Set.of(), Set.of())).getIsAllowed());
        } finally {
            plugin.cleanup();
        }
    }

    @Test
    void concurrentRefreshNeverGrantsAnUnlistedColumnAndCleanupDropsTheIndex() throws Exception {
        CountingPlugin plugin = new CountingPlugin();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            plugin.setPolicies(snapshot(1, policy(1, "alice", "SELECT", resource("db", "t", "c1"))));
            List<Future<?>> readers = new ArrayList<>();
            for (int reader = 0; reader < 3; reader++) {
                readers.add(executor.submit(() -> {
                    for (int iteration = 0; iteration < 1000; iteration++) {
                        Boolean allowed = plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                                column("db", "t", "unlisted"));
                        Assertions.assertNotEquals(Boolean.TRUE, allowed);
                    }
                }));
            }
            for (int version = 2; version <= 8; version++) {
                plugin.setPolicies(snapshot(version, policy(1, "alice", "SELECT",
                        resource("db", "t", "c" + version))));
            }
            for (Future<?> reader : readers) {
                reader.get();
            }
            Assertions.assertTrue(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c8")));
            Assertions.assertFalse(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c1")));
            plugin.cleanup();
            Assertions.assertNull(plugin.indexedReadPrivilege("alice", DorisAccessType.SELECT,
                    column("db", "t", "c8")));
        } finally {
            executor.shutdownNow();
            plugin.cleanup();
        }
    }

    private static void assertParity(CountingPlugin plugin, String user, DorisAccessType access,
            RangerDorisResource resource) {
        boolean expected = plugin.isAccessAllowed(new RangerAccessRequestImpl(resource, access.name(), user,
                Set.of(), Set.of())).getIsAllowed();
        Assertions.assertEquals(Boolean.valueOf(expected), plugin.indexedReadPrivilege(user, access, resource),
                user + "/" + access + "/" + resource);
    }

    private static RangerDorisResource column(String database, String table, String column) {
        return new RangerDorisResource(DorisObjectType.COLUMN, "internal", database, table, column);
    }

    private static RangerPluginConfig config() {
        return RangerReadScopeSummaryTest.config();
    }

    private static RangerPolicy policy(long id, String user, String action, Map<String, RangerPolicyResource> resource) {
        return RangerReadScopeSummaryTest.policy(id, user, action, resource);
    }

    private static Map<String, RangerPolicyResource> resource(String database, String table, String... columns) {
        return RangerReadScopeSummaryTest.resource(database, table, columns);
    }

    private static ServicePolicies snapshot(long version, RangerPolicy... policies) {
        return RangerReadScopeSummaryTest.snapshot(version, policies);
    }
}
