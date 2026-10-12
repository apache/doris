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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.contextenricher.RangerAbstractContextEnricher;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The contract of {@link LoadedRangerPlugin}, run through Ranger's real {@code RangerBasePlugin.init()} -
 * policy refresher, policy cache and user store enricher included - against a Ranger admin this test stands
 * in for: the plugin is built out of what the admin serves or what the cache holds, and not at all otherwise.
 */
public class LoadedRangerPluginTest {
    private static final String PREFIX = "ranger.plugin.test";
    /** Named in a refusal; never dialed, since {@link Admin} takes the REST client's place. */
    private static final String ADMIN_URL = "http://ranger.invalid:6080";

    /**
     * Stands in for Ranger Admin. Ranger builds it by class name ({@code policy.source.impl}) and asks it for
     * the roles, the policies and the user store exactly as it asks its REST client. Down, every call throws,
     * which is all an unreachable admin is to Ranger; up, it serves what the test put here.
     */
    public static final class Admin implements RangerAdminClient {
        private static volatile ServicePolicies policies;
        private static volatile RangerUserStore userStore;
        private static volatile boolean serviceUnknown;

        private static void down() {
            policies = null;
            userStore = null;
            serviceUnknown = false;
        }

        @Override
        public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        }

        @Override
        public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis)
                throws Exception {
            if (serviceUnknown) {
                throw new RangerServiceNotFoundException("test");
            }
            ServicePolicies current = policies;
            if (current == null) {
                throw new Exception("connection refused");
            }
            return lastKnownVersion < current.getPolicyVersion() ? current : null;
        }

        @Override
        public RangerRoles getRolesIfUpdated(long lastKnownRoleVersion, long lastActivationTimeInMills)
                throws Exception {
            if (policies == null) {
                throw new Exception("connection refused");
            }
            return null;
        }

        @Override
        public RangerUserStore getUserStoreIfUpdated(long lastKnownUserStoreVersion, long lastActivationTimeInMillis)
                throws Exception {
            RangerUserStore current = userStore;
            if (current == null) {
                throw new Exception("connection refused");
            }
            return lastKnownUserStoreVersion < current.getUserStoreVersion() ? current : null;
        }

        @Override
        public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) {
            return null;
        }

        @Override
        public List<String> getTagTypes(String tagTypePattern) {
            return Collections.emptyList();
        }

        // Administration this plugin never performs.

        @Override
        public RangerRole createRole(RangerRole request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropRole(String execUser, String roleName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getAllRoles(String execUser) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getUserRoles(String execUser) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RangerRole getRole(String execUser, String roleName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void grantRole(GrantRevokeRoleRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revokeRole(GrantRevokeRoleRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void grantAccess(GrantRevokeRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revokeAccess(GrantRevokeRequest request) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A context enricher the policy engine cannot be built with: Ranger builds the enrichers a service
     * definition names while it builds the engine, and does not catch what their init() throws there.
     */
    public static final class BrokenEnricher extends RangerAbstractContextEnricher {
        @Override
        public void init() {
            throw new IllegalStateException("broken on purpose");
        }

        @Override
        public void enrich(RangerAccessRequest request) {
        }
    }

    private static final class TestPlugin extends LoadedRangerPlugin {
        private TestPlugin(String cacheDir) {
            // Service type "test" reads ranger-test-*.xml, none of which exist here: what the load needs is set
            // below, and the admin it asks is the class above.
            super("test", "test", null);
            getConfig().set(PREFIX + ".policy.source.impl", Admin.class.getName());
            getConfig().set(PREFIX + ".policy.rest.url", ADMIN_URL);
            if (cacheDir != null) {
                getConfig().set(PREFIX + ".policy.cache.dir", cacheDir);
            }
        }

        /** Whether there is a policy engine to answer out of; what a refused plugin must not leave behind. */
        private boolean hasEngine() {
            return getPolicyEngine() != null;
        }
    }

    @TempDir
    Path cacheDir;

    private TestPlugin plugin;

    @BeforeEach
    public void takeTheAdminDown() {
        Admin.down();
    }

    @AfterEach
    public void stopThePlugin() {
        if (plugin != null) {
            plugin.cleanup();
        }
    }

    private TestPlugin build(String cacheDir) {
        plugin = new TestPlugin(cacheDir);
        plugin.init();
        return plugin;
    }

    /** Policies of {@code version} with nothing in them, enough for Ranger to build an engine out of. */
    private static ServicePolicies policies(long version) {
        RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("test");
        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("test");
        policies.setServiceDef(serviceDef);
        policies.setPolicyVersion(version);
        policies.setPolicies(new ArrayList<>());
        return policies;
    }

    /** What Ranger's refresher writes when the admin answers, where it reads it back when the admin does not. */
    private void cache(ServicePolicies policies) throws IOException {
        // <appId>_<serviceName>.json, the appId defaulting to the service type.
        Files.write(cacheDir.resolve("test_test.json"),
                JsonUtils.objectToJson(policies).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean refresherRunning() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().startsWith("PolicyRefresher(serviceName=test)"));
    }

    /** The thread the user store enricher starts to download the store, which cleanup() reaches through the engine. */
    private static boolean userStoreRefresherRunning() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().startsWith("RangerUserStoreRefresher(serviceName=test)"));
    }

    /** Built out of what the admin serves, groups included: the user store came with the policies. */
    @Test
    public void testBuiltWithWhatTheAdminServes() {
        Admin.policies = policies(3L);
        Admin.userStore = new RangerUserStore(1L, null, null,
                ImmutableMap.of("user1", ImmutableSet.of("readers")));

        build(cacheDir.toString());

        Assertions.assertEquals(3L, plugin.getPoliciesVersion());
        Assertions.assertEquals(1L, plugin.getUserStoreVersion());
        // Through Ranger's own enricher, which the policies were handed on with: what a request carries.
        Assertions.assertEquals(ImmutableSet.of("readers"), RangerUserStoreGroups.groupsOf(plugin, "user1"));
        Assertions.assertTrue(RangerUserStoreGroups.groupsOf(plugin, "nobody").isEmpty(),
                "a user the store does not know is in a group");
        Assertions.assertTrue(refresherRunning(), "built, the plugin keeps polling");
    }

    /**
     * The admin is down and what it served last time is in the cache: built out of that, the way Ranger's
     * own plugins ride out an outage. No store came with it, which is logged and not refused.
     */
    @Test
    public void testBuiltOutOfTheCacheWhenTheAdminIsDown() throws IOException {
        cache(policies(7L));

        build(cacheDir.toString());

        Assertions.assertEquals(7L, plugin.getPoliciesVersion());
        Assertions.assertTrue(plugin.getUserStoreVersion() < 0);
        Assertions.assertTrue(RangerUserStoreGroups.groupsOf(plugin, "user1").isEmpty(),
                "groups came from somewhere with no store");
    }

    /** Nothing from the admin, nothing in the cache: refused with the cause, and nothing left running. */
    @Test
    public void testRefusedWithNoPoliciesFromTheAdminOrTheCache() {
        plugin = new TestPlugin(cacheDir.toString());

        IllegalStateException refused = Assertions.assertThrows(IllegalStateException.class, plugin::init);

        Assertions.assertTrue(refused.getMessage().contains("has no policies to authorize against"),
                refused.getMessage());
        Assertions.assertTrue(refused.getMessage().contains(ADMIN_URL), refused.getMessage());
        Assertions.assertTrue(refused.getMessage().contains(cacheDir.toString()), refused.getMessage());
        Assertions.assertFalse(plugin.hasEngine(), "an engine was left behind");
        Assertions.assertFalse(refresherRunning(), "the policy refresher init() started is still running");
    }

    @Test
    public void testRefusedWithNoCacheDirectoryConfigured() {
        plugin = new TestPlugin(null);

        IllegalStateException refused = Assertions.assertThrows(IllegalStateException.class, plugin::init);

        Assertions.assertTrue(refused.getMessage().contains(PREFIX + ".policy.cache.dir"), refused.getMessage());
        Assertions.assertFalse(refresherRunning());
    }

    /** A service the admin does not know - a mistyped service name - has no policies, cache or no cache. */
    @Test
    public void testRefusedForAServiceTheAdminDoesNotKnow() throws IOException {
        cache(policies(7L));
        Admin.serviceUnknown = true;
        plugin = new TestPlugin(cacheDir.toString());

        Assertions.assertThrows(IllegalStateException.class, plugin::init);

        Assertions.assertFalse(plugin.hasEngine(), "an engine was left behind");
        Assertions.assertFalse(refresherRunning());
    }

    /** A configuration the load cannot use fails as it always did: on the calling thread, with its cause. */
    @Test
    public void testAConfigurationTheLoadCannotUseIsRefusedWithItsCause() {
        plugin = new TestPlugin(cacheDir.toString());
        plugin.getConfig().set(PREFIX + ".policy.pollIntervalMs", "often");

        Assertions.assertThrows(NumberFormatException.class, plugin::init);

        Assertions.assertFalse(refresherRunning());
    }

    /**
     * A refresh interval the enricher could not schedule is refused before the load starts. Left to the
     * enricher, a non-positive interval fails in Timer.schedule after the store is downloaded and the
     * refresher thread is up, inside the engine's construction: RangerBasePlugin.setPolicies catches it, the
     * plugin is refused as if it had no policies, and cleanup() cannot reach the thread through an engine
     * that was never built.
     */
    @Test
    public void testRefusedBeforeTheLoadForAnIntervalTheEnricherCouldNotSchedule() {
        Admin.policies = policies(3L);
        Admin.userStore = new RangerUserStore(1L, null, null, ImmutableMap.of("user1", ImmutableSet.of("readers")));
        plugin = new TestPlugin(cacheDir.toString());
        plugin.getConfig().set("userStoreRefresherPollingInterval", "0");

        IllegalArgumentException refused = Assertions.assertThrows(IllegalArgumentException.class, plugin::init);

        Assertions.assertTrue(refused.getMessage().contains("userStoreRefresherPollingInterval=0"),
                refused.getMessage());
        Assertions.assertFalse(plugin.hasEngine(), "an engine was left behind");
        Assertions.assertFalse(refresherRunning(), "the policy refresher was started");
        Assertions.assertFalse(userStoreRefresherRunning(), "the user store refresher was started");
    }

    /** An opt-out Hadoop's getBoolean would have read as the default is refused, not switched on. */
    @Test
    public void testRefusedBeforeTheLoadForASwitchThatIsNeitherTrueNorFalse() {
        Admin.policies = policies(3L);
        plugin = new TestPlugin(cacheDir.toString());
        plugin.getConfig().set(PREFIX + ".use.rangerGroups", "flase");

        IllegalArgumentException refused = Assertions.assertThrows(IllegalArgumentException.class, plugin::init);

        Assertions.assertTrue(refused.getMessage().contains(PREFIX + ".use.rangerGroups=flase"), refused.getMessage());
        Assertions.assertFalse(refresherRunning());
    }

    /**
     * The policies arrived and Ranger could build no engine out of them - here, a context enricher on the
     * service definition whose init() throws; RangerBasePlugin.setPolicies logs that and keeps going with
     * no engine, the same state as no policies at all. Refused for what happened, not for an admin that
     * answered.
     */
    @Test
    public void testRefusedWithPoliciesNoEngineCouldBeBuiltOutOf() {
        ServicePolicies policies = policies(3L);
        policies.getServiceDef().setContextEnrichers(Collections.singletonList(
                new RangerServiceDef.RangerContextEnricherDef(1L, "broken", BrokenEnricher.class.getName(), null)));
        Admin.policies = policies;
        plugin = new TestPlugin(cacheDir.toString());

        IllegalStateException refused = Assertions.assertThrows(IllegalStateException.class, plugin::init);

        Assertions.assertTrue(refused.getMessage().contains("policies (version 3) were downloaded, but no policy"
                + " engine could be built out of them"), refused.getMessage());
        Assertions.assertFalse(refused.getMessage().contains("could not be reached"), refused.getMessage());
        Assertions.assertFalse(plugin.hasEngine(), "an engine was left behind");
        Assertions.assertFalse(refresherRunning());
    }

    /** The seam the groups hang on: policies are handed on with the user store enricher on their definition. */
    @Test
    public void testThePoliciesAreHandedOnWithTheUserStoreEnricher() {
        plugin = new TestPlugin(null);
        ServicePolicies policies = policies(1L);

        plugin.setPolicies(policies);

        Assertions.assertNotNull(plugin.getUserStoreEnricher(), "no user store enricher on the engine");
        Assertions.assertTrue(policies.getServiceDef().getContextEnrichers().stream()
                .anyMatch(enricher -> "userStoreEnricher".equals(enricher.getName())));
    }
}
