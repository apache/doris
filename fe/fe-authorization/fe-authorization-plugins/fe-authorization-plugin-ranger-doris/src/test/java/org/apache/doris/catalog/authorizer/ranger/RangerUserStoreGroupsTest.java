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
import org.apache.ranger.plugin.contextenricher.RangerAdminUserStoreRetriever;
import org.apache.ranger.plugin.contextenricher.RangerUserStoreEnricher;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The half of {@link RangerUserStoreGroups} that runs when policies arrive: putting the user store enricher
 * on the service definition, which is what makes the plugin download the store the other half reads.
 */
public class RangerUserStoreGroupsTest {

    private static RangerPluginConfig config() {
        // Service type "test" reads ranger-test-*.xml, none of which exist here: every property is at its
        // default, which is the state a deployment that has never heard of this is in.
        return new RangerPluginConfig("test", "test", null, null, null, null);
    }

    /** Policies as Ranger Admin hands them over: a service definition carrying no context enricher. */
    private static ServicePolicies downloaded() {
        RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("test");
        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("test");
        policies.setServiceDef(serviceDef);
        return policies;
    }

    private static List<RangerContextEnricherDef> enrichersOf(ServicePolicies policies) {
        return policies.getServiceDef().getContextEnrichers();
    }

    @Test
    public void testOnByDefault() {
        Assertions.assertTrue(RangerUserStoreGroups.enabledFor(config()));
    }

    /** The switch is read as Ranger reads its own copy of it, case and surrounding blanks aside. */
    @Test
    public void testReadsTheSwitchStrictly() {
        RangerPluginConfig config = config();
        config.set("ranger.plugin.test.use.rangerGroups", " False ");
        Assertions.assertFalse(RangerUserStoreGroups.enabledFor(config));
        config.set("ranger.plugin.test.use.rangerGroups", "TRUE");
        Assertions.assertTrue(RangerUserStoreGroups.enabledFor(config));
        config.set("ranger.plugin.test.use.rangerGroups", "");
        Assertions.assertTrue(RangerUserStoreGroups.enabledFor(config));
    }

    /**
     * Hadoop's getBoolean would take a mistyped opt-out as its default and switch this on; an operator who
     * has just decided the opposite is told so, by property and value, rather than granted on groups.
     */
    @Test
    public void testRefusesASwitchThatIsNeitherTrueNorFalse() {
        RangerPluginConfig config = config();
        config.set("ranger.plugin.test.use.rangerGroups", "flase");

        IllegalArgumentException refused = Assertions.assertThrows(IllegalArgumentException.class,
                () -> RangerUserStoreGroups.validate(config));

        Assertions.assertTrue(refused.getMessage().contains("ranger.plugin.test.use.rangerGroups=flase"),
                refused.getMessage());
        Assertions.assertThrows(IllegalArgumentException.class, () -> RangerUserStoreGroups.enabledFor(config));
    }

    /**
     * Ranger's enricher parses the interval inside the engine's construction, and a non-positive one
     * fails in Timer.schedule after the refresher thread is up; both are refused before any of that.
     */
    @Test
    public void testRefusesAnIntervalThatIsNotAPositiveNumberOfMilliseconds() {
        for (String interval : new String[] {"0", "-1", "often", "5s"}) {
            RangerPluginConfig config = config();
            config.set(RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION, interval);

            IllegalArgumentException refused = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> RangerUserStoreGroups.validate(config), interval);

            Assertions.assertTrue(refused.getMessage().contains(
                    RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION + "=" + interval),
                    refused.getMessage());
        }
    }

    @Test
    public void testAcceptsADefaultOrPositiveInterval() {
        RangerPluginConfig config = config();
        Assertions.assertDoesNotThrow(() -> RangerUserStoreGroups.validate(config));
        Assertions.assertEquals(60000L, RangerUserStoreGroups.refreshIntervalMsOf(config));
        config.set(RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION, " 5000 ");
        Assertions.assertDoesNotThrow(() -> RangerUserStoreGroups.validate(config));
        Assertions.assertEquals(5000L, RangerUserStoreGroups.refreshIntervalMsOf(config));
    }

    /** Switched off, no enricher reads the interval, so a bad one refuses nothing. */
    @Test
    public void testAnIntervalDoesNotMatterWhenSwitchedOff() {
        RangerPluginConfig config = config();
        config.set("ranger.plugin.test.use.rangerGroups", "false");
        config.set(RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION, "0");

        Assertions.assertDoesNotThrow(() -> RangerUserStoreGroups.validate(config));
    }

    /**
     * The enricher is the one Ranger adds for its own {@code use.rangerGroups}, options included, so that a
     * deployment that tuned the retriever or the interval for Ranger has tuned them here.
     */
    @Test
    public void testAddsTheUserStoreEnricherRangerWouldHaveAdded() {
        ServicePolicies policies = downloaded();

        RangerUserStoreGroups.addUserStoreEnricher(config(), policies);

        Assertions.assertTrue(ServiceDefUtil.isUserStoreEnricherPresent(policies));
        RangerContextEnricherDef enricher = enrichersOf(policies).get(0);
        Assertions.assertEquals(RangerUserStoreEnricher.class.getName(), enricher.getEnricher());
        Assertions.assertEquals(RangerAdminUserStoreRetriever.class.getCanonicalName(),
                enricher.getEnricherOptions().get(RangerUserStoreEnricher.USERSTORE_RETRIEVER_CLASSNAME_OPTION));
        Assertions.assertEquals("60000",
                enricher.getEnricherOptions().get(RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION));
    }

    @Test
    public void testHonoursTheRetrieverAndIntervalRangerReads() {
        RangerPluginConfig config = config();
        config.set(RangerUserStoreEnricher.USERSTORE_RETRIEVER_CLASSNAME_OPTION, "com.example.Retriever");
        config.set(RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION, "5000");
        ServicePolicies policies = downloaded();

        RangerUserStoreGroups.addUserStoreEnricher(config, policies);

        RangerContextEnricherDef enricher = enrichersOf(policies).get(0);
        Assertions.assertEquals("com.example.Retriever",
                enricher.getEnricherOptions().get(RangerUserStoreEnricher.USERSTORE_RETRIEVER_CLASSNAME_OPTION));
        Assertions.assertEquals("5000",
                enricher.getEnricherOptions().get(RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION));
    }

    /** Called on every download, deltas included, so a second call must not stack a second enricher. */
    @Test
    public void testAddsItOnce() {
        ServicePolicies policies = downloaded();

        RangerUserStoreGroups.addUserStoreEnricher(config(), policies);
        RangerUserStoreGroups.addUserStoreEnricher(config(), policies);

        Assertions.assertEquals(1, enrichersOf(policies).size());
    }

    @Test
    public void testLeavesTheDefinitionAloneWhenSwitchedOff() {
        RangerPluginConfig config = config();
        config.set("ranger.plugin.test.use.rangerGroups", "false");
        ServicePolicies policies = downloaded();

        RangerUserStoreGroups.addUserStoreEnricher(config, policies);

        Assertions.assertFalse(RangerUserStoreGroups.enabledFor(config));
        Assertions.assertFalse(ServiceDefUtil.isUserStoreEnricherPresent(policies));
    }

    /** What the refresher hands over when the service is gone: RangerBasePlugin copes with it, so must this. */
    @Test
    public void testToleratesNoPolicies() {
        Assertions.assertDoesNotThrow(() -> RangerUserStoreGroups.addUserStoreEnricher(config(), null));
    }

    /** The start-up line names the property either way, so an operator reading the log knows the switch. */
    @Test
    public void testDescribesItselfAndTheSwitch() {
        RangerPluginConfig config = config();
        Assertions.assertTrue(RangerUserStoreGroups.describe(config).contains("policy items written against a"
                + " group apply; set ranger.plugin.test.use.rangerGroups=false"));

        config.set("ranger.plugin.test.use.rangerGroups", "false");
        Assertions.assertTrue(RangerUserStoreGroups.describe(config).contains(
                "ranger.plugin.test.use.rangerGroups=false, so requests carry no groups"));
    }
}
