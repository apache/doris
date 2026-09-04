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

package org.apache.doris.connector.maxcompute;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for {@link MCCatalogProperties}. The per-branch validation messages a user sees are pinned
 * through the CREATE door in {@code MaxComputeConnectorProviderTest}; what is tested here is the holder's
 * own contract — binding, defaults, the derived endpoint and enums, and the three rules an of() must obey.
 */
class MCCatalogPropertiesTest {

    @Test
    void bindsEveryKeyAndDefaults() {
        MCCatalogProperties p = MCTestProperties.minimal();
        Assertions.assertEquals(MCTestProperties.PROJECT, p.getProject());
        Assertions.assertEquals(MCTestProperties.ENDPOINT, p.getResolvedEndpoint());
        Assertions.assertEquals("ak", p.getAccessKey());
        Assertions.assertEquals("sk", p.getSecretKey());
        Assertions.assertEquals("pay-as-you-go", p.getQuota());
        Assertions.assertEquals(MCCatalogProperties.AuthType.AK_SK, p.getAuthType());
        Assertions.assertEquals(MCCatalogProperties.AccountFormat.NAME, p.getAccountFormat());
        Assertions.assertEquals(MCCatalogProperties.SplitStrategy.BYTE_SIZE, p.getSplitStrategy());
        Assertions.assertEquals(268435456L, p.getSplitByteSize());
        Assertions.assertEquals(1048576L, p.getSplitRowCount());
        Assertions.assertTrue(p.isSplitCrossPartition());
        Assertions.assertTrue(p.isDateTimePredicatePushDown());
        Assertions.assertFalse(p.isEnableNamespaceSchema());
        Assertions.assertEquals(10, p.getConnectTimeout());
        Assertions.assertEquals(120, p.getReadTimeout());
        Assertions.assertEquals(4, p.getRetryCount());
        Assertions.assertEquals(8388608L, p.getMaxFieldSize());

        Map<String, String> m = MCTestProperties.minimalMap();
        m.put(MCCatalogProperties.QUOTA, "q");
        m.put(MCCatalogProperties.SPLIT_CROSS_PARTITION, "false");
        m.put(MCCatalogProperties.DATETIME_PREDICATE_PUSH_DOWN, "false");
        m.put(MCCatalogProperties.ENABLE_NAMESPACE_SCHEMA, "true");
        m.put(MCCatalogProperties.CONNECT_TIMEOUT, "30");
        m.put(MCCatalogProperties.MAX_FIELD_SIZE, "1024");
        MCCatalogProperties set = MCCatalogProperties.of(m);
        Assertions.assertEquals("q", set.getQuota());
        Assertions.assertFalse(set.isSplitCrossPartition());
        Assertions.assertFalse(set.isDateTimePredicatePushDown());
        Assertions.assertTrue(set.isEnableNamespaceSchema());
        Assertions.assertEquals(30, set.getConnectTimeout());
        Assertions.assertEquals(1024L, set.getMaxFieldSize());
    }

    // Guards DESIGN D3(2): the map also carries engine keys and storage keys, and ALTER CATALOG merges
    // properties -- it can overwrite a key but never remove one, so refusing an unrecognized name would
    // leave a catalog that no statement could repair.
    @Test
    void unknownKeysAreTolerated() {
        Map<String, String> m = MCTestProperties.minimalMap();
        m.put("some_future_key", "x");
        m.put("s3.endpoint", "http://minio:9000");
        m.put("type", "max_compute");
        Assertions.assertDoesNotThrow(() -> MCCatalogProperties.of(m));
    }

    // Guards DESIGN D3(1): of() runs at CREATE, again on the merged candidate at ALTER, and once more
    // every time the connector is rebuilt -- including on an FE replaying the edit log -- so it must be a
    // pure function of its input.
    @Test
    void ofIsPureAndRepeatable() {
        Map<String, String> m = MCTestProperties.minimalMap();
        Map<String, String> before = new LinkedHashMap<>(m);

        MCCatalogProperties first = MCCatalogProperties.of(m);
        MCCatalogProperties second = MCCatalogProperties.of(m);

        Assertions.assertEquals(before, m, "of() must not mutate the caller's map");
        Assertions.assertEquals(first.getResolvedEndpoint(), second.getResolvedEndpoint());
        Assertions.assertEquals(first.getRaw(), second.getRaw());
    }

    @Test
    void missingRequiredKeysFailNamingTheKey() {
        Map<String, String> noProject = MCTestProperties.minimalMap();
        noProject.remove(MCCatalogProperties.PROJECT);
        Assertions.assertEquals("Required property '" + MCCatalogProperties.PROJECT + "' is missing",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> MCCatalogProperties.of(noProject)).getMessage());

        Map<String, String> noEndpoint = MCTestProperties.minimalMap();
        noEndpoint.remove(MCCatalogProperties.ENDPOINT);
        Assertions.assertEquals("Required property '" + MCCatalogProperties.ENDPOINT + "' is missing",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> MCCatalogProperties.of(noEndpoint)).getMessage());
    }

    // --- endpoint resolution: four spellings in priority order ---

    @Test
    void endpointWinsOverEveryLegacySpelling() {
        Map<String, String> m = MCTestProperties.minimalMap();
        m.put(MCCatalogProperties.TUNNEL_SDK_ENDPOINT, "http://dt.cn-beijing.maxcompute.aliyun-inc.com");
        m.put(MCCatalogProperties.ODPS_ENDPOINT, "http://odps.example/api");
        m.put(MCCatalogProperties.REGION, "cn-beijing");
        Assertions.assertEquals(MCTestProperties.ENDPOINT,
                MCCatalogProperties.of(m).getResolvedEndpoint());
    }

    @Test
    void tunnelEndpointIsRewrittenToTheServiceEndpoint() {
        Map<String, String> m = legacyOnly(MCCatalogProperties.TUNNEL_SDK_ENDPOINT,
                "http://dt.cn-beijing.maxcompute.aliyun-inc.com");
        Assertions.assertEquals("http://service.cn-beijing.maxcompute.aliyun-inc.com/api",
                MCCatalogProperties.of(m).getResolvedEndpoint());
    }

    @Test
    void odpsEndpointIsUsedAsWritten() {
        Map<String, String> m = legacyOnly(MCCatalogProperties.ODPS_ENDPOINT, "http://odps.example/api");
        Assertions.assertEquals("http://odps.example/api",
                MCCatalogProperties.of(m).getResolvedEndpoint());
    }

    @Test
    void regionFillsTheTemplateAndPublicAccessDropsTheIntranetSuffix() {
        Map<String, String> intranet = legacyOnly(MCCatalogProperties.REGION, "oss-cn-beijing");
        Assertions.assertEquals("http://service.cn-beijing.maxcompute.aliyun-inc.com/api",
                MCCatalogProperties.of(intranet).getResolvedEndpoint(),
                "an oss- prefixed region must be stripped before filling the template");

        Map<String, String> pub = legacyOnly(MCCatalogProperties.REGION, "cn-beijing");
        pub.put(MCCatalogProperties.PUBLIC_ACCESS, "true");
        Assertions.assertEquals("http://service.cn-beijing.maxcompute.aliyun.com/api",
                MCCatalogProperties.of(pub).getResolvedEndpoint());
    }

    // A catalog stored before mc.endpoint existed carries only a legacy spelling. It must go on building
    // -- of() runs on every FE restart, and refusing it there would take the catalog away from its owner
    // with no statement able to fix it -- while a statement a user writes now still has to spell
    // mc.endpoint. That split is the whole reason checkCreateTimeOnlyRules exists.
    @Test
    void legacyOnlyCatalogBuildsButCannotBeWrittenToday() {
        Map<String, String> m = legacyOnly(MCCatalogProperties.ODPS_ENDPOINT, "http://odps.example/api");
        MCCatalogProperties p = MCCatalogProperties.of(m);
        Assertions.assertEquals("http://odps.example/api", p.getResolvedEndpoint());
        Assertions.assertThrows(IllegalArgumentException.class, p::checkCreateTimeOnlyRules);
    }

    // --- value enums ---

    @Test
    void authTypeMatchesCaseInsensitivelyAndTheOtherTwoDoNot() {
        Assertions.assertEquals(MCCatalogProperties.AuthType.ECS_RAM_ROLE,
                authWith("ECS_RAM_ROLE", MCCatalogProperties.ECS_RAM_ROLE, "role").getAuthType());
        // account_format / split_strategy were compared with equals() before this class existed; keeping
        // that means an upper-case spelling stays an error rather than silently starting to work.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MCTestProperties.with(MCCatalogProperties.ACCOUNT_FORMAT, "ID"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MCTestProperties.with(MCCatalogProperties.SPLIT_STRATEGY, "BYTE_SIZE"));
    }

    @Test
    void unknownEnumValuesAreRefused() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MCTestProperties.with(MCCatalogProperties.SPLIT_STRATEGY, "by_hand"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MCTestProperties.with(MCCatalogProperties.ACCOUNT_FORMAT, "nickname"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MCTestProperties.with(MCCatalogProperties.AUTH_TYPE, "oauth"));
    }

    // Each auth type needs its own credentials; without the check the client is built and fails much
    // later, at the first request, saying only that the service refused it.
    @Test
    void eachAuthTypeRequiresItsOwnCredentials() {
        Map<String, String> akOnly = MCTestProperties.minimalMap();
        akOnly.remove(MCCatalogProperties.SECRET_KEY);
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> MCCatalogProperties.of(akOnly)).getMessage().contains("secret key"));

        Map<String, String> ramNoArn = MCTestProperties.minimalMap();
        ramNoArn.put(MCCatalogProperties.AUTH_TYPE,
                MCCatalogProperties.AuthType.RAM_ROLE_ARN.getValue());
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> MCCatalogProperties.of(ramNoArn)).getMessage().contains("role arn"));

        Map<String, String> ecsNoRole = MCTestProperties.minimalMap();
        ecsNoRole.put(MCCatalogProperties.AUTH_TYPE,
                MCCatalogProperties.AuthType.ECS_RAM_ROLE.getValue());
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> MCCatalogProperties.of(ecsNoRole)).getMessage().contains("role name"));
    }

    // Behaviour change worth pinning: the split sizes used to be parsed only for the selected strategy,
    // so a malformed value on the other one rode along unnoticed. Both are bound now, which is what makes
    // "this object exists, therefore its properties are valid" true of every key rather than of the ones
    // a particular strategy happens to read.
    @Test
    void theSplitSizeOfTheUnselectedStrategyIsStillChecked() {
        Map<String, String> m = MCTestProperties.minimalMap();
        m.put(MCCatalogProperties.SPLIT_STRATEGY, MCCatalogProperties.SplitStrategy.BYTE_SIZE.getValue());
        m.put(MCCatalogProperties.SPLIT_ROW_COUNT, "not_a_number");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MCCatalogProperties.of(m));
    }

    // Guards DESIGN D5: toString() is what a log line renders, and the raw map behind this object holds
    // the AccessKey secret.
    @Test
    void toStringMasksTheCredentials() {
        String rendered = MCTestProperties.minimal().toString();
        Assertions.assertFalse(rendered.contains("sk"), "got: " + rendered);
        Assertions.assertTrue(rendered.contains("secretKey=***"), "got: " + rendered);
        Assertions.assertTrue(rendered.contains("accessKey=***"), "got: " + rendered);
        Assertions.assertTrue(rendered.contains(MCTestProperties.PROJECT), "got: " + rendered);
    }

    @Test
    void rawIsAnImmutableCopyOfTheInput() {
        Map<String, String> m = MCTestProperties.minimalMap();
        MCCatalogProperties p = MCCatalogProperties.of(m);
        m.put("added_afterwards", "v");
        Assertions.assertFalse(p.getRaw().containsKey("added_afterwards"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> p.getRaw().put("k", "v"));
    }

    /** The minimal properties with mc.endpoint replaced by one of the legacy spellings. */
    private static Map<String, String> legacyOnly(String legacyKey, String value) {
        Map<String, String> m = MCTestProperties.minimalMap();
        m.remove(MCCatalogProperties.ENDPOINT);
        m.put(legacyKey, value);
        return m;
    }

    private static MCCatalogProperties authWith(String authType, String credentialKey, String value) {
        Map<String, String> m = MCTestProperties.minimalMap();
        m.put(MCCatalogProperties.AUTH_TYPE, authType);
        m.put(credentialKey, value);
        return MCCatalogProperties.of(m);
    }
}
