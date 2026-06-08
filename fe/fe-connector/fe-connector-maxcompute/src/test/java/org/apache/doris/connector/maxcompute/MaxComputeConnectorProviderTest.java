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

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link MaxComputeConnectorProvider#validateProperties(Map)}.
 *
 * <p>CREATE CATALOG must fail-fast on invalid MaxCompute properties, mirroring the
 * legacy {@code MaxComputeExternalCatalog.checkProperties}. Without this validation
 * the new SPI path degrades to use-time-late failures or silently accepts illegal
 * values (e.g. account_format='foo' coerced to DISPLAYNAME, negative timeouts), so
 * each case below pins one legacy validation branch.
 */
public class MaxComputeConnectorProviderTest {

    private final MaxComputeConnectorProvider provider = new MaxComputeConnectorProvider();

    private Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(MCConnectorProperties.PROJECT, "my_project");
        props.put(MCConnectorProperties.ENDPOINT,
                "http://service.cn-beijing.maxcompute.aliyun-inc.com/api");
        // Default auth type is ak_sk; provide the keys so the minimal config is valid.
        props.put(MCConnectorProperties.ACCESS_KEY, "ak");
        props.put(MCConnectorProperties.SECRET_KEY, "sk");
        return props;
    }

    @Test
    public void testValidPropertiesPass() {
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(validProps()));
    }

    // --- 1. required PROJECT / ENDPOINT ---

    @Test
    public void testMissingProject() {
        Map<String, String> props = validProps();
        props.remove(MCConnectorProperties.PROJECT);
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains(MCConnectorProperties.PROJECT));
    }

    @Test
    public void testMissingEndpoint() {
        Map<String, String> props = validProps();
        props.remove(MCConnectorProperties.ENDPOINT);
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains(MCConnectorProperties.ENDPOINT));
    }

    // --- 2. split strategy + size/count floor ---

    @Test
    public void testSplitByteSizeBelowFloor() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.SPLIT_BYTE_SIZE, "10485759");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("10485760"));
    }

    @Test
    public void testSplitByteSizeAtFloorPasses() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.SPLIT_BYTE_SIZE, "10485760");
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }

    @Test
    public void testSplitByteSizeNotInteger() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.SPLIT_BYTE_SIZE, "abc");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("must be an integer"));
    }

    @Test
    public void testSplitStrategyInvalid() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.SPLIT_STRATEGY, "foo");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains(
                MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY));
    }

    @Test
    public void testSplitRowCountZero() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.SPLIT_STRATEGY,
                MCConnectorProperties.SPLIT_BY_ROW_COUNT_STRATEGY);
        props.put(MCConnectorProperties.SPLIT_ROW_COUNT, "0");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("greater than 0"));
    }

    @Test
    public void testSplitRowCountStrategyValid() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.SPLIT_STRATEGY,
                MCConnectorProperties.SPLIT_BY_ROW_COUNT_STRATEGY);
        props.put(MCConnectorProperties.SPLIT_ROW_COUNT, "100000");
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }

    // --- 3. account_format enum ---

    @Test
    public void testAccountFormatInvalid() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.ACCOUNT_FORMAT, "foo");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("only support name and id"));
    }

    @Test
    public void testAccountFormatIdPasses() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.ACCOUNT_FORMAT, MCConnectorProperties.ACCOUNT_FORMAT_ID);
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }

    @Test
    public void testAccountFormatNamePasses() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.ACCOUNT_FORMAT, MCConnectorProperties.ACCOUNT_FORMAT_NAME);
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(props));
    }

    // --- 4. positive connect/read timeout + retry count ---

    @Test
    public void testConnectTimeoutZero() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.CONNECT_TIMEOUT, "0");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains(MCConnectorProperties.CONNECT_TIMEOUT));
        Assertions.assertTrue(ex.getMessage().contains("greater than 0"));
    }

    @Test
    public void testConnectTimeoutNegative() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.CONNECT_TIMEOUT, "-1");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
    }

    @Test
    public void testReadTimeoutNotInteger() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.READ_TIMEOUT, "abc");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("must be an integer"));
    }

    @Test
    public void testRetryCountZero() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.RETRY_COUNT, "0");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains(MCConnectorProperties.RETRY_COUNT));
    }

    // --- 5. auth completeness (wires the previously-dead checkAuthProperties,
    //        and verifies its exception type is now IllegalArgumentException) ---

    @Test
    public void testAuthMissingSecretKey() {
        Map<String, String> props = validProps();
        props.remove(MCConnectorProperties.SECRET_KEY);
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("secret key"));
    }

    @Test
    public void testAuthRamRoleArnMissingRoleArn() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.AUTH_TYPE,
                MCConnectorProperties.AUTH_TYPE_RAM_ROLE_ARN);
        // has access/secret key but no ram_role_arn
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("role arn"));
    }

    @Test
    public void testAuthUnknownType() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.AUTH_TYPE, "no_such_auth");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains("Unsupported auth type"));
    }
}
