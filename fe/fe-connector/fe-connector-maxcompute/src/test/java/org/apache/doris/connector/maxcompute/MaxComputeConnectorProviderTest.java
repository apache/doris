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

import org.apache.doris.connector.api.ConnectorTestResult;

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

    // --- 6. split-byte-size error message names the byte-size property, not row-count ---
    // Migrated from MaxComputeExternalCatalogTest.testSplitByteSizeErrorMessage (PR
    // apache/doris#64119), which fixed a copy-paste that printed SPLIT_ROW_COUNT in the
    // SPLIT_BYTE_SIZE floor error. This fork was already correct (G6); the test pins it.

    @Test
    public void testSplitByteSizeErrorMessageNamesByteSizeNotRowCount() {
        Map<String, String> props = validProps();
        props.put(MCConnectorProperties.SPLIT_STRATEGY,
                MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY);
        props.put(MCConnectorProperties.SPLIT_BYTE_SIZE, "1048576");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> provider.validateProperties(props));
        Assertions.assertTrue(ex.getMessage().contains(MCConnectorProperties.SPLIT_BYTE_SIZE),
                "got: " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains(MCConnectorProperties.SPLIT_ROW_COUNT),
                "got: " + ex.getMessage());
    }

    // --- 7. CREATE CATALOG connectivity test (test_connection) — the FE->ODPS half of catalog
    // validation, complementing the property half above. Migrated from
    // MaxComputeExternalCatalogTest.testCheckWhenCreating* (PR apache/doris#64119): the legacy
    // MaxComputeExternalCatalog.checkWhenCreating override is now MaxComputeDorisConnector
    // .testConnection(), wired by PluginDrivenExternalCatalog.checkWhenCreating (TEST_CONNECTION
    // gate -> testConnection -> DdlException on failure). The two ODPS calls (project-exists /
    // namespace-schema-list) are overridden so the tests run offline with no Mockito, mirroring the
    // PR's TestMaxComputeExternalCatalog seam subclass. ---

    @Test
    public void testMaxComputeDoesNotForceConnectivityTestByDefault() {
        // PR testCheckWhenCreatingSkipsValidationByDefault: MaxCompute leaves test_connection off by
        // default, so PluginDrivenExternalCatalog.checkWhenCreating skips testConnection entirely.
        Assertions.assertFalse(
                new MaxComputeDorisConnector(connectivityProps(true), null).defaultTestConnection());
    }

    @Test
    public void testConnectionValidatesProjectWhenNamespaceSchemaDisabled() {
        TestMaxComputeDorisConnector connector =
                new TestMaxComputeDorisConnector(connectivityProps(false));
        ConnectorTestResult result = connector.testConnection(null);
        Assertions.assertTrue(result.isSuccess(), "got: " + result.getMessage());
        Assertions.assertEquals("mc_project", connector.checkedProjectName);
        Assertions.assertNull(connector.checkedNamespaceSchemaProjectName);
    }

    @Test
    public void testConnectionValidatesSchemaWhenNamespaceSchemaEnabled() {
        TestMaxComputeDorisConnector connector =
                new TestMaxComputeDorisConnector(connectivityProps(true));
        ConnectorTestResult result = connector.testConnection(null);
        Assertions.assertTrue(result.isSuccess(), "got: " + result.getMessage());
        Assertions.assertEquals("mc_project", connector.checkedNamespaceSchemaProjectName);
        Assertions.assertNull(connector.checkedProjectName);
    }

    @Test
    public void testConnectionReportsInaccessibleProject() {
        TestMaxComputeDorisConnector connector =
                new TestMaxComputeDorisConnector(connectivityProps(false));
        connector.projectExists = false;
        ConnectorTestResult result = connector.testConnection(null);
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertTrue(
                result.getMessage().contains("Failed to validate MaxCompute project 'mc_project'"),
                "got: " + result.getMessage());
        Assertions.assertTrue(
                result.getMessage().contains("does not exist or is not accessible"),
                "got: " + result.getMessage());
        Assertions.assertNull(connector.checkedNamespaceSchemaProjectName);
    }

    @Test
    public void testConnectionReportsInaccessibleNamespaceSchema() {
        TestMaxComputeDorisConnector connector =
                new TestMaxComputeDorisConnector(connectivityProps(true));
        connector.threeTierModel = false;
        ConnectorTestResult result = connector.testConnection(null);
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertTrue(
                result.getMessage().contains("Failed to validate MaxCompute project 'mc_project'"),
                "got: " + result.getMessage());
        Assertions.assertTrue(
                result.getMessage().contains("schema list is accessible"),
                "got: " + result.getMessage());
    }

    private static Map<String, String> connectivityProps(boolean enableNamespaceSchema) {
        Map<String, String> props = new HashMap<>();
        props.put(MCConnectorProperties.PROJECT, "mc_project");
        props.put(MCConnectorProperties.ENDPOINT,
                "http://service.cn-beijing.maxcompute.aliyun-inc.com/api");
        props.put(MCConnectorProperties.ACCESS_KEY, "access_key");
        props.put(MCConnectorProperties.SECRET_KEY, "secret_key");
        props.put(MCConnectorProperties.ENABLE_NAMESPACE_SCHEMA,
                Boolean.toString(enableNamespaceSchema));
        return props;
    }

    /**
     * Overrides the two ODPS-touching seams so the connectivity test runs offline, mirroring the
     * PR's {@code TestMaxComputeExternalCatalog}. {@code projectExists}/{@code threeTierModel} drive
     * the simulated remote state; {@code checked*ProjectName} record which validation path ran.
     */
    private static final class TestMaxComputeDorisConnector extends MaxComputeDorisConnector {
        private boolean projectExists = true;
        private boolean threeTierModel = true;
        private String checkedProjectName;
        private String checkedNamespaceSchemaProjectName;

        private TestMaxComputeDorisConnector(Map<String, String> props) {
            super(props, null);
        }

        @Override
        protected boolean maxComputeProjectExists(String projectName) {
            checkedProjectName = projectName;
            return projectExists;
        }

        @Override
        protected void validateMaxComputeNamespaceSchemaAccess(String projectName) {
            checkedNamespaceSchemaProjectName = projectName;
            if (!threeTierModel) {
                throw new RuntimeException("schema list is not accessible");
            }
        }
    }
}
