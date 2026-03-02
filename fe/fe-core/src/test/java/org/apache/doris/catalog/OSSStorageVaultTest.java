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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for OSSStorageVault.checkCreationProperties() validation logic.
 *
 * Mirrors the validation test patterns used in S3StorageVault tests.
 * Each test targets one specific validation rule in checkCreationProperties().
 */
public class OSSStorageVaultTest {

    // ---------------------------------------------------------------------------
    // Helper: build a minimal valid property map so individual tests can remove
    // or override only the property they care about.
    // ---------------------------------------------------------------------------
    private static Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(OSSStorageVault.PropertyKey.ENDPOINT, "oss-cn-hangzhou.aliyuncs.com");
        props.put(OSSStorageVault.PropertyKey.BUCKET, "my-doris-bucket");
        props.put(OSSStorageVault.PropertyKey.ROOT_PATH, "doris/data");
        // AK/SK absent → ECS instance profile path (both absent is valid)
        return props;
    }

    private static OSSStorageVault newVault() throws DdlException {
        // OSSStorageVault.checkCreationProperties is the method under test.
        // We call it directly without constructing a full vault object to
        // keep tests lightweight and independent of catalog infrastructure.
        return null; // placeholder — tests call checkCreationProperties directly
    }

    // Thin wrapper so tests read cleanly.
    private static void check(Map<String, String> props) throws UserException {
        // Re-use a minimal stub: we only need the validation logic.
        // Reflection-free: instantiate via anonymous subclass if needed,
        // or call the static helper if one is added. For now, call directly.
        new OSSStorageVaultValidator().checkCreationProperties(props);
    }

    // ---------------------------------------------------------------------------
    // 1. Endpoint validation
    // ---------------------------------------------------------------------------

    @Test
    public void testMissingEndpointThrows() {
        Map<String, String> props = validProps();
        props.remove(OSSStorageVault.PropertyKey.ENDPOINT);
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Missing endpoint must throw DdlException");
    }

    @Test
    public void testBlankEndpointThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ENDPOINT, "   ");
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Blank endpoint must throw DdlException");
    }

    @Test
    public void testValidStandardEndpointAccepted() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ENDPOINT, "oss-cn-beijing.aliyuncs.com");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    @Test
    public void testValidInternalEndpointAccepted() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ENDPOINT, "oss-cn-shanghai-internal.aliyuncs.com");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    @Test
    public void testCustomEndpointWithExplicitRegionAccepted() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ENDPOINT, "custom.storage.example.com");
        props.put(OSSStorageVault.PropertyKey.REGION, "cn-hangzhou");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    @Test
    public void testCustomEndpointWithoutRegionThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ENDPOINT, "custom.storage.example.com");
        props.remove(OSSStorageVault.PropertyKey.REGION);
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Custom endpoint without region must throw — region cannot be inferred");
    }

    // ---------------------------------------------------------------------------
    // 2. Bucket validation
    // ---------------------------------------------------------------------------

    @Test
    public void testMissingBucketThrows() {
        Map<String, String> props = validProps();
        props.remove(OSSStorageVault.PropertyKey.BUCKET);
        Assertions.assertThrows(DdlException.class, () -> check(props));
    }

    @Test
    public void testBlankBucketThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.BUCKET, "");
        Assertions.assertThrows(DdlException.class, () -> check(props));
    }

    // ---------------------------------------------------------------------------
    // 3. Root path validation
    // ---------------------------------------------------------------------------

    @Test
    public void testMissingRootPathThrows() {
        Map<String, String> props = validProps();
        props.remove(OSSStorageVault.PropertyKey.ROOT_PATH);
        Assertions.assertThrows(DdlException.class, () -> check(props));
    }

    @Test
    public void testRootPathStartingWithSlashThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ROOT_PATH, "/doris/data");
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Root path starting with '/' must be rejected");
    }

    @Test
    public void testRootPathEndingWithSlashThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ROOT_PATH, "doris/data/");
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Root path ending with '/' must be rejected");
    }

    @Test
    public void testValidRootPathAccepted() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ROOT_PATH, "doris/warehouse/prod");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    // ---------------------------------------------------------------------------
    // 4. AK/SK pairing validation
    // ---------------------------------------------------------------------------

    @Test
    public void testAKWithoutSKThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ACCESS_KEY, "myAccessKey");
        // secret_key intentionally absent
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Providing access_key without secret_key must throw");
    }

    @Test
    public void testSKWithoutAKThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.SECRET_KEY, "mySecretKey");
        // access_key intentionally absent
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Providing secret_key without access_key must throw");
    }

    @Test
    public void testBothAKSKAccepted() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ACCESS_KEY, "myAccessKey");
        props.put(OSSStorageVault.PropertyKey.SECRET_KEY, "mySecretKey");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    @Test
    public void testNeitherAKNorSKAccepted() {
        // Instance profile mode: no AK/SK is valid
        Map<String, String> props = validProps();
        Assertions.assertDoesNotThrow(() -> check(props),
                "Omitting both AK and SK must be accepted (ECS instance profile)");
    }

    // ---------------------------------------------------------------------------
    // 5. AssumeRole validation
    // ---------------------------------------------------------------------------

    @Test
    public void testRoleArnWithoutAKSKAccepted() {
        // AssumeRole using instance profile as base — no AK/SK needed
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ROLE_ARN,
                "acs:ram::5503360369842933:role/ecs-oss-access-role");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    @Test
    public void testRoleArnWithExternalIdAccepted() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ROLE_ARN,
                "acs:ram::5503360369842933:role/cross-account-role");
        props.put(OSSStorageVault.PropertyKey.EXTERNAL_ID, "partner-external-id-12345");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    @Test
    public void testRoleArnWithAKSKAccepted() {
        // AK/SK provided alongside role_arn — explicitly allowed
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.ACCESS_KEY, "myAccessKey");
        props.put(OSSStorageVault.PropertyKey.SECRET_KEY, "mySecretKey");
        props.put(OSSStorageVault.PropertyKey.ROLE_ARN,
                "acs:ram::5503360369842933:role/ecs-oss-access-role");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    // ---------------------------------------------------------------------------
    // 6. Connection settings validation
    // ---------------------------------------------------------------------------

    @Test
    public void testNegativeRequestTimeoutThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.REQUEST_TIMEOUT_MS, "-1");
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Negative request timeout must throw");
    }

    @Test
    public void testZeroConnectionTimeoutThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.CONNECTION_TIMEOUT_MS, "0");
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Zero connection timeout must throw");
    }

    @Test
    public void testNonNumericMaxConnectionsThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.MAX_CONNECTIONS, "abc");
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Non-numeric max_connections must throw");
    }

    @Test
    public void testZeroMaxConnectionsThrows() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.MAX_CONNECTIONS, "0");
        Assertions.assertThrows(DdlException.class, () -> check(props),
                "Zero max_connections must throw");
    }

    @Test
    public void testValidConnectionSettingsAccepted() {
        Map<String, String> props = validProps();
        props.put(OSSStorageVault.PropertyKey.REQUEST_TIMEOUT_MS, "30000");
        props.put(OSSStorageVault.PropertyKey.CONNECTION_TIMEOUT_MS, "10000");
        props.put(OSSStorageVault.PropertyKey.MAX_CONNECTIONS, "100");
        Assertions.assertDoesNotThrow(() -> check(props));
    }

    // ---------------------------------------------------------------------------
    // 7. ALLOW_ALTER_PROPERTIES contract
    // ---------------------------------------------------------------------------

    @Test
    public void testAllowAlterPropertiesContainsExpectedKeys() {
        // Verify the alter-allowed set matches the documented contract.
        // Immutable fields (endpoint, bucket, type) must NOT be alterable.
        Assertions.assertFalse(OSSStorageVault.ALLOW_ALTER_PROPERTIES.contains(
                OSSStorageVault.PropertyKey.ENDPOINT),
                "endpoint must not be alterable");
        Assertions.assertFalse(OSSStorageVault.ALLOW_ALTER_PROPERTIES.contains(
                OSSStorageVault.PropertyKey.BUCKET),
                "bucket must not be alterable");

        // Mutable fields must be in the set
        Assertions.assertTrue(OSSStorageVault.ALLOW_ALTER_PROPERTIES.contains(
                OSSStorageVault.PropertyKey.ACCESS_KEY));
        Assertions.assertTrue(OSSStorageVault.ALLOW_ALTER_PROPERTIES.contains(
                OSSStorageVault.PropertyKey.SECRET_KEY));
        Assertions.assertTrue(OSSStorageVault.ALLOW_ALTER_PROPERTIES.contains(
                OSSStorageVault.PropertyKey.ROLE_ARN));
        Assertions.assertTrue(OSSStorageVault.ALLOW_ALTER_PROPERTIES.contains(
                OSSStorageVault.PropertyKey.EXTERNAL_ID));
        Assertions.assertTrue(OSSStorageVault.ALLOW_ALTER_PROPERTIES.contains(
                OSSStorageVault.PropertyKey.MAX_CONNECTIONS));
    }

    // ---------------------------------------------------------------------------
    // Inner validator stub — calls checkCreationProperties directly.
    // Avoids constructing a full OSSStorageVault (which requires catalog context).
    // ---------------------------------------------------------------------------
    private static class OSSStorageVaultValidator extends OSSStorageVault {
        OSSStorageVaultValidator() throws DdlException {
            super(/* name= */ "_test_vault_",
                  /* ifNotExists= */ false,
                  /* setAsDefault= */ false,
                  /* stmt= */ null);
        }
    }
}
