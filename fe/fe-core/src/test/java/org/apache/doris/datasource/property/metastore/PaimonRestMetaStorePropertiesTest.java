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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonRestMetaStorePropertiesTest {

    @Test
    public void testBasicRestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("warehouse", "catalog_name");
        props.put("paimon.rest.token.provider", "none");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps();

        Assertions.assertEquals(PaimonExternalCatalog.PAIMON_REST, restProps.getPaimonCatalogType());
        Assertions.assertEquals("rest", restProps.getMetastoreType());
    }

    @Test
    public void testUriAliases() {
        // Test different URI property names
        Map<String, String> props1 = new HashMap<>();
        props1.put("uri", "http://localhost:8080");
        props1.put("paimon.rest.token.provider", "none");
        props1.put("warehouse", "catalog_name");
        PaimonRestMetaStoreProperties restProps1 = new PaimonRestMetaStoreProperties(props1);
        restProps1.initNormalizeAndCheckProps();

        Map<String, String> props2 = new HashMap<>();
        props2.put("paimon.rest.uri", "http://localhost:8080");
        props2.put("paimon.rest.token.provider", "none");
        props2.put("warehouse", "catalog_name");
        PaimonRestMetaStoreProperties restProps2 = new PaimonRestMetaStoreProperties(props2);
        restProps2.initNormalizeAndCheckProps();

        // Both should work and set the same URI in catalog options
        List<StorageProperties> storagePropertiesList = new ArrayList<>();
        restProps1.buildCatalogOptions(storagePropertiesList);
        restProps2.buildCatalogOptions(storagePropertiesList);

        Options options1 = restProps1.getCatalogOptions();
        Options options2 = restProps2.getCatalogOptions();

        Assertions.assertEquals("http://localhost:8080", options1.get("uri"));
        Assertions.assertEquals("http://localhost:8080", options2.get("uri"));
    }

    @Test
    public void testPaimonRestPropertiesPassthrough() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.custom.property", "custom-value");
        props.put("paimon.rest.timeout", "30000");
        props.put("paimon.rest.retry.count", "3");
        props.put("paimon.rest.token.provider", "none");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps();

        List<StorageProperties> storagePropertiesList = new ArrayList<>();
        restProps.buildCatalogOptions(storagePropertiesList);
        Options catalogOptions = restProps.getCatalogOptions();

        // Basic URI should be set
        Assertions.assertEquals("http://localhost:8080", catalogOptions.get("uri"));

        // Custom paimon.rest.* properties should be passed through without prefix
        Assertions.assertEquals("custom-value", catalogOptions.get("custom.property"));
        Assertions.assertEquals("30000", catalogOptions.get("timeout"));
        Assertions.assertEquals("3", catalogOptions.get("retry.count"));
    }

    @Test
    public void testTokenProviderProperty() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "dlf");
        props.put("warehouse", "catalog_name");
        props.put("paimon.rest.dlf.access-key-id", "ak");
        props.put("paimon.rest.dlf.access-key-secret", "sk");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps();

        Assertions.assertEquals("dlf", restProps.getTokenProvider());
    }

    @Test
    public void testDlfTokenProviderValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "dlf");
        props.put("paimon.rest.dlf.access-key-id", "ak");
        props.put("paimon.rest.dlf.access-key-secret", "sk");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps(); // Should not throw

        Assertions.assertEquals("dlf", restProps.getTokenProvider());
    }

    @Test
    public void testDlfTokenProviderMissingAccessKeyId() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "dlf");
        props.put("paimon.rest.dlf.access-key-secret", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        // Missing access-key-id

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testDlfTokenProviderMissingSecretKey() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "dlf");
        props.put("paimon.rest.dlf.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        // Missing secret-access-key

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testDlfTokenProviderMissingBothCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "dlf");
        props.put("warehouse", "catalog_name");
        // Missing both credentials

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);

        // The error message should mention the required properties
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("access-key-id")
                && errorMessage.contains("access-key-secret"));
    }

    @Test
    public void testNonDlfTokenProviderDoesNotRequireCredentials() {
        // Test that non-dlf token providers don't require DLF credentials
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "custom-provider");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps(); // Should not throw

        Assertions.assertEquals("custom-provider", restProps.getTokenProvider());
    }

    @Test
    public void testNonDlfTokenProviderWithDlfCredentialsStillWorks() {
        // Test that non-dlf token provider doesn't require DLF credentials
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "other");
        props.put("paimon.rest.dlf.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("paimon.rest.dlf.access-key-secret", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps(); // Should not throw

        Assertions.assertEquals("other", restProps.getTokenProvider());
    }

    @Test
    public void testWarehouseProperty() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("warehouse", "s3://my-warehouse/path");
        props.put("paimon.rest.token.provider", "none");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps();

        // Warehouse property should be accessible through parent class
        Assertions.assertNotNull(restProps);
    }

    @Test
    public void testCaseInsensitiveDlfTokenProvider() {
        // Test that "DLF" is also recognized (case insensitive in validation)
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "DLF");
        props.put("paimon.rest.dlf.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("paimon.rest.dlf.access-key-secret", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps();

        Assertions.assertEquals("DLF", restProps.getTokenProvider());
    }

    @Test
    public void testMixedCaseTokenProvider() {
        // Test mixed case token provider
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "DlF");
        props.put("paimon.rest.dlf.access-key-id", "ak");
        props.put("paimon.rest.dlf.access-key-secret", "sk");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps();

        Assertions.assertEquals("DlF", restProps.getTokenProvider());
    }

    @Test
    public void testTokenProviderValidationLogic() {
        // Test: Token provider is required - should throw when missing
        Map<String, String> props1 = new HashMap<>();
        props1.put("paimon.rest.uri", "http://localhost:8080");

        PaimonRestMetaStoreProperties restProps1 = new PaimonRestMetaStoreProperties(props1);
        IllegalArgumentException exception1 = Assertions.assertThrows(
                IllegalArgumentException.class, restProps1::initNormalizeAndCheckProps);
        Assertions.assertTrue(exception1.getMessage().contains("paimon.rest.token.provider"));

        // Test: Token provider is required - should throw when empty
        Map<String, String> props2 = new HashMap<>();
        props2.put("paimon.rest.uri", "http://localhost:8080");
        props2.put("paimon.rest.token.provider", "");

        PaimonRestMetaStoreProperties restProps2 = new PaimonRestMetaStoreProperties(props2);
        IllegalArgumentException exception2 = Assertions.assertThrows(
                IllegalArgumentException.class, restProps2::initNormalizeAndCheckProps);
        Assertions.assertTrue(exception2.getMessage().contains("paimon.rest.token.provider"));

        // Test: Valid non-dlf token provider should work
        Map<String, String> props3 = new HashMap<>();
        props3.put("paimon.rest.uri", "http://localhost:8080");
        props3.put("paimon.rest.token.provider", "oauth2");
        props3.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps3 = new PaimonRestMetaStoreProperties(props3);
        restProps3.initNormalizeAndCheckProps(); // Should not throw
        Assertions.assertEquals("oauth2", restProps3.getTokenProvider());
    }

    @Test
    public void testDlfTokenProviderPositiveValidation() {
        // Test: DLF token provider with all required credentials should work
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "dlf");
        props.put("paimon.rest.dlf.access-key-id", "ak");
        props.put("paimon.rest.dlf.access-key-secret", "sk");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps(); // Should not throw

        Assertions.assertEquals("dlf", restProps.getTokenProvider());
    }

    @Test
    public void testDlfTokenProviderNegativeValidation() {
        // Test: DLF token provider missing access-key-id should throw
        Map<String, String> props1 = new HashMap<>();
        props1.put("paimon.rest.uri", "http://localhost:8080");
        props1.put("paimon.rest.token.provider", "dlf");
        props1.put("warehouse", "catalog_name");
        props1.put("paimon.rest.dlf.access-key-secret", "sk");
        // Missing access-key-id

        PaimonRestMetaStoreProperties restProps1 = new PaimonRestMetaStoreProperties(props1);
        IllegalArgumentException exception1 = Assertions.assertThrows(
                IllegalArgumentException.class, restProps1::initNormalizeAndCheckProps);
        String errorMessage1 = exception1.getMessage();
        Assertions.assertTrue(errorMessage1.contains("DLF token provider requires"));
        Assertions.assertTrue(errorMessage1.contains("access-key-id"));

        // Test: DLF token provider missing secret-access-key should throw
        Map<String, String> props2 = new HashMap<>();
        props2.put("paimon.rest.uri", "http://localhost:8080");
        props2.put("paimon.rest.token.provider", "dlf");
        props2.put("warehouse", "catalog_name");
        props2.put("paimon.rest.dlf.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        // Missing secret-access-key

        PaimonRestMetaStoreProperties restProps2 = new PaimonRestMetaStoreProperties(props2);
        IllegalArgumentException exception2 = Assertions.assertThrows(
                IllegalArgumentException.class, restProps2::initNormalizeAndCheckProps);
        String errorMessage2 = exception2.getMessage();
        Assertions.assertTrue(errorMessage2.contains("DLF token provider requires"));
        Assertions.assertTrue(errorMessage2.contains("access-key-secret"));

        // Test: DLF token provider missing both credentials should throw
        Map<String, String> props3 = new HashMap<>();
        props3.put("paimon.rest.uri", "http://localhost:8080");
        props3.put("paimon.rest.token.provider", "dlf");
        props3.put("warehouse", "catalog_name");
        // Missing both credentials

        PaimonRestMetaStoreProperties restProps3 = new PaimonRestMetaStoreProperties(props3);
        IllegalArgumentException exception3 = Assertions.assertThrows(
                IllegalArgumentException.class, restProps3::initNormalizeAndCheckProps);
        String errorMessage3 = exception3.getMessage();
        Assertions.assertTrue(errorMessage3.contains("DLF token provider requires"));
    }

    @Test
    public void testPaimonRestPropertiesWithMultipleCustomProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.custom.auth.token", "token123");
        props.put("paimon.rest.custom.header.x-api-key", "api-key-456");
        props.put("paimon.rest.custom.ssl.verify", "false");
        props.put("non.paimon.property", "should-not-be-included");
        props.put("paimon.rest.token.provider", "none");
        props.put("warehouse", "catalog_name");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        restProps.initNormalizeAndCheckProps();

        List<StorageProperties> storagePropertiesList = new ArrayList<>();
        restProps.buildCatalogOptions(storagePropertiesList);
        Options catalogOptions = restProps.getCatalogOptions();

        // paimon.rest.* properties should be passed through without prefix
        Assertions.assertEquals("token123", catalogOptions.get("custom.auth.token"));
        Assertions.assertEquals("api-key-456", catalogOptions.get("custom.header.x-api-key"));
        Assertions.assertEquals("false", catalogOptions.get("custom.ssl.verify"));

        // Non-paimon.rest properties should not be included
        Assertions.assertNull(catalogOptions.get("non.paimon.property"));
        Assertions.assertNull(catalogOptions.get("should-not-be-included"));
    }

    @Test
    public void testMissingTokenProviderThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        // Missing token provider

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);

        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("paimon.rest.token.provider"));
    }

    @Test
    public void testEmptyTokenProviderThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.rest.uri", "http://localhost:8080");
        props.put("paimon.rest.token.provider", "");

        PaimonRestMetaStoreProperties restProps = new PaimonRestMetaStoreProperties(props);
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);

        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("paimon.rest.token.provider"));
    }
}
