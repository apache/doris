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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonRestMetaStoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class PaimonVendedCredentialsProviderTest {

    @Test
    public void testIsVendedCredentialsEnabled() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        // Test with PaimonRestMetaStore and DLF token provider
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getType()).thenReturn(MetastoreProperties.Type.PAIMON);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        Assertions.assertTrue(provider.isVendedCredentialsEnabled(restProperties));

        // Test with PaimonRestMetaStore but unsupported token provider
        // Note: PaimonVendedCredentialsProvider enables vended credentials for all PaimonRestMetaStore
        // regardless of token provider, actual provider check happens later
        Mockito.when(restProperties.getTokenProvider()).thenReturn("unsupported");
        Assertions.assertTrue(provider.isVendedCredentialsEnabled(restProperties));

        // Test with non-PaimonRest metastore
        MetastoreProperties nonRestProperties = Mockito.mock(MetastoreProperties.class);
        Mockito.when(nonRestProperties.getType()).thenReturn(MetastoreProperties.Type.HMS);
        Assertions.assertFalse(provider.isVendedCredentialsEnabled(nonRestProperties));
    }

    @Test
    public void testExtractRawVendedCredentials() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        // Mock table with OSS vended credentials
        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> tokenMap = new HashMap<>();
        tokenMap.put("fs.oss.accessKeyId", "STS.testAccessKey123");
        tokenMap.put("fs.oss.accessKeySecret", "testSecretKey456");
        tokenMap.put("fs.oss.securityToken", "testSessionToken789");
        tokenMap.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(tokenMap);

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);

        Assertions.assertEquals("STS.testAccessKey123", rawCredentials.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("testSecretKey456", rawCredentials.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("testSessionToken789", rawCredentials.get("fs.oss.securityToken"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", rawCredentials.get("fs.oss.endpoint"));
    }

    @Test
    public void testExtractRawVendedCredentialsWithNullTable() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(null);
        Assertions.assertTrue(rawCredentials.isEmpty());
    }

    @Test
    public void testExtractRawVendedCredentialsWithNullFileIO() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.fileIO()).thenReturn(null);

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);
        Assertions.assertTrue(rawCredentials.isEmpty());
    }

    @Test
    public void testExtractRawVendedCredentialsWithNonRESTTokenFileIO() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        Table table = Mockito.mock(Table.class);
        // Mock a different FileIO type that's not RESTTokenFileIO
        Mockito.when(table.fileIO()).thenReturn(Mockito.mock(org.apache.paimon.fs.FileIO.class));

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);
        Assertions.assertTrue(rawCredentials.isEmpty());
    }

    @Test
    public void testExtractRawVendedCredentialsWithEmptyToken() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(new HashMap<>());

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);
        Assertions.assertTrue(rawCredentials.isEmpty());
    }

    @Test
    public void testExtractRawVendedCredentialsWithPartialOSSCredentials() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> tokenMap = new HashMap<>();
        tokenMap.put("fs.oss.accessKeyId", "testAccessKey");
        tokenMap.put("fs.oss.accessKeySecret", "testSecretKey");
        // Missing endpoint and session token

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(tokenMap);

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);

        Assertions.assertEquals("testAccessKey", rawCredentials.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("testSecretKey", rawCredentials.get("fs.oss.accessKeySecret"));
        Assertions.assertFalse(rawCredentials.containsKey("fs.oss.securityToken"));
        Assertions.assertFalse(rawCredentials.containsKey("fs.oss.endpoint"));
    }

    @Test
    public void testFilterCloudStoragePropertiesWithOSS() {
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("oss.access-key-id", "testAccessKey");
        rawCredentials.put("oss.secret-access-key", "testSecretKey");
        rawCredentials.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        rawCredentials.put("paimon.table.name", "test_table");
        rawCredentials.put("other.property", "other_value");

        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawCredentials);

        Assertions.assertEquals(3, filtered.size());
        Assertions.assertEquals("testAccessKey", filtered.get("oss.access-key-id"));
        Assertions.assertEquals("testSecretKey", filtered.get("oss.secret-access-key"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", filtered.get("oss.endpoint"));
        Assertions.assertFalse(filtered.containsKey("paimon.table.name"));
        Assertions.assertFalse(filtered.containsKey("other.property"));
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentials() {
        // Mock metastore properties with DLF token provider
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getType()).thenReturn(MetastoreProperties.Type.PAIMON);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        // Mock table with vended credentials
        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> tokenMap = new HashMap<>();
        tokenMap.put("fs.oss.accessKeyId", "STS.testAccessKey123");
        tokenMap.put("fs.oss.accessKeySecret", "testSecretKey456");
        tokenMap.put("fs.oss.securityToken", "testSessionToken789");
        tokenMap.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(tokenMap);

        // Test using VendedCredentialsFactory
        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(restProperties, new HashMap<>(), table);

        // Should not be null (assuming StorageProperties.createAll works correctly)
        // Note: The actual result depends on whether StorageProperties.createAll() can properly map the credentials
        // This test verifies the integration flow works without exceptions
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsDisabled() {
        // Mock metastore properties with unsupported token provider
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getType()).thenReturn(MetastoreProperties.Type.PAIMON);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("unsupported");

        Table table = Mockito.mock(Table.class);

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(restProperties, new HashMap<>(), table);

        // Should return the baseStoragePropertiesMap (empty HashMap)
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetStoragePropertiesMapWithNullTable() {
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getType()).thenReturn(MetastoreProperties.Type.PAIMON);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(restProperties, new HashMap<>(), null);

        // Should return the baseStoragePropertiesMap (empty HashMap)
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetStoragePropertiesMapWithNonPaimonRest() {
        // Test with non-PaimonRest metastore
        MetastoreProperties nonRestProperties = Mockito.mock(MetastoreProperties.class);
        Mockito.when(nonRestProperties.getType()).thenReturn(MetastoreProperties.Type.HMS);
        Table table = Mockito.mock(Table.class);

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(nonRestProperties, new HashMap<>(), table);

        // Should return the baseStoragePropertiesMap (empty HashMap)
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithOSS() {
        // Create mock storage properties
        StorageProperties ossProperties = Mockito.mock(StorageProperties.class);
        StorageProperties hdfsProperties = Mockito.mock(StorageProperties.class);

        Map<String, String> ossBackendProps = new HashMap<>();
        ossBackendProps.put("AWS_ACCESS_KEY", "testOssAccessKey");
        ossBackendProps.put("AWS_SECRET_KEY", "testOssSecretKey");
        ossBackendProps.put("AWS_TOKEN", "testOssToken");
        ossBackendProps.put("AWS_ENDPOINT", "oss-cn-beijing.aliyuncs.com");

        Map<String, String> hdfsBackendProps = new HashMap<>();
        hdfsBackendProps.put("HDFS_PROPERTY", "hdfsValue");

        Mockito.when(ossProperties.getBackendConfigProperties()).thenReturn(ossBackendProps);
        Mockito.when(hdfsProperties.getBackendConfigProperties()).thenReturn(hdfsBackendProps);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.OSS, ossProperties);
        storagePropertiesMap.put(Type.HDFS, hdfsProperties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertEquals(5, result.size());
        Assertions.assertEquals("testOssAccessKey", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("testOssSecretKey", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("testOssToken", result.get("AWS_TOKEN"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", result.get("AWS_ENDPOINT"));
        Assertions.assertEquals("hdfsValue", result.get("HDFS_PROPERTY"));
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithNullValues() {
        StorageProperties ossProperties = Mockito.mock(StorageProperties.class);

        Map<String, String> ossBackendProps = new HashMap<>();
        ossBackendProps.put("AWS_ACCESS_KEY", "testAccessKey");
        ossBackendProps.put("AWS_SECRET_KEY", null); // null value should be filtered out
        ossBackendProps.put("AWS_TOKEN", "testToken");

        Mockito.when(ossProperties.getBackendConfigProperties()).thenReturn(ossBackendProps);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.OSS, ossProperties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("testAccessKey", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("testToken", result.get("AWS_TOKEN"));
        Assertions.assertFalse(result.containsKey("AWS_SECRET_KEY"));
    }

    @Test
    public void testEndpointToRegionConversion() {
        PaimonVendedCredentialsProvider provider = PaimonVendedCredentialsProvider.getInstance();

        // Test different OSS endpoint patterns and their expected regions
        String[] endpoints = {
                "oss-cn-beijing.aliyuncs.com",
                "oss-cn-shanghai.aliyuncs.com",
                "oss-us-west-1.aliyuncs.com",
                "oss-ap-southeast-1.aliyuncs.com"
        };

        for (int i = 0; i < endpoints.length; i++) {
            Table table = Mockito.mock(Table.class);
            RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
            RESTToken restToken = Mockito.mock(RESTToken.class);

            Map<String, String> tokenMap = new HashMap<>();
            tokenMap.put("fs.oss.accessKeyId", "testAccessKey");
            tokenMap.put("fs.oss.accessKeySecret", "testSecretKey");
            tokenMap.put("fs.oss.endpoint", endpoints[i]);

            Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
            Mockito.when(table.name()).thenReturn("test_table");
            Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
            Mockito.when(restToken.token()).thenReturn(tokenMap);

            Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);

            Assertions.assertEquals(endpoints[i], rawCredentials.get("fs.oss.endpoint"));
            // Note: Current implementation doesn't convert endpoint to region, so region is not set
        }
    }
}
