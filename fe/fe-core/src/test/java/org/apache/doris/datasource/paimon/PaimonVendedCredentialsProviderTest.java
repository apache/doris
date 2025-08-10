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
    public void testOssCredentialExtractorWithValidCredentials() {
        PaimonOssCredentialExtractor extractor = new PaimonOssCredentialExtractor();

        Map<String, String> properties = new HashMap<>();
        properties.put("fs.oss.accessKeyId", "test-access-key");
        properties.put("fs.oss.accessKeySecret", "test-secret-key");
        properties.put("fs.oss.securityToken", "test-session-token");
        properties.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");

        Map<String, String> credentials = extractor.extractCredentials(properties);

        Assertions.assertEquals("test-access-key", credentials.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("test-secret-key", credentials.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("test-session-token", credentials.get("AWS_TOKEN"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", credentials.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-beijing", credentials.get("AWS_REGION"));
    }

    @Test
    public void testOssCredentialExtractorWithPartialCredentials() {
        PaimonOssCredentialExtractor extractor = new PaimonOssCredentialExtractor();

        Map<String, String> properties = new HashMap<>();
        properties.put("fs.oss.accessKeyId", "test-access-key");
        properties.put("fs.oss.accessKeySecret", "test-secret-key");
        // No session token and endpoint

        Map<String, String> credentials = extractor.extractCredentials(properties);

        Assertions.assertEquals("test-access-key", credentials.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("test-secret-key", credentials.get("AWS_SECRET_KEY"));
        Assertions.assertFalse(credentials.containsKey("AWS_TOKEN"));
        Assertions.assertFalse(credentials.containsKey("AWS_ENDPOINT"));
        Assertions.assertFalse(credentials.containsKey("AWS_REGION"));
    }

    @Test
    public void testOssCredentialExtractorWithEmptyProperties() {
        PaimonOssCredentialExtractor extractor = new PaimonOssCredentialExtractor();

        Map<String, String> credentials = extractor.extractCredentials(new HashMap<>());
        Assertions.assertTrue(credentials.isEmpty());

        credentials = extractor.extractCredentials(null);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testOssCredentialExtractorWithNonOssProperties() {
        PaimonOssCredentialExtractor extractor = new PaimonOssCredentialExtractor();

        Map<String, String> properties = new HashMap<>();
        properties.put("some.other.property", "value");
        properties.put("unrelated.key", "unrelated-value");

        Map<String, String> credentials = extractor.extractCredentials(properties);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testOssCredentialExtractorWithDifferentEndpoints() {
        PaimonOssCredentialExtractor extractor = new PaimonOssCredentialExtractor();

        // Test different OSS endpoint patterns
        String[] endpoints = {
            "oss-cn-shanghai.aliyuncs.com",
            "oss-us-west-1.aliyuncs.com",
            "oss-ap-southeast-1.aliyuncs.com"
        };
        String[] expectedRegions = {
            "cn-shanghai",
            "us-west-1",
            "ap-southeast-1"
        };

        for (int i = 0; i < endpoints.length; i++) {
            Map<String, String> properties = new HashMap<>();
            properties.put("fs.oss.accessKeyId", "test-access-key");
            properties.put("fs.oss.accessKeySecret", "test-secret-key");
            properties.put("fs.oss.endpoint", endpoints[i]);

            Map<String, String> credentials = extractor.extractCredentials(properties);

            Assertions.assertEquals("test-access-key", credentials.get("AWS_ACCESS_KEY"));
            Assertions.assertEquals("test-secret-key", credentials.get("AWS_SECRET_KEY"));
            Assertions.assertEquals(endpoints[i], credentials.get("AWS_ENDPOINT"));
            Assertions.assertEquals(expectedRegions[i], credentials.get("AWS_REGION"));
        }
    }

    @Test
    public void testExtractVendedCredentialsFromTableWithDlfTokenProvider() {
        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> tokenMap = new HashMap<>();
        tokenMap.put("fs.oss.accessKeyId", "dlf-access-key");
        tokenMap.put("fs.oss.accessKeySecret", "dlf-secret-key");
        tokenMap.put("fs.oss.securityToken", "dlf-session-token");
        tokenMap.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(tokenMap);

        Map<String, String> credentials = PaimonVendedCredentialsProvider
                .extractVendedCredentialsFromTable("dlf", table);

        Assertions.assertEquals("dlf-access-key", credentials.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("dlf-secret-key", credentials.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("dlf-session-token", credentials.get("AWS_TOKEN"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", credentials.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-beijing", credentials.get("AWS_REGION"));
    }

    @Test
    public void testExtractVendedCredentialsFromTableWithUnsupportedTokenProvider() {
        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> tokenMap = new HashMap<>();
        tokenMap.put("fs.oss.accessKeyId", "unsupported-access-key");
        tokenMap.put("fs.oss.accessKeySecret", "unsupported-secret-key");

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(tokenMap);

        Map<String, String> credentials = PaimonVendedCredentialsProvider
                .extractVendedCredentialsFromTable("unsupported", table);

        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testExtractVendedCredentialsFromTableWithNullTable() {
        Map<String, String> credentials = PaimonVendedCredentialsProvider
                .extractVendedCredentialsFromTable("dlf", null);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testExtractVendedCredentialsFromTableWithNullFileIO() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.fileIO()).thenReturn(null);

        Map<String, String> credentials = PaimonVendedCredentialsProvider
                .extractVendedCredentialsFromTable("dlf", table);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testExtractVendedCredentialsFromTableWithNonRESTTokenFileIO() {
        Table table = Mockito.mock(Table.class);
        // fileIO returns null, which is not an instance of RESTTokenFileIO
        Mockito.when(table.fileIO()).thenReturn(null);
        Mockito.when(table.name()).thenReturn("test_table");

        Map<String, String> credentials = PaimonVendedCredentialsProvider
                .extractVendedCredentialsFromTable("dlf", table);

        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testGetBackendLocationPropertiesWithPaimonRestMetaStore() {
        // Mock PaimonRestMetaStoreProperties
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        // Mock storage properties
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.put("base.property", "base-value");

        Map<String, String> vendedProperties = new HashMap<>();
        vendedProperties.put("base.property", "base-value");
        vendedProperties.put("AWS_ACCESS_KEY", "vended-access-key");
        vendedProperties.put("AWS_SECRET_KEY", "vended-secret-key");
        vendedProperties.put("AWS_TOKEN", "vended-session-token");

        Mockito.when(storageProperties.getBackendConfigProperties()).thenReturn(baseProperties);
        Mockito.when(storageProperties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(vendedProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, storageProperties);

        // Mock table with vended credentials
        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> tokenMap = new HashMap<>();
        tokenMap.put("fs.oss.accessKeyId", "vended-access-key");
        tokenMap.put("fs.oss.accessKeySecret", "vended-secret-key");
        tokenMap.put("fs.oss.securityToken", "vended-session-token");

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(tokenMap);

        // Test with PaimonRestMetaStoreProperties
        Map<String, String> result = PaimonVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertEquals("vended-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("vended-secret-key", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("vended-session-token", result.get("AWS_TOKEN"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithNonPaimonRestMetaStore() {
        // Mock non-PaimonRest metastore properties
        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);

        // Mock storage properties
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.put("base.property", "base-value");
        baseProperties.put("AWS_ACCESS_KEY", "static-access-key");

        Mockito.when(storageProperties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(baseProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, storageProperties);

        // Mock table (should be ignored for non-REST catalogs)
        Table table = Mockito.mock(Table.class);

        // Test with non-PaimonRest metastore
        Map<String, String> result = PaimonVendedCredentialsProvider.getBackendLocationProperties(
                metastoreProperties, storagePropertiesMap, table);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertEquals("static-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertFalse(result.containsKey("AWS_TOKEN"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithMultipleStorageTypes() {
        // Mock PaimonRestMetaStoreProperties
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        // Mock S3 storage properties
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);
        Map<String, String> s3VendedProperties = new HashMap<>();
        s3VendedProperties.put("s3.property", "s3-value");
        s3VendedProperties.put("AWS_ACCESS_KEY", "vended-access-key");

        Mockito.when(s3Properties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(s3VendedProperties);

        // Mock HDFS storage properties
        StorageProperties hdfsStorageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> hdfsPropertiesMap = new HashMap<>();
        hdfsPropertiesMap.put("hdfs.property", "hdfs-value");

        Mockito.when(hdfsStorageProperties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(hdfsPropertiesMap);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);
        storagePropertiesMap.put(Type.HDFS, hdfsStorageProperties);

        // Mock table with vended credentials
        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> tokenMap = new HashMap<>();
        tokenMap.put("fs.oss.accessKeyId", "vended-access-key");

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(tokenMap);

        // Test with multiple storage types
        Map<String, String> result = PaimonVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertEquals("s3-value", result.get("s3.property"));
        Assertions.assertEquals("hdfs-value", result.get("hdfs.property"));
        Assertions.assertEquals("vended-access-key", result.get("AWS_ACCESS_KEY"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithNullTable() {
        // Mock PaimonRestMetaStoreProperties
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        // Mock storage properties
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.put("base.property", "base-value");

        Mockito.when(storageProperties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(baseProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, storageProperties);

        // Test with null table (should fall back to base properties without vended credentials)
        Map<String, String> result = PaimonVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, null);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertFalse(result.containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithEmptyStorageMap() {
        // Mock PaimonRestMetaStoreProperties
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        // Empty storage properties map
        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();

        // Mock table
        Table table = Mockito.mock(Table.class);

        // Test with empty storage map
        Map<String, String> result = PaimonVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetBackendLocationPropertiesWithEmptyTokenMap() {
        // Mock PaimonRestMetaStoreProperties
        PaimonRestMetaStoreProperties restProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(restProperties.getTokenProvider()).thenReturn("dlf");

        // Mock storage properties
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.put("base.property", "base-value");

        Mockito.when(storageProperties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(baseProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, storageProperties);

        // Mock table with empty token map
        Table table = Mockito.mock(Table.class);
        RESTTokenFileIO restTokenFileIO = Mockito.mock(RESTTokenFileIO.class);
        RESTToken restToken = Mockito.mock(RESTToken.class);

        Map<String, String> emptyTokenMap = new HashMap<>();

        Mockito.when(table.fileIO()).thenReturn(restTokenFileIO);
        Mockito.when(table.name()).thenReturn("test_table");
        Mockito.when(restTokenFileIO.validToken()).thenReturn(restToken);
        Mockito.when(restToken.token()).thenReturn(emptyTokenMap);

        // Test with empty token map (should fall back to base properties without vended credentials)
        Map<String, String> result = PaimonVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertFalse(result.containsKey("AWS_ACCESS_KEY"));
    }
}
