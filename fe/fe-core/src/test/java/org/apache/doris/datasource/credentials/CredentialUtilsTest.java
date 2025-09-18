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

package org.apache.doris.datasource.credentials;

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class CredentialUtilsTest {

    @Test
    public void testFilterCloudStoragePropertiesWithS3() {
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("s3.access-key-id", "testS3AccessKey");
        rawCredentials.put("s3.secret-access-key", "testS3SecretKey");
        rawCredentials.put("s3.region", "us-west-2");
        rawCredentials.put("s3.endpoint", "s3.amazonaws.com");
        rawCredentials.put("other.property", "other_value");
        rawCredentials.put("table.name", "test_table");

        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawCredentials);

        Assertions.assertEquals(4, filtered.size());
        Assertions.assertEquals("testS3AccessKey", filtered.get("s3.access-key-id"));
        Assertions.assertEquals("testS3SecretKey", filtered.get("s3.secret-access-key"));
        Assertions.assertEquals("us-west-2", filtered.get("s3.region"));
        Assertions.assertEquals("s3.amazonaws.com", filtered.get("s3.endpoint"));
        Assertions.assertFalse(filtered.containsKey("other.property"));
        Assertions.assertFalse(filtered.containsKey("table.name"));
    }

    @Test
    public void testFilterCloudStoragePropertiesWithOSS() {
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("oss.access-key-id", "testOssAccessKey");
        rawCredentials.put("oss.secret-access-key", "testOssSecretKey");
        rawCredentials.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        rawCredentials.put("oss.region", "cn-beijing");
        rawCredentials.put("unrelated.key", "unrelated_value");

        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawCredentials);

        Assertions.assertEquals(4, filtered.size());
        Assertions.assertEquals("testOssAccessKey", filtered.get("oss.access-key-id"));
        Assertions.assertEquals("testOssSecretKey", filtered.get("oss.secret-access-key"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", filtered.get("oss.endpoint"));
        Assertions.assertEquals("cn-beijing", filtered.get("oss.region"));
        Assertions.assertFalse(filtered.containsKey("unrelated.key"));
    }

    @Test
    public void testFilterCloudStoragePropertiesWithMultipleCloudTypes() {
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("s3.access-key-id", "s3AccessKey");
        rawCredentials.put("oss.access-key-id", "ossAccessKey");
        rawCredentials.put("cos.secret-key", "cosSecretKey");
        rawCredentials.put("obs.endpoint", "obs.endpoint.com");
        rawCredentials.put("gs.project-id", "gsProjectId");
        rawCredentials.put("azure.account-name", "azureAccount");
        rawCredentials.put("hdfs.namenode", "hdfsNamenode");

        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawCredentials);

        Assertions.assertEquals(6, filtered.size());
        Assertions.assertEquals("s3AccessKey", filtered.get("s3.access-key-id"));
        Assertions.assertEquals("ossAccessKey", filtered.get("oss.access-key-id"));
        Assertions.assertEquals("cosSecretKey", filtered.get("cos.secret-key"));
        Assertions.assertEquals("obs.endpoint.com", filtered.get("obs.endpoint"));
        Assertions.assertEquals("gsProjectId", filtered.get("gs.project-id"));
        Assertions.assertEquals("azureAccount", filtered.get("azure.account-name"));
        Assertions.assertFalse(filtered.containsKey("hdfs.namenode"));
    }

    @Test
    public void testFilterCloudStoragePropertiesWithEmptyInput() {
        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(new HashMap<>());
        Assertions.assertTrue(filtered.isEmpty());
    }

    @Test
    public void testFilterCloudStoragePropertiesWithNullInput() {
        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(null);
        Assertions.assertNotNull(filtered);
        Assertions.assertTrue(filtered.isEmpty());
    }

    @Test
    public void testFilterCloudStoragePropertiesWithNullKeys() {
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put(null, "nullKeyValue");
        rawCredentials.put("s3.access-key-id", "validValue");
        rawCredentials.put("valid.key", null);

        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawCredentials);

        Assertions.assertEquals(1, filtered.size());
        Assertions.assertEquals("validValue", filtered.get("s3.access-key-id"));
        Assertions.assertFalse(filtered.containsKey(null));
        Assertions.assertFalse(filtered.containsKey("valid.key"));
    }

    @Test
    public void testFilterCloudStoragePropertiesWithNullValues() {
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("s3.access-key-id", "validValue");
        rawCredentials.put("s3.secret-access-key", null);
        rawCredentials.put("oss.endpoint", "ossEndpoint");

        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawCredentials);

        Assertions.assertEquals(2, filtered.size());
        Assertions.assertEquals("validValue", filtered.get("s3.access-key-id"));
        Assertions.assertEquals("ossEndpoint", filtered.get("oss.endpoint"));
        Assertions.assertFalse(filtered.containsKey("s3.secret-access-key"));
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithSingleStorage() {
        // Create mock storage properties
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);

        Map<String, String> s3BackendProps = new HashMap<>();
        s3BackendProps.put("AWS_ACCESS_KEY", "testAccessKey");
        s3BackendProps.put("AWS_SECRET_KEY", "testSecretKey");
        s3BackendProps.put("AWS_REGION", "us-west-2");

        Mockito.when(s3Properties.getBackendConfigProperties()).thenReturn(s3BackendProps);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("testAccessKey", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("testSecretKey", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("us-west-2", result.get("AWS_REGION"));
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithMultipleStorages() {
        // Create mock storage properties
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);
        StorageProperties ossProperties = Mockito.mock(StorageProperties.class);
        StorageProperties hdfsProperties = Mockito.mock(StorageProperties.class);

        Map<String, String> s3BackendProps = new HashMap<>();
        s3BackendProps.put("AWS_ACCESS_KEY", "s3AccessKey");
        s3BackendProps.put("AWS_REGION", "us-west-2");

        Map<String, String> ossBackendProps = new HashMap<>();
        ossBackendProps.put("AWS_ACCESS_KEY", "ossAccessKey"); // OSS might use AWS-compatible props
        ossBackendProps.put("AWS_ENDPOINT", "oss-cn-beijing.aliyuncs.com");

        Map<String, String> hdfsBackendProps = new HashMap<>();
        hdfsBackendProps.put("HDFS_NAMENODE", "hdfs://namenode:9000");

        Mockito.when(s3Properties.getBackendConfigProperties()).thenReturn(s3BackendProps);
        Mockito.when(ossProperties.getBackendConfigProperties()).thenReturn(ossBackendProps);
        Mockito.when(hdfsProperties.getBackendConfigProperties()).thenReturn(hdfsBackendProps);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);
        storagePropertiesMap.put(Type.OSS, ossProperties);
        storagePropertiesMap.put(Type.HDFS, hdfsProperties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertEquals(4, result.size());
        // Note: Last one wins when there are duplicate keys, but order is not guaranteed due to HashMap
        Assertions.assertTrue(result.containsKey("AWS_ACCESS_KEY"));
        Assertions.assertTrue(result.get("AWS_ACCESS_KEY").equals("s3AccessKey") || result.get("AWS_ACCESS_KEY").equals("ossAccessKey"));
        Assertions.assertEquals("us-west-2", result.get("AWS_REGION"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", result.get("AWS_ENDPOINT"));
        Assertions.assertEquals("hdfs://namenode:9000", result.get("HDFS_NAMENODE"));
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithNullValues() {
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);

        Map<String, String> s3BackendProps = new HashMap<>();
        s3BackendProps.put("AWS_ACCESS_KEY", "testAccessKey");
        s3BackendProps.put("AWS_SECRET_KEY", null); // null value should be filtered out
        s3BackendProps.put("AWS_REGION", "us-west-2");
        s3BackendProps.put("AWS_TOKEN", null); // null value should be filtered out

        Mockito.when(s3Properties.getBackendConfigProperties()).thenReturn(s3BackendProps);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("testAccessKey", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("us-west-2", result.get("AWS_REGION"));
        Assertions.assertFalse(result.containsKey("AWS_SECRET_KEY"));
        Assertions.assertFalse(result.containsKey("AWS_TOKEN"));
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithEmptyMap() {
        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithEmptyBackendProps() {
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);
        Mockito.when(s3Properties.getBackendConfigProperties()).thenReturn(new HashMap<>());

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }
}
