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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class IcebergVendedCredentialsProviderTest {

    @Test
    public void testIsVendedCredentialsEnabled() {
        IcebergVendedCredentialsProvider provider = IcebergVendedCredentialsProvider.getInstance();

        // Test with REST catalog and vended credentials enabled
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        Assertions.assertTrue(provider.isVendedCredentialsEnabled(restProperties));

        // Test with REST catalog and vended credentials disabled
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(false);
        Assertions.assertFalse(provider.isVendedCredentialsEnabled(restProperties));
    }

    @Test
    public void testExtractRawVendedCredentials() {
        IcebergVendedCredentialsProvider provider = IcebergVendedCredentialsProvider.getInstance();

        // Mock table with S3 vended credentials
        Table table = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);

        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "ASIATEST123456");
        ioProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "testSecretKey");
        ioProperties.put(S3FileIOProperties.SESSION_TOKEN, "testSessionToken");
        ioProperties.put("s3.region", "us-west-2");

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(ioProperties);

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);

        Assertions.assertEquals("ASIATEST123456", rawCredentials.get("s3.access-key-id"));
        Assertions.assertEquals("testSecretKey", rawCredentials.get("s3.secret-access-key"));
        Assertions.assertEquals("testSessionToken", rawCredentials.get("s3.session-token"));
        Assertions.assertEquals("us-west-2", rawCredentials.get("s3.region"));
    }

    @Test
    public void testExtractRawVendedCredentialsWithNullTable() {
        IcebergVendedCredentialsProvider provider = IcebergVendedCredentialsProvider.getInstance();

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(null);
        Assertions.assertTrue(rawCredentials.isEmpty());
    }

    @Test
    public void testExtractRawVendedCredentialsWithNullIO() {
        IcebergVendedCredentialsProvider provider = IcebergVendedCredentialsProvider.getInstance();

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.io()).thenReturn(null);

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);
        Assertions.assertTrue(rawCredentials.isEmpty());
    }

    @Test
    public void testExtractRawVendedCredentialsWithEmptyProperties() {
        IcebergVendedCredentialsProvider provider = IcebergVendedCredentialsProvider.getInstance();

        Table table = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(new HashMap<>());

        Map<String, String> rawCredentials = provider.extractRawVendedCredentials(table);
        Assertions.assertTrue(rawCredentials.isEmpty());
    }

    @Test
    public void testFilterCloudStorageProperties() {
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("s3.access-key-id", "testAccessKey");
        rawCredentials.put("s3.secret-access-key", "testSecretKey");
        rawCredentials.put("s3.region", "us-west-2");
        rawCredentials.put("iceberg.table.name", "test_table");
        rawCredentials.put("other.property", "other_value");

        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawCredentials);

        Assertions.assertEquals(3, filtered.size());
        Assertions.assertEquals("testAccessKey", filtered.get("s3.access-key-id"));
        Assertions.assertEquals("testSecretKey", filtered.get("s3.secret-access-key"));
        Assertions.assertEquals("us-west-2", filtered.get("s3.region"));
        Assertions.assertFalse(filtered.containsKey("iceberg.table.name"));
        Assertions.assertFalse(filtered.containsKey("other.property"));
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentials() {
        // Mock metastore properties with vended credentials enabled
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        // Mock table with vended credentials
        Table table = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);

        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "ASIATEST123456");
        ioProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "testSecretKey");
        ioProperties.put(S3FileIOProperties.SESSION_TOKEN, "testSessionToken");
        ioProperties.put("s3.region", "us-west-2");

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(ioProperties);

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
        // Mock metastore properties with vended credentials disabled
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(false);

        Table table = Mockito.mock(Table.class);

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(restProperties, new HashMap<>(), table);

        // When vended credentials are disabled, should return the baseStoragePropertiesMap (empty HashMap)
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetStoragePropertiesMapWithNullTable() {
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(restProperties, new HashMap<>(), null);

        // When table is null, should return the baseStoragePropertiesMap (empty HashMap)
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetBackendPropertiesFromStorageMap() {
        // Create mock storage properties
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);
        StorageProperties hdfsProperties = Mockito.mock(StorageProperties.class);

        Map<String, String> s3BackendProps = new HashMap<>();
        s3BackendProps.put("AWS_ACCESS_KEY", "testAccessKey");
        s3BackendProps.put("AWS_SECRET_KEY", "testSecretKey");
        s3BackendProps.put("AWS_TOKEN", "testToken");

        Map<String, String> hdfsBackendProps = new HashMap<>();
        hdfsBackendProps.put("HDFS_PROPERTY", "hdfsValue");

        Mockito.when(s3Properties.getBackendConfigProperties()).thenReturn(s3BackendProps);
        Mockito.when(hdfsProperties.getBackendConfigProperties()).thenReturn(hdfsBackendProps);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);
        storagePropertiesMap.put(Type.HDFS, hdfsProperties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertEquals(4, result.size());
        Assertions.assertEquals("testAccessKey", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("testSecretKey", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("testToken", result.get("AWS_TOKEN"));
        Assertions.assertEquals("hdfsValue", result.get("HDFS_PROPERTY"));
    }

    @Test
    public void testGetBackendPropertiesFromStorageMapWithNullValues() {
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);

        Map<String, String> s3BackendProps = new HashMap<>();
        s3BackendProps.put("AWS_ACCESS_KEY", "testAccessKey");
        s3BackendProps.put("AWS_SECRET_KEY", null); // null value should be filtered out
        s3BackendProps.put("AWS_TOKEN", "testToken");

        Mockito.when(s3Properties.getBackendConfigProperties()).thenReturn(s3BackendProps);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);

        Map<String, String> result = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("testAccessKey", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("testToken", result.get("AWS_TOKEN"));
        Assertions.assertFalse(result.containsKey("AWS_SECRET_KEY"));
    }
}
