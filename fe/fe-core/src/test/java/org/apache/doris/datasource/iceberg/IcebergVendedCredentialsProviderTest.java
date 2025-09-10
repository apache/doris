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

import org.apache.doris.datasource.credentials.CredentialExtractor;
import org.apache.doris.datasource.credentials.CredentialUtils;
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
    public void testS3CredentialExtractorWithValidCredentials() {
        IcebergS3CredentialExtractor extractor = new IcebergS3CredentialExtractor();

        Map<String, String> properties = new HashMap<>();
        properties.put(S3FileIOProperties.ACCESS_KEY_ID, "test-access-key");
        properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "test-secret-key");
        properties.put(S3FileIOProperties.SESSION_TOKEN, "test-session-token");

        Map<String, String> credentials = extractor.extractCredentials(properties);

        Assertions.assertEquals("test-access-key", credentials.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("test-secret-key", credentials.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("test-session-token", credentials.get("AWS_TOKEN"));
    }

    @Test
    public void testS3CredentialExtractorWithPartialCredentials() {
        IcebergS3CredentialExtractor extractor = new IcebergS3CredentialExtractor();

        Map<String, String> properties = new HashMap<>();
        properties.put(S3FileIOProperties.ACCESS_KEY_ID, "test-access-key");
        properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "test-secret-key");
        // No session token

        Map<String, String> credentials = extractor.extractCredentials(properties);

        Assertions.assertEquals("test-access-key", credentials.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("test-secret-key", credentials.get("AWS_SECRET_KEY"));
        Assertions.assertFalse(credentials.containsKey("AWS_TOKEN"));
    }

    @Test
    public void testS3CredentialExtractorWithEmptyProperties() {
        IcebergS3CredentialExtractor extractor = new IcebergS3CredentialExtractor();

        Map<String, String> credentials = extractor.extractCredentials(new HashMap<>());
        Assertions.assertTrue(credentials.isEmpty());

        credentials = extractor.extractCredentials(null);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testS3CredentialExtractorWithNonAwsProperties() {
        IcebergS3CredentialExtractor extractor = new IcebergS3CredentialExtractor();

        Map<String, String> properties = new HashMap<>();
        properties.put("some.other.property", "value");
        properties.put("unrelated.key", "unrelated-value");

        Map<String, String> credentials = extractor.extractCredentials(properties);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testExtractVendedCredentialsFromTable() {
        Table table = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);

        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "vended-access-key");
        ioProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "vended-secret-key");
        ioProperties.put(S3FileIOProperties.SESSION_TOKEN, "vended-session-token");

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(ioProperties);

        Map<String, String> credentials = IcebergVendedCredentialsProvider.extractVendedCredentialsFromTable(table);

        Assertions.assertEquals("vended-access-key", credentials.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("vended-secret-key", credentials.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("vended-session-token", credentials.get("AWS_TOKEN"));
    }

    @Test
    public void testExtractVendedCredentialsFromTableWithNullTable() {
        Map<String, String> credentials = IcebergVendedCredentialsProvider.extractVendedCredentialsFromTable(null);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testExtractVendedCredentialsFromTableWithNullIO() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.io()).thenReturn(null);

        Map<String, String> credentials = IcebergVendedCredentialsProvider.extractVendedCredentialsFromTable(table);
        Assertions.assertTrue(credentials.isEmpty());
    }

    @Test
    public void testExtractCredentialsFromFileIO() {
        Map<String, String> fileIoProperties = new HashMap<>();
        fileIoProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "fileio-access-key");
        fileIoProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "fileio-secret-key");

        IcebergS3CredentialExtractor extractor = new IcebergS3CredentialExtractor();

        Map<String, String> credentials = CredentialUtils
                .extractCredentialsFromFileIO(fileIoProperties, extractor);

        Assertions.assertEquals("fileio-access-key", credentials.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("fileio-secret-key", credentials.get("AWS_SECRET_KEY"));
        Assertions.assertFalse(credentials.containsKey("AWS_TOKEN"));
    }

    @Test
    public void testCustomCredentialExtractor() {
        CredentialExtractor customExtractor =
                new CredentialExtractor() {
                    @Override
                    public Map<String, String> extractCredentials(Map<String, String> properties) {
                        Map<String, String> result = new HashMap<>();
                        if (properties != null && properties.containsKey("custom.key")) {
                            result.put("CUSTOM_BACKEND_PROPERTY", properties.get("custom.key"));
                        }
                        return result;
                    }
                };

        Map<String, String> properties = new HashMap<>();
        properties.put("custom.key", "custom-value");

        Map<String, String> credentials = CredentialUtils
                .extractCredentialsFromFileIO(properties, customExtractor);

        Assertions.assertEquals("custom-value", credentials.get("CUSTOM_BACKEND_PROPERTY"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithRestVendedCredentialsEnabled() {
        // Mock IcebergRestProperties with vended credentials enabled
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

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
        FileIO fileIO = Mockito.mock(FileIO.class);
        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "vended-access-key");
        ioProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "vended-secret-key");
        ioProperties.put(S3FileIOProperties.SESSION_TOKEN, "vended-session-token");

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(ioProperties);

        // Test with vended credentials enabled
        Map<String, String> result = IcebergVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertEquals("vended-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("vended-secret-key", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("vended-session-token", result.get("AWS_TOKEN"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithRestVendedCredentialsDisabled() {
        // Mock IcebergRestProperties with vended credentials disabled
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(false);

        // Mock storage properties
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.put("base.property", "base-value");
        baseProperties.put("AWS_ACCESS_KEY", "static-access-key");
        baseProperties.put("AWS_SECRET_KEY", "static-secret-key");

        Mockito.when(storageProperties.getBackendConfigProperties()).thenReturn(baseProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, storageProperties);

        // Mock table (should be ignored since vended credentials are disabled)
        Table table = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);
        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "vended-access-key");
        ioProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "vended-secret-key");

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(ioProperties);

        // Test with vended credentials disabled
        Map<String, String> result = IcebergVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertEquals("static-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("static-secret-key", result.get("AWS_SECRET_KEY"));
        Assertions.assertFalse(result.containsKey("AWS_TOKEN"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithNonRestMetastore() {
        // Mock non-REST metastore properties
        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);

        // Mock storage properties
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.put("base.property", "base-value");
        baseProperties.put("AWS_ACCESS_KEY", "static-access-key");

        Mockito.when(storageProperties.getBackendConfigProperties()).thenReturn(baseProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, storageProperties);

        // Mock table (should be ignored for non-REST catalogs)
        Table table = Mockito.mock(Table.class);

        // Test with non-REST metastore
        Map<String, String> result = IcebergVendedCredentialsProvider.getBackendLocationProperties(
                metastoreProperties, storagePropertiesMap, table);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertEquals("static-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertFalse(result.containsKey("AWS_TOKEN"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithMultipleStorageTypes() {
        // Mock IcebergRestProperties with vended credentials enabled
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        // Mock S3 storage properties
        StorageProperties s3Properties = Mockito.mock(StorageProperties.class);
        Map<String, String> s3BaseProperties = new HashMap<>();
        s3BaseProperties.put("s3.property", "s3-value");

        Map<String, String> s3VendedProperties = new HashMap<>();
        s3VendedProperties.put("s3.property", "s3-value");
        s3VendedProperties.put("AWS_ACCESS_KEY", "vended-access-key");

        Mockito.when(s3Properties.getBackendConfigProperties()).thenReturn(s3BaseProperties);
        Mockito.when(s3Properties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(s3VendedProperties);

        // Mock HDFS storage properties
        StorageProperties hdfsProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> hdfsBaseProperties = new HashMap<>();
        hdfsBaseProperties.put("hdfs.property", "hdfs-value");

        Mockito.when(hdfsProperties.getBackendConfigProperties()).thenReturn(hdfsBaseProperties);
        Mockito.when(hdfsProperties.getBackendConfigProperties(Mockito.anyMap())).thenReturn(hdfsBaseProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, s3Properties);
        storagePropertiesMap.put(Type.HDFS, hdfsProperties);

        // Mock table with vended credentials
        Table table = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);
        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "vended-access-key");

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(ioProperties);

        // Test with multiple storage types
        Map<String, String> result = IcebergVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertEquals("s3-value", result.get("s3.property"));
        Assertions.assertEquals("hdfs-value", result.get("hdfs.property"));
        Assertions.assertEquals("vended-access-key", result.get("AWS_ACCESS_KEY"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithNullTable() {
        // Mock IcebergRestProperties with vended credentials enabled
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        // Mock storage properties
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.put("base.property", "base-value");

        Mockito.when(storageProperties.getBackendConfigProperties()).thenReturn(baseProperties);

        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(Type.S3, storageProperties);

        // Test with null table (should fall back to base properties)
        Map<String, String> result = IcebergVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, null);

        Assertions.assertEquals("base-value", result.get("base.property"));
        Assertions.assertFalse(result.containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    public void testGetBackendLocationPropertiesWithEmptyStorageMap() {
        // Mock IcebergRestProperties
        IcebergRestProperties restProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(restProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        // Empty storage properties map
        Map<Type, StorageProperties> storagePropertiesMap = new HashMap<>();

        // Mock table
        Table table = Mockito.mock(Table.class);

        // Test with empty storage map
        Map<String, String> result = IcebergVendedCredentialsProvider.getBackendLocationProperties(
                restProperties, storagePropertiesMap, table);

        Assertions.assertTrue(result.isEmpty());
    }
}
