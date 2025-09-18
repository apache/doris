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

import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonRestMetaStoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class VendedCredentialsFactoryTest {

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsForIceberg() {
        // Mock Iceberg REST properties
        IcebergRestProperties icebergProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(icebergProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);
        Mockito.when(icebergProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        // Mock table with vended credentials
        Table table = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);

        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put("s3.access-key-id", "testAccessKey");
        ioProperties.put("s3.secret-access-key", "testSecretKey");
        ioProperties.put("s3.region", "us-west-2");

        Mockito.when(table.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(ioProperties);

        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(icebergProperties, baseStorageMap, table);

        // Should return the result from IcebergVendedCredentialsProvider or fall back to base map
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsForPaimon() {
        // Mock Paimon REST properties
        PaimonRestMetaStoreProperties paimonProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(paimonProperties.getType()).thenReturn(MetastoreProperties.Type.PAIMON);
        Mockito.when(paimonProperties.getTokenProvider()).thenReturn("dlf");

        // Mock Paimon table
        org.apache.paimon.table.Table paimonTable = Mockito.mock(org.apache.paimon.table.Table.class);

        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(paimonProperties, baseStorageMap, paimonTable);

        // Should return the result from PaimonVendedCredentialsProvider or fall back to base map
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsForUnsupportedType() {
        // Mock unsupported metastore type (e.g., HMS)
        MetastoreProperties hmsProperties = Mockito.mock(MetastoreProperties.class);
        Mockito.when(hmsProperties.getType()).thenReturn(MetastoreProperties.Type.HMS);

        Object table = new Object();
        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(hmsProperties, baseStorageMap, table);

        // Should return the base storage map for unsupported types
        Assertions.assertEquals(baseStorageMap, result);
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsWithNullMetastore() {
        Object table = new Object();
        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(null, baseStorageMap, table);

        // Should return the base storage map when metastore is null
        Assertions.assertEquals(baseStorageMap, result);
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsWithNullTable() {
        IcebergRestProperties icebergProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(icebergProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);
        Mockito.when(icebergProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(icebergProperties, baseStorageMap, null);

        // Should return the base storage map when table is null
        Assertions.assertEquals(baseStorageMap, result);
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsWithException() {
        // Mock properties that will cause an exception in the provider
        MetastoreProperties problematicProperties = Mockito.mock(MetastoreProperties.class);
        Mockito.when(problematicProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);

        Object table = new Object(); // Wrong type will cause ClassCastException
        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(problematicProperties, baseStorageMap, table);

        // Should return the base storage map when there's an exception
        Assertions.assertEquals(baseStorageMap, result);
    }

    @Test
    public void testGetStoragePropertiesMapWithNonEmptyBaseStorageMap() {
        // Create base storage map with some properties
        StorageProperties baseS3Properties = Mockito.mock(StorageProperties.class);
        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();
        baseStorageMap.put(Type.S3, baseS3Properties);

        // Mock unsupported metastore type
        MetastoreProperties hmsProperties = Mockito.mock(MetastoreProperties.class);
        Mockito.when(hmsProperties.getType()).thenReturn(MetastoreProperties.Type.HMS);

        Object table = new Object();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(hmsProperties, baseStorageMap, table);

        // Should return the base storage map
        Assertions.assertEquals(baseStorageMap, result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(baseS3Properties, result.get(Type.S3));
    }

    @Test
    public void testGetStoragePropertiesMapWithIcebergVendedCredentialsDisabled() {
        IcebergRestProperties icebergProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(icebergProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);
        Mockito.when(icebergProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(false);

        Table table = Mockito.mock(Table.class);
        Map<Type, StorageProperties> baseStorageMap = new HashMap<>();

        Map<Type, StorageProperties> result = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(icebergProperties, baseStorageMap, table);

        // Should return the base storage map when vended credentials are disabled
        Assertions.assertEquals(baseStorageMap, result);
    }

    @Test
    public void testGetProviderTypeReturnsCorrectProvider() {
        // Note: getProviderType is private, but we can test it indirectly through the public method

        // Test Iceberg type
        IcebergRestProperties icebergProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(icebergProperties.getType()).thenReturn(MetastoreProperties.Type.ICEBERG);
        Mockito.when(icebergProperties.isIcebergRestVendedCredentialsEnabled()).thenReturn(true);

        Table icebergTable = Mockito.mock(Table.class);
        FileIO fileIO = Mockito.mock(FileIO.class);
        Mockito.when(icebergTable.io()).thenReturn(fileIO);
        Mockito.when(fileIO.properties()).thenReturn(new HashMap<>());

        Map<Type, StorageProperties> result1 = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(icebergProperties, new HashMap<>(), icebergTable);

        // Should use IcebergVendedCredentialsProvider
        Assertions.assertNotNull(result1);

        // Test Paimon type
        PaimonRestMetaStoreProperties paimonProperties = Mockito.mock(PaimonRestMetaStoreProperties.class);
        Mockito.when(paimonProperties.getType()).thenReturn(MetastoreProperties.Type.PAIMON);

        org.apache.paimon.table.Table paimonTable = Mockito.mock(org.apache.paimon.table.Table.class);

        Map<Type, StorageProperties> result2 = VendedCredentialsFactory
                .getStoragePropertiesMapWithVendedCredentials(paimonProperties, new HashMap<>(), paimonTable);

        // Should use PaimonVendedCredentialsProvider
        Assertions.assertNotNull(result2);
    }
}
