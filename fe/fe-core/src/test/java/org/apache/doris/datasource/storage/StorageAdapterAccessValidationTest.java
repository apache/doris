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

package org.apache.doris.datasource.storage;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.fs.FileSystemPluginManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StorageAdapterAccessValidationTest {

    private final FileSystemProperties properties = Mockito.mock(FileSystemProperties.class);
    private final BackendStorageProperties backend = Mockito.mock(BackendStorageProperties.class);

    @AfterEach
    void resetPluginManager() {
        // Match the other provider-injection tests: later classes use the built-in registry.
        StorageAdapter.initPluginManager(null);
    }

    @Test
    void rejectsInvalidCredentialsBeforeComputingBackendMap() {
        StorageAdapter adapter = createTestAdapter();
        StoragePropertiesException failure = new StoragePropertiesException("Test credential is expired");
        Mockito.doThrow(failure).when(properties).validateForAccess();

        Assertions.assertSame(failure,
                Assertions.assertThrows(StoragePropertiesException.class, adapter::getBackendConfigProperties));
        Mockito.verifyNoInteractions(backend);
    }

    @Test
    void validatesEveryAccessWithoutRecomputingOrExposingCachedMap() {
        StorageAdapter adapter = createTestAdapter();
        Map<String, String> first = adapter.getBackendConfigProperties();
        Map<String, String> expected = new HashMap<>(first);
        first.put("caller.option", "must-not-be-cached");

        Map<String, String> second = adapter.getBackendConfigProperties();

        Assertions.assertEquals(expected, second);
        Assertions.assertNotSame(first, second);
        Assertions.assertEquals("fingerprint", second.get(FsCacheKeys.fsCacheKeyProperty("test")));
        Mockito.verify(properties, Mockito.times(2)).validateForAccess();
        Mockito.verify(backend, Mockito.times(1)).toMap();
    }

    @Test
    void rejectsCachedCredentialsWhenProviderReportsExpiry() {
        StorageAdapter adapter = createTestAdapter();
        Assertions.assertEquals("temporary", adapter.getBackendConfigProperties().get("credential"));
        StoragePropertiesException failure = new StoragePropertiesException("Test credential is expired");
        // Model the provider's clock crossing expiry after the first successful access, without sleeping.
        Mockito.doThrow(failure).when(properties).validateForAccess();

        Assertions.assertSame(failure,
                Assertions.assertThrows(StoragePropertiesException.class, adapter::getBackendConfigProperties));
        Mockito.verify(properties, Mockito.times(2)).validateForAccess();
        Mockito.verify(backend, Mockito.times(1)).toMap();
    }

    @Test
    void bindsExpiredAzureSasButRejectsBackendAccess() {
        StorageAdapter adapter = StorageAdapter.ofProvider("AZURE", Map.of(
                "azure.account_name", "account",
                "azure.sas_token", "sig=temporary",
                "azure.sas_expiry_ms", "1"));

        Assertions.assertEquals(StorageTypeId.AZURE, adapter.getType());
        Assertions.assertDoesNotThrow(adapter.getSpiProperties()::validate);
        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                adapter::getBackendConfigProperties);
        Assertions.assertEquals("Azure SAS credential is expired", failure.getMessage());
        Assertions.assertNull(failure.getCause());
    }

    @ParameterizedTest
    @CsvSource({
            "S3, s3.us-west-2.amazonaws.com",
            "OSS, oss-cn-beijing.aliyuncs.com",
            "GCS, storage.googleapis.com"
    })
    void preservesNonAzureObjectStoreParameters(String provider, String endpoint) {
        // These providers retain their existing S3-compatible input aliases and default access validation.
        StorageAdapter adapter = StorageAdapter.ofProvider(provider, Map.of(
                "s3.endpoint", endpoint,
                "s3.access_key", "access-key",
                "s3.secret_key", "secret-key"));

        Map<String, String> first = adapter.getBackendConfigProperties();
        Assertions.assertEquals(first, adapter.getBackendConfigProperties());
        Assertions.assertEquals("access-key", first.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("secret-key", first.get("AWS_SECRET_KEY"));
        Assertions.assertFalse(first.keySet().stream().anyMatch(key -> key.startsWith("AZURE_")));
    }

    @Test
    void preservesHdfsParametersAndCacheFingerprint() {
        StorageAdapter adapter = StorageAdapter.ofProvider("HDFS", Map.of(
                "uri", "hdfs://namenode/data",
                "hadoop.username", "test-user"));

        Map<String, String> first = adapter.getBackendConfigProperties();
        Assertions.assertEquals(first, adapter.getBackendConfigProperties());
        Assertions.assertEquals(StorageTypeId.HDFS, adapter.getType());
        Assertions.assertEquals(adapter.getFsCacheFingerprint(), first.get(FsCacheKeys.fsCacheKeyProperty("hdfs")));
        Assertions.assertFalse(first.keySet().stream().anyMatch(key -> key.startsWith("AZURE_")));
    }

    private StorageAdapter createTestAdapter() {
        Mockito.when(properties.providerName()).thenReturn("TEST");
        Mockito.when(properties.getSupportedSchemes()).thenReturn(Set.of("test"));
        Mockito.when(properties.fsCacheFingerprint()).thenReturn("fingerprint");
        Mockito.when(properties.toBackendProperties()).thenReturn(Optional.of(backend));
        Mockito.when(backend.toMap()).thenReturn(Map.of("credential", "temporary"));
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(new FileSystemProvider<FileSystemProperties>() {
            @Override
            public String name() {
                return "TEST";
            }

            @Override
            public boolean supports(Map<String, String> raw) {
                return true;
            }

            @Override
            public FileSystemProperties bind(Map<String, String> raw) {
                return properties;
            }

            @Override
            public FileSystem create(Map<String, String> raw) {
                throw new UnsupportedOperationException("No client needed for access-validation tests");
            }
        });
        StorageAdapter.initPluginManager(manager);
        return StorageAdapter.ofProvider("TEST", Map.of());
    }
}
