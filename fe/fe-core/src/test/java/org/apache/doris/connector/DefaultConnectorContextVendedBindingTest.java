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

package org.apache.doris.connector;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.fs.FileSystemPluginManager;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Verifies the failure boundary without depending on a particular cloud credential dialect. */
public class DefaultConnectorContextVendedBindingTest {
    private final FileSystemProperties properties = Mockito.mock(FileSystemProperties.class);
    private final BackendStorageProperties backend = Mockito.mock(BackendStorageProperties.class);
    @SuppressWarnings("unchecked")
    private final FileSystemProvider<FileSystemProperties> provider = Mockito.mock(FileSystemProvider.class);
    private final Map<String, String> token = Map.of("s3.test-token", "temporary");

    @BeforeEach
    void installProvider() {
        Mockito.when(provider.name()).thenReturn("TEST");
        Mockito.when(provider.sensitivePropertyKeys()).thenReturn(Set.of());
        Mockito.when(provider.supportsGuess(Mockito.anyMap())).thenReturn(true);
        Mockito.when(provider.bindVended(Mockito.anyMap(), Mockito.anyMap())).thenReturn(Optional.empty());
        Mockito.when(provider.bind(Mockito.anyMap())).thenReturn(properties);
        Mockito.when(properties.providerName()).thenReturn("TEST");
        Mockito.when(properties.rawProperties()).thenReturn(Map.of());
        Mockito.when(properties.getSupportedSchemes()).thenReturn(Set.of("test"));
        Mockito.when(properties.fsCacheFingerprint()).thenReturn("fingerprint");
        Mockito.when(properties.toBackendProperties()).thenReturn(Optional.of(backend));
        Mockito.when(backend.toMap()).thenReturn(Map.of("credential", "temporary"));
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(provider);
        StorageAdapter.initPluginManager(manager);
    }

    @AfterEach
    void resetPluginManager() {
        StorageAdapter.initPluginManager(null);
    }

    @Test
    void recognizedAccessFailureMustEscapeLegacyFailSoftBoundary() {
        Mockito.when(provider.bindVended(Mockito.anyMap(), Mockito.anyMap())).thenReturn(Optional.of(properties));
        StoragePropertiesException failure = new StoragePropertiesException("Test credential is expired");
        Mockito.doThrow(failure).when(properties).validateForAccess();

        Assertions.assertSame(failure, Assertions.assertThrows(StoragePropertiesException.class,
                () -> context().vendStorageCredentials(token)));

        Mockito.verify(provider, Mockito.never()).bind(Mockito.anyMap());
        Mockito.verifyNoInteractions(backend);
    }

    @Test
    void recognizedBindingFailureMustEscapeLegacyFailSoftBoundary() {
        StoragePropertiesException failure = new StoragePropertiesException("Invalid test credential");
        Mockito.when(provider.bindVended(Mockito.anyMap(), Mockito.anyMap())).thenThrow(failure);

        Assertions.assertSame(failure, Assertions.assertThrows(StoragePropertiesException.class,
                () -> context().vendStorageCredentials(token)));

        Mockito.verify(provider, Mockito.never()).bind(Mockito.anyMap());
    }

    @Test
    void legacyBackendFailureRetainsEmptyOverlay() {
        Mockito.doThrow(new StoragePropertiesException("Legacy backend failure")).when(properties).validateForAccess();

        Assertions.assertTrue(context().vendStorageCredentials(token).isEmpty());

        Mockito.verify(provider).bind(Mockito.anyMap());
    }

    @Test
    void legacyBindingFailureRetainsEmptyOverlay() {
        Mockito.when(provider.bind(Mockito.anyMap())).thenThrow(new StoragePropertiesException("Legacy bind failure"));

        Assertions.assertTrue(context().vendStorageCredentials(token).isEmpty());

        Mockito.verify(provider).bind(Mockito.anyMap());
    }

    @Test
    void vendedBindingReceivesUnfilteredCredentialsAndCatalogDefaults() {
        Map<String, String> raw = Map.of("unlisted.credential", "temporary");
        Map<String, String> catalog = Map.of("unlisted.endpoint", "endpoint");
        Mockito.when(provider.bindVended(Mockito.anyMap(), Mockito.anyMap())).thenReturn(Optional.of(properties));
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, Collections::emptyMap, () -> catalog);

        Assertions.assertEquals("temporary", context.vendStorageCredentials(raw).get("credential"));

        Mockito.verify(provider).bindVended(raw, catalog);
        Mockito.verify(provider, Mockito.never()).bind(Mockito.anyMap());
    }

    private static DefaultConnectorContext context() {
        return new DefaultConnectorContext("c", 1L);
    }
}
