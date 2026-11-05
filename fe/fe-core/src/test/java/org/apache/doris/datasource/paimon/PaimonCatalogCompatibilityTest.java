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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class PaimonCatalogCompatibilityTest {
    @TempDir
    java.nio.file.Path warehouse;

    @Test
    void ordinaryWarehouseRemainsUsable() throws Exception {
        try (Catalog catalog = localCatalog()) {
            Assertions.assertSame(catalog, PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        }
    }

    @Test
    void singleMarkerDoesNotEnableLegacyPrivileges() throws Exception {
        for (String marker : new String[] {"user.sys", "privilege.sys"}) {
            java.nio.file.Path directory = Files.createDirectory(warehouse.resolve(marker));
            try (Catalog catalog = localCatalog()) {
                Assertions.assertSame(catalog, PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
            }
            Files.delete(directory);
        }
    }

    @Test
    void legacyWarehouseIsRejectedAndClosedWithoutDeletingMetadata() throws Exception {
        legacyMarkers();
        Catalog catalog = Mockito.spy(localCatalog());
        UnsupportedOperationException failure = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        Assertions.assertTrue(failure.getMessage().contains("Migrate authorization"));
        Mockito.verify(catalog).close();
        Assertions.assertTrue(Files.isDirectory(warehouse.resolve("user.sys")));
        Assertions.assertTrue(Files.isDirectory(warehouse.resolve("privilege.sys")));
    }

    @Test
    void catalogWrapperCannotHideLegacyPrivileges() throws Exception {
        legacyMarkers();
        Catalog root = Mockito.spy(localCatalog());
        Catalog wrapper = new DelegateCatalog(root) {
            @Override
            public CatalogLoader catalogLoader() {
                return root.catalogLoader();
            }
        };
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(wrapper));
        Mockito.verify(root).close();
    }

    @Test
    void nonFileCatalogIsNotProbed() throws Exception {
        Catalog catalog = Mockito.mock(Catalog.class);
        Assertions.assertSame(catalog, PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        Mockito.verifyNoInteractions(catalog);
    }

    @Test
    void metadataAccessFailureIsNotTreatedAsAbsent() throws Exception {
        AbstractCatalog catalog = Mockito.mock(AbstractCatalog.class);
        FileIO io = Mockito.mock(FileIO.class);
        Mockito.when(catalog.warehouse()).thenReturn(warehouse.toUri().toString());
        Mockito.when(catalog.fileIO()).thenReturn(io);
        Mockito.when(io.exists(Mockito.any(Path.class))).thenThrow(new IOException("access denied"));
        IOException failure = Assertions.assertThrows(IOException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        Assertions.assertEquals("access denied", failure.getMessage());
        Mockito.verify(catalog).close();
    }

    @Test
    void closeFailureDoesNotHideUpgradeError() throws Exception {
        legacyMarkers();
        Catalog catalog = Mockito.spy(localCatalog());
        Mockito.doThrow(new IOException("close failed")).when(catalog).close();
        UnsupportedOperationException failure = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        Assertions.assertEquals("close failed", failure.getSuppressed()[0].getMessage());
    }

    @Test
    void catalogCreationAndRejectionRunInsideAuthenticationContext() throws Exception {
        AtomicBoolean authenticated = new AtomicBoolean();
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                authenticated.set(true);
                try {
                    return task.call();
                } finally {
                    authenticated.set(false);
                }
            }
        };
        AbstractCatalog catalog = Mockito.mock(AbstractCatalog.class);
        FileIO io = Mockito.mock(FileIO.class);
        Mockito.when(catalog.warehouse()).thenReturn(warehouse.toUri().toString());
        Mockito.when(catalog.fileIO()).thenReturn(io);
        Mockito.when(io.exists(Mockito.any(Path.class))).thenAnswer(call -> {
            Assertions.assertTrue(authenticated.get());
            return true;
        });
        Mockito.doAnswer(call -> {
            Assertions.assertTrue(authenticated.get());
            return null;
        }).when(catalog).close();
        AbstractPaimonProperties properties = Mockito.mock(AbstractPaimonProperties.class);
        Mockito.when(properties.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(properties.initializeCatalog(Mockito.anyString(), Mockito.anyList())).thenAnswer(call -> {
            Assertions.assertTrue(authenticated.get());
            return catalog;
        });
        PaimonExternalCatalog external = new PaimonExternalCatalog(1L, "legacy", null,
                Collections.emptyMap(), "");
        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(catalogProperty.getOrderedStoragePropertiesList()).thenReturn(Collections.emptyList());
        Deencapsulation.setField(external, "catalogProperty", catalogProperty);
        Deencapsulation.setField(external, "paimonProperties", properties);
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, external::createCatalog);
        Assertions.assertTrue(failure.getMessage().contains("Migrate authorization"));
        Assertions.assertFalse(authenticated.get());
        Mockito.verify(catalog).close();
    }

    private Catalog localCatalog() {
        return new FileSystemCatalog(LocalFileIO.create(), new Path(warehouse.toUri()));
    }

    private void legacyMarkers() throws IOException {
        Files.createDirectory(warehouse.resolve("user.sys"));
        Files.createDirectory(warehouse.resolve("privilege.sys"));
    }
}
