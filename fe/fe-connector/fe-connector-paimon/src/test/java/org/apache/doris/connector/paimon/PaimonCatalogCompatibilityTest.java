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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.Connector;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

class PaimonCatalogCompatibilityTest {
    @TempDir
    java.nio.file.Path warehouse;

    @Test
    void ordinaryWarehouseRemainsUsable() throws Exception {
        try (Catalog catalog = catalog(new ProbeFileIO())) {
            Assertions.assertSame(catalog, PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        }
    }

    @Test
    void singleMarkerDoesNotEnableLegacyPrivileges() throws Exception {
        for (String marker : new String[] {"user.sys", "privilege.sys"}) {
            java.nio.file.Path directory = Files.createDirectory(warehouse.resolve(marker));
            try (Catalog catalog = catalog(new ProbeFileIO())) {
                Assertions.assertSame(catalog, PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
            }
            Files.delete(directory);
        }
    }

    @Test
    void legacyWarehouseIsRejectedAndClosedWithoutDeletingMetadata() throws Exception {
        legacyMarkers();
        RecordingCatalog catalog = catalog(new ProbeFileIO());
        UnsupportedOperationException failure = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        Assertions.assertTrue(failure.getMessage().contains("Migrate authorization"));
        Assertions.assertTrue(catalog.closed);
        Assertions.assertTrue(Files.isDirectory(warehouse.resolve("user.sys")));
        Assertions.assertTrue(Files.isDirectory(warehouse.resolve("privilege.sys")));
    }

    @Test
    void catalogWrapperCannotHideLegacyPrivileges() throws Exception {
        legacyMarkers();
        RecordingCatalog root = catalog(new ProbeFileIO());
        Catalog wrapper = new DelegateCatalog(root) {
            @Override
            public CatalogLoader catalogLoader() {
                return root.catalogLoader();
            }
        };
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(wrapper));
        Assertions.assertTrue(root.closed);
    }

    @Test
    void nonFileCatalogIsNotProbed() throws Exception {
        Catalog catalog = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(),
                new Class<?>[] {Catalog.class}, (proxy, method, arguments) -> {
                    throw new AssertionError("Non-file catalog must not be probed: " + method.getName());
                });
        Assertions.assertSame(catalog, PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
    }

    @Test
    void metadataAccessFailureIsNotTreatedAsAbsent() {
        ProbeFileIO io = new ProbeFileIO();
        io.failExists = true;
        RecordingCatalog catalog = catalog(io);
        IOException failure = Assertions.assertThrows(IOException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        Assertions.assertEquals("access denied", failure.getMessage());
        Assertions.assertTrue(catalog.closed);
    }

    @Test
    void closeFailureDoesNotHideUpgradeError() throws Exception {
        legacyMarkers();
        RecordingCatalog catalog = catalog(new ProbeFileIO());
        catalog.failClose = true;
        UnsupportedOperationException failure = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PaimonCatalogCompatibility.requireSupportedPrivileges(catalog));
        Assertions.assertTrue(catalog.closed);
        Assertions.assertEquals("close failed", failure.getSuppressed()[0].getMessage());
    }

    @Test
    void probesAndRejectedCatalogCloseRemainInAuthenticationContext() throws Exception {
        legacyMarkers();
        RecordingConnectorContext context = new RecordingConnectorContext();
        ProbeFileIO io = new ProbeFileIO();
        io.context = context;
        RecordingCatalog catalog = catalog(io);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> context.executeAuthenticated(
                        () -> PaimonCatalogCompatibility.requireSupportedPrivileges(catalog)));
        Assertions.assertTrue(catalog.closed);
        Assertions.assertEquals(2, io.probes);
        Assertions.assertFalse(context.isAuthenticated());
    }

    @Test
    void pluginCatalogCreationRejectsLegacyWarehouse() throws Exception {
        legacyMarkers();
        RecordingConnectorContext context = new RecordingConnectorContext();
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "paimon");
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("warehouse", warehouse.toUri().toString());
        try (Connector connector = new PaimonConnector(properties, context)) {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> connector.getMetadata(null));
            Assertions.assertTrue(failure.getMessage().contains("Migrate authorization"), failure.getMessage());
            Assertions.assertTrue(context.authCount > 0);
        }
        Assertions.assertTrue(Files.isDirectory(warehouse.resolve("user.sys")));
        Assertions.assertTrue(Files.isDirectory(warehouse.resolve("privilege.sys")));
    }

    private void legacyMarkers() throws IOException {
        Files.createDirectory(warehouse.resolve("user.sys"));
        Files.createDirectory(warehouse.resolve("privilege.sys"));
    }

    private RecordingCatalog catalog(ProbeFileIO io) {
        return new RecordingCatalog(io, new Path(warehouse.toUri().toString()));
    }

    private static class ProbeFileIO extends LocalFileIO {
        boolean failExists;
        int probes;
        RecordingConnectorContext context;

        @Override
        public boolean exists(Path path) throws IOException {
            if (context != null) {
                Assertions.assertTrue(context.isAuthenticated());
            }
            probes++;
            if (failExists) {
                throw new IOException("access denied");
            }
            return super.exists(path);
        }
    }

    private static class RecordingCatalog extends FileSystemCatalog {
        final ProbeFileIO io;
        boolean closed;
        boolean failClose;

        RecordingCatalog(ProbeFileIO io, Path path) {
            super(io, path);
            this.io = io;
        }

        @Override
        public void close() throws Exception {
            if (io.context != null) {
                Assertions.assertTrue(io.context.isAuthenticated());
            }
            closed = true;
            super.close();
            if (failClose) {
                throw new IOException("close failed");
            }
        }
    }
}
