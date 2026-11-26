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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class IcebergHadoopCatalogNamespaceTest {
    @TempDir
    Path tempDir;

    @Test
    public void freshWarehouseCanListAndCreateFirstDatabase() throws IOException {
        Path warehouse = tempDir.resolve("warehouse");
        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(false), warehouse.toString())) {
            CatalogBackedIcebergCatalogOps ops = new CatalogBackedIcebergCatalogOps(catalog);
            Assertions.assertFalse(Files.exists(warehouse));
            Assertions.assertTrue(ops.listDatabaseNames().isEmpty());
            Assertions.assertFalse(Files.exists(warehouse), "Listing must not create the warehouse");
            ops.createDatabase("first_db", Collections.emptyMap());
            Assertions.assertEquals(Collections.singletonList("first_db"), ops.listDatabaseNames());
        }
    }

    @Test
    public void missingNamedHadoopRootStillFails() throws IOException {
        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(false),
                tempDir.resolve("warehouse").toString())) {
            CatalogBackedIcebergCatalogOps ops = new CatalogBackedIcebergCatalogOps(
                    catalog, false, false, false, Optional.of("missing"));
            Assertions.assertThrows(NoSuchNamespaceException.class, ops::listDatabaseNames);
        }
    }

    @Test
    public void hadoopIoFailureIsNotAnEmptyCatalog() throws IOException {
        RuntimeIOException failure = new RuntimeIOException(new IOException("Storage unavailable"),
                "Cannot list warehouse");
        try (HadoopCatalog catalog = new HadoopCatalog() {
            @Override
            public List<Namespace> listNamespaces(Namespace namespace) {
                throw failure;
            }
        }) {
            Assertions.assertSame(failure, Assertions.assertThrows(RuntimeIOException.class,
                    () -> new CatalogBackedIcebergCatalogOps(catalog).listDatabaseNames()));
        }
    }
}
