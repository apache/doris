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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class IcebergGcsRoutingCompatibilityTest {
    private static final String RESOLVER = "org.apache.iceberg.io.ResolvingFileIO";
    private static final String HADOOP_GCS_RESOLVER = "org.apache.iceberg.io.NativeGcsResolvingFileIO";

    @ParameterizedTest
    @ValueSource(strings = {" true ", " TRUE ", "true"})
    void normalizedVendingPreservesServerCredentials(String enabled) {
        Map<String, String> raw = restProperties();
        raw.put("iceberg.rest.vended-credentials-enabled", enabled);
        raw.put("fs.gs.impl", "example.GcsFileSystem");
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        IcebergRestProperties properties = new IcebergRestProperties(raw);
        properties.initNormalizeAndCheckProps();
        Assertions.assertTrue(properties.isIcebergRestVendedCredentialsEnabled());
        Map<String, String> options = new HashMap<>(raw);
        options.putAll(properties.getIcebergRestCatalogProperties());
        properties.toFileIOProperties(ImmutableList.of(), options, new Configuration(false));
        Assertions.assertEquals(RESOLVER, options.get(CatalogProperties.FILE_IO_IMPL));
    }

    @Test
    void detectsGcsSettingsLoadedFromXml(@TempDir Path directory) throws Exception {
        Configuration resource = new Configuration(false);
        resource.set("fs.gs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        resource.set("fs.s3a.access.key", "test-access-key");
        resource.set("fs.s3a.secret.key", "test-secret-key");
        try (OutputStream output = Files.newOutputStream(directory.resolve("gcs-site.xml"))) {
            resource.writeXml(output);
        }
        String previousDirectory = Config.hadoop_config_dir;
        try {
            Config.hadoop_config_dir = directory.toString() + "/";
            Map<String, String> raw = restProperties();
            raw.put("fs.hdfs.support", "true");
            raw.put("hadoop.config.resources", "gcs-site.xml");
            IcebergRestProperties properties = new IcebergRestProperties(raw);
            properties.initNormalizeAndCheckProps();
            Map<String, String> options = new HashMap<>(raw);
            options.putAll(properties.getIcebergRestCatalogProperties());
            Configuration conf = new Configuration(false);
            properties.toFileIOProperties(ImmutableList.of(StorageProperties.createPrimary(raw)), options, conf);
            Assertions.assertEquals("test-access-key", conf.get("fs.s3a.access.key"));
            Assertions.assertFalse(raw.containsKey("fs.gs.impl"));
            Assertions.assertEquals(HADOOP_GCS_RESOLVER, options.get(CatalogProperties.FILE_IO_IMPL));
        } finally {
            Config.hadoop_config_dir = previousDirectory;
        }
    }

    @Test
    void jdbcGenericResolverPreservesHmac() throws Exception {
        Map<String, String> raw = hmacProperties();
        raw.put("uri", "jdbc:sqlite::memory:");
        raw.put("warehouse", "gs://test-bucket/warehouse");
        raw.put("iceberg.jdbc.catalog_name", "test");
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        AtomicReference<Map<String, String>> captured = new AtomicReference<>();
        IcebergJdbcMetaStoreProperties properties = new IcebergJdbcMetaStoreProperties(raw) {
            @Override
            protected Catalog buildIcebergCatalog(String name, Map<String, String> options, Configuration conf) {
                captured.set(new HashMap<>(options));
                return Mockito.mock(Catalog.class);
            }
        };
        properties.initNormalizeAndCheckProps();
        properties.initializeCatalog("test", ImmutableList.of(StorageProperties.createPrimary(raw)));
        Assertions.assertEquals(HADOOP_GCS_RESOLVER, captured.get().get(CatalogProperties.FILE_IO_IMPL));
    }

    @ParameterizedTest
    @ValueSource(strings = {"glue", "dlf", "s3tables"})
    void preservesGenericResolverBeforeFactoriesThatDoNotConvertStorageProperties(String catalogType) throws Exception {
        Map<String, String> raw = hmacProperties();
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        AtomicReference<Map<String, String>> captured = new AtomicReference<>();
        AbstractIcebergProperties properties = new AbstractIcebergProperties(raw) {
            @Override
            public String getIcebergCatalogType() {
                return catalogType;
            }

            @Override
            protected Catalog initCatalog(String name, Map<String, String> options, List<StorageProperties> storage) {
                captured.set(new HashMap<>(options));
                return Mockito.mock(Catalog.class);
            }
        };
        properties.initNormalizeAndCheckProps();
        properties.initializeCatalog("test", ImmutableList.of(StorageProperties.createPrimary(raw)));
        Assertions.assertEquals(HADOOP_GCS_RESOLVER, captured.get().get(CatalogProperties.FILE_IO_IMPL));
    }

    @Test
    void hiveGenericResolverPreservesHmac() throws Exception {
        Map<String, String> raw = hmacProperties();
        raw.put("hive.metastore.uris", "thrift://127.0.0.1:1");
        raw.put("warehouse", "gs://test-bucket/warehouse");
        raw.put(CatalogProperties.FILE_IO_IMPL, RESOLVER);
        IcebergHMSMetaStoreProperties properties = new IcebergHMSMetaStoreProperties(raw);
        properties.initNormalizeAndCheckProps();
        try (HiveCatalog catalog = (HiveCatalog) properties.initializeCatalog(
                "test", ImmutableList.of(StorageProperties.createPrimary(raw)))) {
            // Inspect the real Hive initialization path, which does not use toFileIOProperties.
            Field field = HiveCatalog.class.getDeclaredField("fileIO");
            field.setAccessible(true);
            // Older HiveCatalog implementations do not own FileIO close; the fixture must release it too.
            try (FileIO io = (FileIO) field.get(catalog)) {
                Assertions.assertEquals(HADOOP_GCS_RESOLVER, io.getClass().getName());
            }
        }
    }

    private static Map<String, String> restProperties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("iceberg.rest.uri", "https://catalog.example.com");
        return raw;
    }

    private static Map<String, String> hmacProperties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("gs.endpoint", "https://storage.googleapis.com");
        raw.put("gs.access_key", "test-access-key");
        raw.put("gs.secret_key", "test-secret-key");
        return raw;
    }
}
