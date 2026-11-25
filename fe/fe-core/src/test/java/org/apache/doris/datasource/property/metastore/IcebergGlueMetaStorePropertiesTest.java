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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergGlueMetaStorePropertiesTest {

    @Test
    public void glueTest() throws UserException {
        Map<String, String> baseProps = ImmutableMap.of(
                "type", "iceberg",
                "iceberg.catalog.type", "glue",
                "glue.region", "us-west-2",
                "glue.access_key", "AK",
                "glue.secret_key", "SK",
                "glue.endpoint", "https://glue.us-west-2.amazonaws.com",
                "warehouse", "s3://my-bucket/warehouse");
        IcebergGlueMetaStoreProperties properties = (IcebergGlueMetaStoreProperties) MetastoreProperties.create(baseProps);
        Assertions.assertEquals("glue", properties.getIcebergCatalogType());
        Catalog catalog = properties.initializeCatalog("iceberg_catalog", StorageProperties.createAll(baseProps));
        Assertions.assertEquals(GlueCatalog.class, catalog.getClass());
    }

    @Test
    public void glueAndS3Test() throws UserException {
        Map<String, String> baseProps = ImmutableMap.of(
                "type", "iceberg",
                "iceberg.catalog.type", "glue",
                "glue.region", "us-west-2",
                "glue.access_key", "AK",
                "glue.secret_key", "SK",
                "glue.endpoint", "https://glue.us-west-2.amazonaws.com",
                "warehouse", "s3://my-bucket/warehouse",
                "s3.region", "us-west-2",
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com"
        );
        IcebergGlueMetaStoreProperties properties = (IcebergGlueMetaStoreProperties) MetastoreProperties.create(baseProps);
        Catalog catalog = properties.initializeCatalog("iceberg_catalog", StorageProperties.createAll(baseProps));
        Assertions.assertEquals(GlueCatalog.class, catalog.getClass());
    }

    @ParameterizedTest
    @ValueSource(strings = {"hmac", "xml", "sdk-token"})
    void actualGlueFileIoRetainsEffectiveGcsConfiguration(String mode, @TempDir Path directory) throws Exception {
        Map<String, String> raw = new HashMap<>();
        raw.put("type", "iceberg");
        raw.put("iceberg.catalog.type", "glue");
        raw.put("glue.region", "us-east-1");
        raw.put("glue.endpoint", "https://glue.us-east-1.amazonaws.com");
        raw.put("glue.access_key", "fixture-glue-key");
        raw.put("glue.secret_key", "fixture-glue-secret");
        raw.put(CatalogProperties.FILE_IO_IMPL, ResolvingFileIO.class.getName());
        if ("hmac".equals(mode) || "native-default".equals(mode)) {
            // Keep Glue's legacy S3 property syntax; native-provider selectors are not accepted here.
            raw.put("gs.endpoint", "https://storage.googleapis.com");
        }
        if ("hmac".equals(mode)) {
            raw.put("gs.access_key", "fixture-storage-key");
            raw.put("gs.secret_key", "fixture-storage-secret");
        }
        if ("sdk-token".equals(mode)) {
            raw.put("gcs.oauth2.token", "fixture-sdk-token");
        }
        String previousDirectory = Config.hadoop_config_dir;
        try {
            if ("xml".equals(mode) || "sdk-token".equals(mode)) {
                Configuration resource = new Configuration(false);
                resource.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
                resource.set("fs.gs.auth.type", "APPLICATION_DEFAULT");
                resource.set("fs.gs.auth.impersonation.service.account", "target@fixture.iam.gserviceaccount.com");
                resource.set("fixture.resource.marker", "loaded-from-xml");
                try (OutputStream output = Files.newOutputStream(directory.resolve("gcs-site.xml"))) {
                    resource.writeXml(output);
                }
                Config.hadoop_config_dir = directory.toString() + "/";
                raw.put("fs.hdfs.support", "true");
                raw.put("hadoop.config.resources", "gcs-site.xml");
            }
            IcebergGlueMetaStoreProperties properties =
                    (IcebergGlueMetaStoreProperties) MetastoreProperties.create(raw);
            List<StorageProperties> storage = StorageProperties.createAll(raw);
            try (GlueCatalog catalog = (GlueCatalog) properties.initializeCatalog("fixture", storage)) {
                // Exercise the actual SDK factory and lazy TableOperations/FileIO chain without a Glue request.
                Method newTableOps = GlueCatalog.class.getDeclaredMethod("newTableOps", TableIdentifier.class);
                newTableOps.setAccessible(true);
                TableOperations operations = (TableOperations) newTableOps.invoke(catalog,
                        TableIdentifier.of("fixture_namespace", "fixture_table"));
                // Metadata scanners deserialize FileIO; its Hadoop identity must survive that handoff.
                try (FileIO original = operations.io();
                        FileIO io = SerializationUtil.deserializeFromBytes(SerializationUtil.serializeToBytes(original))) {
                    ResolvingFileIO resolver = (ResolvingFileIO) io;
                    Method resolve = ResolvingFileIO.class.getDeclaredMethod("io", String.class);
                    resolve.setAccessible(true);
                    FileIO delegate = (FileIO) resolve.invoke(resolver, "gs://fixture-bucket/metadata.json");
                    if ("sdk-token".equals(mode)) {
                        Assertions.assertEquals(ResolvingFileIO.class, resolver.getClass());
                        Assertions.assertEquals("org.apache.iceberg.gcp.gcs.GCSFileIO", delegate.getClass().getName());
                        Assertions.assertEquals("fixture-sdk-token", delegate.properties().get("gcs.oauth2.token"));
                    } else {
                        Assertions.assertEquals("org.apache.iceberg.io.NativeGcsResolvingFileIO",
                                resolver.getClass().getName());
                        Assertions.assertTrue(delegate instanceof HadoopFileIO);
                        Configuration conf = ((HadoopFileIO) delegate).getConf();
                        if ("hmac".equals(mode)) {
                            Assertions.assertEquals("fixture-storage-key", conf.get("fs.s3a.access.key"));
                            Assertions.assertEquals("fixture-storage-secret", conf.get("fs.s3a.secret.key"));
                        } else {
                            Assertions.assertEquals("APPLICATION_DEFAULT", conf.get("fs.gs.auth.type"));
                            if ("xml".equals(mode)) {
                                Assertions.assertEquals("loaded-from-xml", conf.get("fixture.resource.marker"));
                                Assertions.assertEquals("target@fixture.iam.gserviceaccount.com",
                                        conf.get("fs.gs.auth.impersonation.service.account"));
                            }
                        }
                        Assertions.assertEquals(conf.get("fs.gs.impl"), resolver.getConf().get("fs.gs.impl"));
                    }
                }
            }
        } finally {
            Config.hadoop_config_dir = previousDirectory;
        }
    }
}
