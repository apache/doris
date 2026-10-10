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

import org.apache.doris.filesystem.gcs.GcsFileSystemProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class IcebergConnectorGlueFileIOTest {

    @Test
    void gluePassesNativeGcsConfigurationToHadoopFileIO(@TempDir java.nio.file.Path temp) throws Exception {
        java.nio.file.Path metadata = temp.resolve("metadata.json");
        Files.writeString(metadata, "metadata", StandardCharsets.UTF_8);
        for (String source : List.of("DEFAULT", "COMPUTE_ENGINE")) {
            for (String account : List.of("", "reader@test.iam.gserviceaccount.com")) {
                Map<String, String> properties = new HashMap<>(Map.of(
                        "iceberg.catalog.type", "glue",
                        "provider", "GCP",
                        "warehouse", "gs://test-bucket/warehouse",
                        "gs.credential_provider_type", source,
                        "gs.impersonation_service_account", account,
                        "glue.access_key", "test-ak",
                        "glue.secret_key", "test-sk",
                        "glue.region", "us-east-1",
                        "glue.endpoint", "http://127.0.0.1:1"));
                // Replace only the Hadoop transport. Keep the real connector, GlueCatalog,
                // table operations and ResolvingFileIO -> HadoopFileIO dispatch.
                properties.put("fs.gs.impl", LocalGcsFileSystem.class.getName());
                properties.put("fs.gs.impl.disable.cache", "true");
                RecordingConnectorContext context = new RecordingConnectorContext();
                context.storageProperties = List.of(GcsFileSystemProperties.of(properties));
                try (IcebergConnector connector = new IcebergConnector(properties, context)) {
                    connector.getMetadata(null);
                    Field catalogField = IcebergConnector.class.getDeclaredField("icebergCatalog");
                    catalogField.setAccessible(true);
                    GlueCatalog catalog = (GlueCatalog) catalogField.get(connector);
                    // Construct table IO without contacting Glue to fetch table metadata.
                    Method newTableOps = GlueCatalog.class.getDeclaredMethod("newTableOps", TableIdentifier.class);
                    newTableOps.setAccessible(true);
                    TableOperations operations = (TableOperations) newTableOps.invoke(
                            catalog, TableIdentifier.of("db", "table"));
                    FileIO io = operations.io();
                    Assertions.assertInstanceOf(ResolvingFileIO.class, io);
                    Configuration conf = ((ResolvingFileIO) io).getConf();
                    Assertions.assertNotNull(conf, "Glue must receive the bound storage Hadoop configuration");
                    String authType = "DEFAULT".equals(source) ? "APPLICATION_DEFAULT" : "COMPUTE_ENGINE";
                    Assertions.assertEquals(authType, conf.get("fs.gs.auth.type"));
                    Assertions.assertEquals(account, conf.get("fs.gs.auth.impersonation.service.account"));
                    Assertions.assertEquals("https://storage.googleapis.com/", conf.get("fs.gs.storage.root.url"));

                    HadoopInputFile input = Assertions.assertInstanceOf(HadoopInputFile.class,
                            io.newInputFile(new Path("gs", "test-bucket", metadata.toString()).toString()));
                    try (FileSystem fs = input.getFileSystem(); SeekableInputStream stream = input.newStream()) {
                        Assertions.assertInstanceOf(LocalGcsFileSystem.class, fs);
                        Assertions.assertEquals(authType, fs.getConf().get("fs.gs.auth.type"));
                        Assertions.assertEquals(account,
                                fs.getConf().get("fs.gs.auth.impersonation.service.account"));
                        Assertions.assertEquals("metadata", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
                    }
                }
            }
        }
    }

    /** Serves gs://test-bucket paths from the local test directory without cloud credentials or I/O. */
    public static class LocalGcsFileSystem extends RawLocalFileSystem {
        @Override
        public URI getUri() {
            return URI.create("gs://test-bucket/");
        }
    }
}
