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

import org.apache.doris.filesystem.hdfs.properties.HdfsProperties;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class IcebergGcsXmlFileIOTest {
    @ParameterizedTest
    @ValueSource(strings = {"", "io-impl", "iceberg.rest.io-impl"})
    void effectiveXmlConfigurationReachesTheRealRestFileIO(String alias, @TempDir Path temp) throws Exception {
        Path metadata = temp.resolve("metadata.json");
        Files.writeString(metadata, "fixture-metadata", StandardCharsets.UTF_8);
        Path resource = temp.resolve("gcs-site.xml");
        Configuration xml = new Configuration(false);
        xml.set("fs.gs.impl", LocalGcsFileSystem.class.getName());
        xml.setBoolean("fs.gs.impl.disable.cache", true);
        xml.set("fs.gs.fixture.identity", "fixture-xml");
        try (OutputStream output = Files.newOutputStream(resource)) {
            xml.writeXml(output);
        }
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/config", exchange -> {
            try (exchange) {
                byte[] body = "{\"defaults\":{},\"overrides\":{}}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
        try {
            Map<String, String> raw = new HashMap<>(Map.of("iceberg.catalog.type", "rest",
                    "iceberg.rest.uri", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "warehouse", "opaque-warehouse", "rest.auth.type", "none",
                    "fs.defaultFS", "hdfs://fixture-namenode/", "hadoop.config.resources", resource.toString()));
            if (!alias.isEmpty()) {
                raw.put(alias, ResolvingFileIO.class.getName());
            }
            HdfsProperties hdfs = new HdfsProperties(raw);
            hdfs.initNormalizeAndCheckProps();
            RecordingConnectorContext context = new RecordingConnectorContext();
            context.storageProperties = List.of(hdfs);
            try (IcebergConnector connector = new IcebergConnector(raw, context)) {
                connector.getMetadata(null);
                Field field = IcebergConnector.class.getDeclaredField("restSessionCatalog");
                field.setAccessible(true);
                FileIO actual = IcebergConnector.restCatalogFileIO(field.get(connector));
                ResolvingFileIO resolver = Assertions.assertInstanceOf(ResolvingFileIO.class, actual);
                String location = new org.apache.hadoop.fs.Path("gs", "fixture-bucket", metadata.toString()).toString();
                // Check before opening: a broken route must not consult operator ADC or a real bucket.
                Assertions.assertEquals("org.apache.iceberg.hadoop.HadoopFileIO",
                        resolver.ioClass(location).getName());
                try (FileIO restored = SerializationUtil.deserializeFromBytes(
                        SerializationUtil.serializeToBytes(actual))) {
                    HadoopInputFile input = Assertions.assertInstanceOf(HadoopInputFile.class,
                            restored.newInputFile(location));
                    try (FileSystem fs = input.getFileSystem(); SeekableInputStream stream = input.newStream()) {
                        Assertions.assertInstanceOf(LocalGcsFileSystem.class, fs);
                        Assertions.assertEquals("fixture-xml", fs.getConf().get("fs.gs.fixture.identity"));
                        Assertions.assertEquals("fixture-metadata", new String(stream.readAllBytes(),
                                StandardCharsets.UTF_8));
                    }
                }
            }
        } finally {
            server.stop(0);
        }
    }

    public static class LocalGcsFileSystem extends RawLocalFileSystem {
        @Override
        public URI getUri() {
            return URI.create("gs://fixture-bucket/");
        }
    }
}
