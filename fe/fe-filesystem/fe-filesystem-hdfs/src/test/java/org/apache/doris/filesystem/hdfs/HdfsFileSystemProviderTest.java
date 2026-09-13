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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.hdfs.properties.HdfsProperties;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Plain HDFS must own only hdfs/viewfs. jfs:// (fe-core marker "HDFS") belongs to the jfs plugin,
 * oss:// / OSS_HDFS marker to the oss-hdfs plugin. Routing is first-match-wins over an unordered
 * provider list, so these predicates must stay disjoint.
 */
class HdfsFileSystemProviderTest {

    private final HdfsFileSystemProvider provider = new HdfsFileSystemProvider();

    private Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void claimsHdfsAndViewfsSchemes() {
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "hdfs://ns")));
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "viewfs://cluster")));
    }

    @Test
    void claimsHdfsMarkerWithoutUri() {
        Assertions.assertTrue(provider.supports(props("_STORAGE_TYPE_", "HDFS")));
    }

    @Test
    void rejectsJfsScheme() {
        // jfs carries fe-core marker "HDFS" but belongs to the jfs plugin.
        Assertions.assertFalse(provider.supports(props("_STORAGE_TYPE_", "HDFS",
                "fs.defaultFS", "jfs://cluster")));
    }

    @Test
    void rejectsOssSchemeAndOssHdfsMarker() {
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "oss://bucket/p")));
        Assertions.assertFalse(provider.supports(props("_STORAGE_TYPE_", "OSS_HDFS")));
    }

    @Test
    void rejectsOfsScheme() {
        // ofs (Tencent CHDFS) is broker-routed by fe-core, never claimed by this SPI provider.
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "ofs://cluster/p")));
    }

    @Test
    void schemeMatchIsCaseInsensitive() {
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "HDFS://ns")));
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "ViewFS://cluster")));
    }

    @Test
    void defaultBindingKeepsTypedPropertiesAndConfiguration() {
        Map<String, String> raw = Map.of("azure.account_name", "account", "azure.account_key", "key");
        HdfsProperties normal = provider.bind(raw);
        HdfsProperties fallback = provider.bindDefault(raw);
        StorageProperties storage = fallback;

        Assertions.assertFalse(normal.isSyntheticDefault());
        Assertions.assertTrue(storage.isSyntheticDefault());
        Assertions.assertEquals(HdfsProperties.class, fallback.getClass());
        Assertions.assertEquals(raw, fallback.rawProperties());
        Assertions.assertEquals(normal.matchedProperties(), fallback.matchedProperties());
        Assertions.assertEquals(normal.toBackendProperties().get().toMap(),
                fallback.toBackendProperties().get().toMap());
        Assertions.assertEquals(normal.toHadoopProperties().get().toHadoopConfigurationMap(),
                fallback.toHadoopProperties().get().toHadoopConfigurationMap());
        Assertions.assertEquals(normal.fsCacheFingerprint(), fallback.fsCacheFingerprint());
    }

    @Test
    void normalBindingsNeverInferSyntheticOriginFromMissingHints() {
        Assertions.assertFalse(provider.bind(Map.of()).isSyntheticDefault());
        Assertions.assertFalse(provider.bind(Map.of("fs.hdfs.support", "true")).isSyntheticDefault());
        HdfsProperties hdfsUri = provider.bind(Map.of("uri", "hdfs://namenode/warehouse"));
        Assertions.assertFalse(hdfsUri.isSyntheticDefault());
        Assertions.assertEquals("hdfs://namenode", hdfsUri.getBackendConfigProperties().get("fs.defaultFS"));
    }

    @Test
    void recognizesAndLoadsActualHadoopResources(@TempDir Path tmp) throws Exception {
        Path xml = tmp.resolve("storage-site.xml");
        Files.writeString(xml, "<configuration><property>"
                + "<name>fs.s3a.connection.ssl.enabled</name><value>false</value>"
                + "</property></configuration>");
        Map<String, String> raw = Map.of("hadoop.config.resources", xml.toString());

        Assertions.assertTrue(provider.supportsGuess(raw));
        HdfsProperties properties = provider.bind(raw);

        Assertions.assertFalse(properties.isSyntheticDefault());
        Assertions.assertEquals("false", properties.getBackendConfigProperties().get("fs.s3a.connection.ssl.enabled"));
        Assertions.assertEquals("false",
                properties.toHadoopConfigurationMap().get("fs.s3a.connection.ssl.enabled"));
    }

    @Test
    void retainsLegacyResourceGuessHintWithoutLoadingFilesDuringGuess() {
        Assertions.assertTrue(provider.supportsGuess(Map.of("hdfs.config.resources", "legacy-site.xml")));
        Assertions.assertTrue(provider.supportsGuess(Map.of("hadoop.config.resources", "missing-site.xml")));
    }
}
