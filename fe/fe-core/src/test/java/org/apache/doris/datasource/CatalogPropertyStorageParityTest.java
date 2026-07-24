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

package org.apache.doris.datasource;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Phase B2 golden test: CatalogProperty's SPI-facade adapter track must merge the exact same
 * backend / Hadoop property maps the legacy typed track produced (legacy stays the oracle until
 * Phase D). The oracle below merges over the legacy createAll ORDERED list (HDFS pad first,
 * explicit providers after). The pre-flip CatalogProperty merged over an enum-keyed HashMap
 * whose observed iteration order equals this list order; the adapter track pins that order
 * down deterministically via a LinkedHashMap in binding order.
 */
public class CatalogPropertyStorageParityTest {

    private static Map<String, String> legacyBackendMerge(Map<String, String> props) throws UserException {
        Map<String, String> result = new HashMap<>();
        List<StorageProperties> all = StorageProperties.createAll(props);
        for (StorageProperties sp : all) {
            sp.getBackendConfigProperties().entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .forEach(e -> result.put(e.getKey(), e.getValue()));
        }
        return result;
    }

    private static Map<String, String> legacyHadoopMerge(Map<String, String> props) throws UserException {
        Map<String, String> result = new HashMap<>();
        for (StorageProperties sp : StorageProperties.createAll(props)) {
            Configuration configuration = sp.getHadoopStorageConfig();
            if (configuration != null) {
                configuration.forEach(entry -> {
                    if (entry.getValue() != null) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                });
            }
        }
        return result;
    }

    private static void assertTracksAgree(Map<String, String> props) throws UserException {
        CatalogProperty catalogProperty = new CatalogProperty(null, new HashMap<>(props));
        assertMapAgree(legacyBackendMerge(props), catalogProperty.getBackendStorageProperties(),
                "backend map, props=" + props);
        assertMapAgree(legacyHadoopMerge(props), catalogProperty.getHadoopProperties(),
                "hadoop map, props=" + props);
    }

    /** assertEquals over maps, but the failure message lists only the differing keys. */
    private static void assertMapAgree(Map<String, String> expected, Map<String, String> actual, String label) {
        StringBuilder diff = new StringBuilder();
        for (Map.Entry<String, String> e : new TreeMap<>(expected).entrySet()) {
            String actualValue = actual.get(e.getKey());
            if (!e.getValue().equals(actualValue)) {
                diff.append("\n  ").append(e.getKey()).append(": legacy=").append(e.getValue())
                        .append(" adapter=").append(actualValue);
            }
        }
        for (String key : new TreeMap<>(actual).keySet()) {
            if (!expected.containsKey(key)) {
                diff.append("\n  ").append(key).append(": legacy=<absent> adapter=").append(actual.get(key));
            }
        }
        if (diff.length() > 0) {
            Assertions.fail(label + diff);
        }
    }

    @Test
    public void testHdfsCatalogTracksAgree() throws UserException {
        assertTracksAgree(ImmutableMap.of(
                "type", "hms",
                "hive.metastore.uris", "thrift://127.0.0.1:9083",
                "hadoop.username", "hadoop",
                "fs.defaultFS", "hdfs://nameservice1"));
    }

    @Test
    public void testS3CatalogTracksAgree() throws UserException {
        assertTracksAgree(ImmutableMap.of(
                "type", "hms",
                "hive.metastore.uris", "thrift://127.0.0.1:9083",
                "s3.endpoint", "s3.us-east-1.amazonaws.com",
                "s3.access_key", "ak",
                "s3.secret_key", "sk"));
    }

    @Test
    public void testOssCatalogTracksAgree() throws UserException {
        assertTracksAgree(ImmutableMap.of(
                "type", "hms",
                "hive.metastore.uris", "thrift://127.0.0.1:9083",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk"));
    }

    @Test
    public void testAmbiguousMultiHitTracksAgree() throws UserException {
        // Double-hit quirk frozen by StoragePropertiesRoutingParityTest: an aliyuncs OSS endpoint
        // plus an s3.region matches both OSS and S3 (with the default HDFS pad at index 0).
        assertTracksAgree(ImmutableMap.of(
                "type", "hms",
                "hive.metastore.uris", "thrift://127.0.0.1:9083",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "s3.region", "us-east-1"));
    }

    @Test
    public void testAdapterOrderMirrorsLegacyOrder() throws UserException {
        Map<String, String> props = ImmutableMap.of(
                "type", "hms",
                "hive.metastore.uris", "thrift://127.0.0.1:9083",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "s3.region", "us-east-1");
        CatalogProperty catalogProperty = new CatalogProperty(null, new HashMap<>(props));
        List<StorageProperties> legacy = StorageProperties.createAll(new HashMap<>(props));
        List<StorageAdapter> adapters = catalogProperty.getOrderedStorageAdapters();
        Assertions.assertEquals(legacy.size(), adapters.size());
        for (int i = 0; i < legacy.size(); i++) {
            Assertions.assertEquals(legacy.get(i).getType().name(), adapters.get(i).getType().name(),
                    "index " + i);
        }
        Assertions.assertEquals(StorageTypeId.HDFS, adapters.get(0).getType());
    }
}
