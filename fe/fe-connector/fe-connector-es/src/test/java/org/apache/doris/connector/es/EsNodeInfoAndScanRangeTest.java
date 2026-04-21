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

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

class EsNodeInfoAndScanRangeTest {

    // === EsNodeInfo seed constructor ===

    @Test
    void testSeedHostPort() {
        EsNodeInfo info = new EsNodeInfo("node1", "192.168.1.1:9200");
        Assertions.assertEquals("node1", info.getId());
        Assertions.assertEquals("192.168.1.1", info.getPublishHost());
        Assertions.assertEquals(9200, info.getPublishPort());
        Assertions.assertEquals("192.168.1.1:9200", info.getPublishAddress());
        Assertions.assertTrue(info.hasHttp());
        Assertions.assertTrue(info.isData());
    }

    @Test
    void testSeedWithHttpScheme() {
        EsNodeInfo info = new EsNodeInfo("n2", "http://es-node:9200");
        Assertions.assertEquals("http://es-node", info.getPublishHost());
        Assertions.assertEquals(9200, info.getPublishPort());
    }

    @Test
    void testSeedWithHttpsScheme() {
        EsNodeInfo info = new EsNodeInfo("n3", "https://es-node:9243");
        Assertions.assertEquals("https://es-node", info.getPublishHost());
        Assertions.assertEquals(9243, info.getPublishPort());
    }

    @Test
    void testSeedHostOnly() {
        EsNodeInfo info = new EsNodeInfo("n4", "es-host");
        Assertions.assertEquals("es-host", info.getPublishHost());
        Assertions.assertEquals(80, info.getPublishPort());
    }

    @Test
    void testSeedWithTrailingSlash() {
        EsNodeInfo info = new EsNodeInfo("n5", "http://host:9200/");
        Assertions.assertEquals("http://host", info.getPublishHost());
        Assertions.assertEquals(9200, info.getPublishPort());
    }

    @Test
    void testSeedEquality() {
        EsNodeInfo a = new EsNodeInfo("n1", "host:9200");
        EsNodeInfo b = new EsNodeInfo("n1", "host:9200");
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void testSeedInequality() {
        EsNodeInfo a = new EsNodeInfo("n1", "host:9200");
        EsNodeInfo b = new EsNodeInfo("n2", "host:9200");
        Assertions.assertNotEquals(a, b);
    }

    // === EsScanRange ===

    @Test
    void testScanRangeBasicProperties() {
        EsScanRange range = new EsScanRange("my_index", "_doc", 0,
                Arrays.asList("host1:9200", "host2:9200"));
        Assertions.assertEquals("my_index", range.getIndexName());
        Assertions.assertEquals("_doc", range.getMappingType());
        Assertions.assertEquals(0, range.getShardId());
        Assertions.assertEquals(2, range.getEsHosts().size());
    }

    @Test
    void testScanRangeType() {
        EsScanRange range = new EsScanRange("idx", null, 1, Collections.emptyList());
        Assertions.assertEquals(ConnectorScanRangeType.ES_SCAN, range.getRangeType());
    }

    @Test
    void testScanRangeGetProperties() {
        EsScanRange range = new EsScanRange("logs", "_doc", 3,
                Arrays.asList("a:9200", "b:9200"));
        Map<String, String> props = range.getProperties();
        Assertions.assertEquals("logs", props.get(EsScanRange.PROP_INDEX));
        Assertions.assertEquals("_doc", props.get(EsScanRange.PROP_TYPE));
        Assertions.assertEquals("3", props.get(EsScanRange.PROP_SHARD_ID));
        Assertions.assertEquals("a:9200,b:9200", props.get(EsScanRange.PROP_HOSTS));
    }

    @Test
    void testScanRangeNullMappingTypeOmitted() {
        EsScanRange range = new EsScanRange("idx", null, 0, Collections.emptyList());
        Map<String, String> props = range.getProperties();
        Assertions.assertFalse(props.containsKey(EsScanRange.PROP_TYPE));
    }

    @Test
    void testScanRangeNullHostsBecomesEmptyList() {
        EsScanRange range = new EsScanRange("idx", null, 0, null);
        Assertions.assertTrue(range.getEsHosts().isEmpty());
        Assertions.assertEquals("", range.getProperties().get(EsScanRange.PROP_HOSTS));
    }

    @Test
    void testScanRangeHostsAreUnmodifiable() {
        EsScanRange range = new EsScanRange("idx", null, 0,
                Arrays.asList("h1:9200"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> range.getEsHosts().add("h2:9200"));
    }

    @Test
    void testScanRangeGetHostsReturnsPlainHostnames() {
        EsScanRange range = new EsScanRange("idx", null, 0,
                Arrays.asList("a:9200", "b:9200"));
        // getHosts() returns plain hostnames for locality scheduling
        Assertions.assertEquals(Arrays.asList("a", "b"), range.getHosts());
        // getEsHosts() returns full host:port for BE
        Assertions.assertEquals(Arrays.asList("a:9200", "b:9200"), range.getEsHosts());
    }

    @Test
    void testScanRangeGetHostsStripsScheme() {
        EsScanRange range = new EsScanRange("idx", null, 0,
                Arrays.asList("https://es1.example.com:9243", "http://es2.example.com:9200"));
        Assertions.assertEquals(Arrays.asList("es1.example.com", "es2.example.com"), range.getHosts());
    }

    @Test
    void testScanRangeGetHostsDeduplicates() {
        EsScanRange range = new EsScanRange("idx", null, 0,
                Arrays.asList("h1:9200", "h1:9201", "h2:9200"));
        // h1 appears twice (different ports) but should be deduplicated
        Assertions.assertEquals(Arrays.asList("h1", "h2"), range.getHosts());
    }
}
