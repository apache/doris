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

package org.apache.doris.connector.hive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the pure hudi-sibling property synthesis. Unlike iceberg there is no flavor key to inject: hudi has no
 * {@code iceberg.catalog.type} analogue, so synthesis is a verbatim defensive copy of the gateway's catalog map.
 */
public class HudiSiblingPropertiesTest {

    @Test
    public void carriesMetastoreStorageAndKerberosKeysVerbatim() {
        // The sibling connects to the SAME metastore/storage as the gateway; dropping any of these keys would
        // leave the embedded hudi connector unable to reach HMS or object storage. The uri short form must
        // survive too (HudiConnector.createClient reads both hive.metastore.uris and uri).
        Map<String, String> in = new HashMap<>();
        in.put("uri", "thrift://host:9083");
        in.put("fs.s3a.access.key", "AK");
        in.put("dfs.nameservices", "ns1");
        in.put("hadoop.security.authentication", "kerberos");
        in.put("hive.metastore.client.principal", "hive/_HOST@REALM");

        Map<String, String> out = HudiSiblingProperties.synthesize(in);

        Assertions.assertEquals("thrift://host:9083", out.get("uri"), "the uri short form must be carried");
        Assertions.assertEquals("AK", out.get("fs.s3a.access.key"), "object-storage creds must be carried");
        Assertions.assertEquals("ns1", out.get("dfs.nameservices"), "HDFS-HA config must be carried");
        Assertions.assertEquals("kerberos", out.get("hadoop.security.authentication"),
                "kerberos auth mode must be carried");
        Assertions.assertEquals("hive/_HOST@REALM", out.get("hive.metastore.client.principal"),
                "kerberos principal must be carried");
    }

    @Test
    public void injectsNoFlavorKey() {
        // Hudi has no iceberg.catalog.type analogue; synthesis must NOT invent one. The output equals the input.
        Map<String, String> in = new HashMap<>();
        in.put("hive.metastore.uris", "thrift://host:9083");

        Map<String, String> out = HudiSiblingProperties.synthesize(in);

        Assertions.assertFalse(out.containsKey("iceberg.catalog.type"),
                "hudi synthesis must inject no flavor key");
        Assertions.assertEquals(in, out, "synthesis is a verbatim copy of the gateway property map");
    }

    @Test
    public void returnsDefensiveCopyThatDoesNotMutateInput() {
        // The gateway holds its catalog properties unmodifiable and shared; the returned map must be a distinct
        // instance so a later mutation by the sibling connector cannot corrupt the gateway's own hive path.
        Map<String, String> in = new HashMap<>();
        in.put("hive.metastore.uris", "thrift://host:9083");

        Map<String, String> out = HudiSiblingProperties.synthesize(in);
        out.put("extra.key", "v");

        Assertions.assertNotSame(in, out, "synthesis must return a NEW map, not alias the gateway's");
        Assertions.assertFalse(in.containsKey("extra.key"),
                "mutating the synthesized map must not affect the gateway's input map");
    }
}
