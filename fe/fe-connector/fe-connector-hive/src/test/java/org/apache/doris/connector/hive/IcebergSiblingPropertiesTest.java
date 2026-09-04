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
 * Tests the pure iceberg-sibling property synthesis. Each case pins WHY the behavior matters for the embedded
 * iceberg-on-HMS connector the flipped gateway builds from the returned map.
 */
public class IcebergSiblingPropertiesTest {

    @Test
    public void injectsHmsFlavor() {
        // Without iceberg.catalog.type the sibling's createCatalog() throws "Missing 'iceberg.catalog.type'";
        // an iceberg-on-HMS table is always served by the hms flavor.
        Map<String, String> in = new HashMap<>();
        in.put("hive.metastore.uris", "thrift://host:9083");

        Map<String, String> out = IcebergSiblingProperties.synthesize(in);

        Assertions.assertEquals("hms", out.get("iceberg.catalog.type"),
                "the sibling must resolve the hms flavor");
    }

    @Test
    public void carriesMetastoreStorageAndKerberosKeys() {
        // The sibling connects to the SAME metastore/storage as the gateway; dropping any of these keys would
        // leave the embedded HiveCatalog unable to reach HMS or object storage. The uri short form must survive
        // too (the iceberg HMS parser binds both hive.metastore.uris and uri).
        Map<String, String> in = new HashMap<>();
        in.put("uri", "thrift://host:9083");
        in.put("fs.s3a.access.key", "AK");
        in.put("dfs.nameservices", "ns1");
        in.put("hadoop.security.authentication", "kerberos");
        in.put("hive.metastore.client.principal", "hive/_HOST@REALM");
        in.put("hive.conf.resources", "hive-site.xml");

        Map<String, String> out = IcebergSiblingProperties.synthesize(in);

        Assertions.assertEquals("thrift://host:9083", out.get("uri"), "the uri short form must be carried");
        Assertions.assertEquals("AK", out.get("fs.s3a.access.key"), "object-storage creds must be carried");
        Assertions.assertEquals("ns1", out.get("dfs.nameservices"), "HDFS-HA config must be carried");
        Assertions.assertEquals("kerberos", out.get("hadoop.security.authentication"),
                "kerberos auth mode must be carried");
        Assertions.assertEquals("hive/_HOST@REALM", out.get("hive.metastore.client.principal"),
                "kerberos principal must be carried");
        Assertions.assertEquals("hive-site.xml", out.get("hive.conf.resources"),
                "external hive-site.xml reference must be carried");
        Assertions.assertEquals("hms", out.get("iceberg.catalog.type"));
    }

    @Test
    public void doesNotMutateInput() {
        // The gateway holds its catalog properties unmodifiable and shared; mutating them would corrupt the
        // gateway's own hive path.
        Map<String, String> in = new HashMap<>();
        in.put("hive.metastore.uris", "thrift://host:9083");

        IcebergSiblingProperties.synthesize(in);

        Assertions.assertFalse(in.containsKey("iceberg.catalog.type"), "the input map must not be mutated");
        Assertions.assertEquals(1, in.size());
    }

    @Test
    public void overridesAnyPreexistingFlavor() {
        // Defensive: even a stray iceberg.catalog.type on the gateway catalog must not select a non-hms flavor
        // for the iceberg-on-HMS sibling.
        Map<String, String> in = new HashMap<>();
        in.put("iceberg.catalog.type", "rest");

        Map<String, String> out = IcebergSiblingProperties.synthesize(in);

        Assertions.assertEquals("hms", out.get("iceberg.catalog.type"),
                "the iceberg-on-HMS sibling is always the hms flavor, overriding any pre-existing value");
    }
}
