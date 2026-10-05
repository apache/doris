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

package org.apache.doris.connector.metastore.iceberg.hms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Parity for the iceberg HMS backend: it reuses the shared {@link
 * org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties} connection rules but — unlike
 * paimon — does NOT require {@code warehouse} (legacy {@code IcebergHMSMetaStoreProperties} →
 * {@code HMSBaseProperties.of}; §4 of the P6-T10 design). Conf ({@code toHiveConfOverrides}, used by the
 * connector via {@code bindForType("hms")}) comes from the shared base unchanged.
 */
public class IcebergHmsMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static IcebergHmsMetaStoreProperties of(Map<String, String> raw) {
        return IcebergHmsMetaStoreProperties.of(raw, Collections.emptyMap());
    }

    @Test
    public void validWithoutWarehouse() {
        // KEY iceberg-vs-paimon difference: iceberg HMS does NOT require warehouse. A bare uri validates.
        // MUTATION: if IcebergHms.validate() called requireWarehouse(), this would throw
        // "Property warehouse is required.".
        of(raw("hive.metastore.uris", "thrift://h:9083")).validate();
        Assertions.assertEquals("HMS", of(raw("hive.metastore.uris", "thrift://h")).providerName());
    }

    @Test
    public void uriRequiredFirstWithoutWarehouseCheck() {
        // No warehouse set, no uri set: the FIRST error is the uri rule (not a warehouse rule), proving
        // iceberg HMS skips requireWarehouse().
        Assertions.assertEquals("hive.metastore.uris or uri is required",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> of(raw()).validate()).getMessage());
    }

    @Test
    public void simpleAuthForbidsClientCredentials() {
        Assertions.assertEquals("hive.metastore.client.principal and hive.metastore.client.keytab cannot be set when "
                        + "hive.metastore.authentication.type is simple",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> of(raw("hive.metastore.uris", "thrift://h",
                                "hive.metastore.authentication.type", "simple",
                                "hive.metastore.client.principal", "p")).validate()).getMessage());
    }

    @Test
    public void kerberosAuthRequiresClientCredentials() {
        Assertions.assertEquals("hive.metastore.client.principal and hive.metastore.client.keytab are required when "
                        + "hive.metastore.authentication.type is kerberos",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> of(raw("hive.metastore.uris", "thrift://h",
                                "hive.metastore.authentication.type", "kerberos",
                                "hive.metastore.client.principal", "p")).validate()).getMessage());
    }

    @Test
    public void confComesFromSharedBaseUnchanged() {
        // The conf the connector layers onto its HiveConf (bindForType("hms").toHiveConfOverrides) is the
        // shared base's — identical to paimon's. Pin the uri + the default socket timeout.
        Map<String, String> conf = of(raw("hive.metastore.uris", "thrift://h:9083")).toHiveConfOverrides("10");
        Assertions.assertEquals("thrift://h:9083", conf.get("hive.metastore.uris"));
        Assertions.assertEquals("10", conf.get("hive.metastore.client.socket.timeout"));
    }
}
