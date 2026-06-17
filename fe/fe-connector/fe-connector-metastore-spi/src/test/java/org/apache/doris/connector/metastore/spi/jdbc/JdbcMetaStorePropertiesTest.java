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

package org.apache.doris.connector.metastore.spi.jdbc;

import org.apache.doris.connector.metastore.spi.JdbcDriverSupport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** T2 parity for the JDBC backend (legacy {@code PaimonJdbcMetaStoreProperties}) + driver-url resolution. */
public class JdbcMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void gettersReturnRawAliasResolvedValues() {
        JdbcMetaStorePropertiesImpl props = JdbcMetaStorePropertiesImpl.of(raw(
                "uri", "jdbc:mysql://h:3306/db",
                "paimon.jdbc.user", "u",
                "paimon.jdbc.password", "p",
                "paimon.jdbc.driver_url", "mysql-connector.jar",
                "paimon.jdbc.driver_class", "com.mysql.cj.jdbc.Driver",
                "warehouse", "wh"));

        Assertions.assertEquals("JDBC", props.providerName());
        Assertions.assertFalse(props.needsStorage());
        Assertions.assertEquals("jdbc:mysql://h:3306/db", props.getUri());
        Assertions.assertEquals("u", props.getUser());
        Assertions.assertEquals("p", props.getPassword());
        // RAW driver url (resolution is consumer-side via JdbcDriverSupport).
        Assertions.assertEquals("mysql-connector.jar", props.getDriverUrl());
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", props.getDriverClass());
    }

    @Test
    public void validateChecksWarehouseThenUriThenDriverClass() {
        Assertions.assertEquals("Property warehouse is required.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcMetaStorePropertiesImpl.of(raw("uri", "jdbc:x")).validate()).getMessage());
        Assertions.assertEquals("uri or paimon.jdbc.uri is required",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcMetaStorePropertiesImpl.of(raw("warehouse", "wh")).validate()).getMessage());
        Assertions.assertEquals("jdbc.driver_class or paimon.jdbc.driver_class is required when "
                        + "jdbc.driver_url or paimon.jdbc.driver_url is specified",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcMetaStorePropertiesImpl.of(raw(
                                "warehouse", "wh", "uri", "jdbc:x", "paimon.jdbc.driver_url", "d.jar")).validate())
                        .getMessage());
    }

    @Test
    public void resolveDriverUrl() {
        Map<String, String> env = new HashMap<>();
        // already scheme-bearing -> as-is
        Assertions.assertEquals("https://host/d.jar", JdbcDriverSupport.resolveDriverUrl("https://host/d.jar", env));
        // absolute path -> as-is (no driversDir prepend)
        Assertions.assertEquals("/opt/drivers/d.jar", JdbcDriverSupport.resolveDriverUrl("/opt/drivers/d.jar", env));
        // bare jar with explicit drivers dir
        env.put("jdbc_drivers_dir", "/custom/drivers");
        Assertions.assertEquals("file:///custom/drivers/d.jar", JdbcDriverSupport.resolveDriverUrl("d.jar", env));
        // bare jar falling back to doris_home/plugins/jdbc_drivers
        Map<String, String> env2 = new HashMap<>();
        env2.put("doris_home", "/dh");
        Assertions.assertEquals("file:///dh/plugins/jdbc_drivers/d.jar", JdbcDriverSupport.resolveDriverUrl("d.jar", env2));
        // empty env -> doris_home defaults to "."
        Assertions.assertEquals("file://./plugins/jdbc_drivers/d.jar",
                JdbcDriverSupport.resolveDriverUrl("d.jar", new HashMap<>()));
    }

    @Test
    public void uriPrefersFirstAlias() {
        // names = {"uri", "paimon.jdbc.uri"} -> the plain "uri" wins when both are set.
        Assertions.assertEquals("jdbc:a", JdbcMetaStorePropertiesImpl.of(raw(
                "uri", "jdbc:a", "paimon.jdbc.uri", "jdbc:b", "warehouse", "wh")).getUri());
    }
}
