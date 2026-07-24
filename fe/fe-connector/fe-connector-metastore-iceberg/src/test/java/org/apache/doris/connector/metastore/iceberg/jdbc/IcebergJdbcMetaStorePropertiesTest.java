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

package org.apache.doris.connector.metastore.iceberg.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Parity for the iceberg JDBC backend (legacy {@code IcebergJdbcMetaStoreProperties} required props +
 * warehouse check): verbatim §4 messages, in fire order uri → catalog_name → warehouse.
 */
public class IcebergJdbcMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String validateError(Map<String, String> raw) {
        return Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergJdbcMetaStoreProperties.of(raw).validate()).getMessage();
    }

    @Test
    public void requiredInOrderUriCatalogNameWarehouse() {
        Assertions.assertEquals("Property uri is required.", validateError(raw()));
        Assertions.assertEquals("Property iceberg.jdbc.catalog_name is required.",
                validateError(raw("uri", "jdbc:postgresql://h/db")));
        Assertions.assertEquals("Property warehouse is required.",
                validateError(raw("uri", "jdbc:postgresql://h/db", "iceberg.jdbc.catalog_name", "c")));
    }

    @Test
    public void validWithUriCatalogNameWarehouse() {
        IcebergJdbcMetaStoreProperties props = IcebergJdbcMetaStoreProperties.of(raw(
                "uri", "jdbc:postgresql://h/db", "iceberg.jdbc.catalog_name", "c", "warehouse", "s3://b/wh"));
        props.validate();
        Assertions.assertEquals("JDBC", props.providerName());
    }

    @Test
    public void uriAliasResolvesFromIcebergJdbcUri() {
        // names = {"uri", "iceberg.jdbc.uri"}: iceberg.jdbc.uri satisfies the uri requirement.
        IcebergJdbcMetaStoreProperties.of(raw(
                "iceberg.jdbc.uri", "jdbc:postgresql://h/db", "iceberg.jdbc.catalog_name", "c",
                "warehouse", "s3://b/wh")).validate();
    }
}
