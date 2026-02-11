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

package org.apache.doris.datasource.property.metastore;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergJdbcMetaStorePropertiesTest {

    @Test
    public void testBasicJdbcProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jdbc:mysql://localhost:3306/iceberg");
        props.put("warehouse", "s3://warehouse/path");
        props.put("jdbc.user", "iceberg");
        props.put("jdbc.password", "secret");

        IcebergJdbcMetaStoreProperties jdbcProps = new IcebergJdbcMetaStoreProperties(props);
        jdbcProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = jdbcProps.getIcebergJdbcCatalogProperties();
        Assertions.assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_JDBC,
                catalogProps.get(CatalogUtil.ICEBERG_CATALOG_TYPE));
        Assertions.assertEquals("jdbc:mysql://localhost:3306/iceberg", catalogProps.get(CatalogProperties.URI));
        Assertions.assertEquals("s3://warehouse/path", catalogProps.get(CatalogProperties.WAREHOUSE_LOCATION));
        Assertions.assertEquals("iceberg", catalogProps.get("jdbc.user"));
        Assertions.assertEquals("secret", catalogProps.get("jdbc.password"));
    }

    @Test
    public void testJdbcPrefixPassthrough() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jdbc:mysql://localhost:3306/iceberg");
        props.put("warehouse", "s3://warehouse/path");
        props.put("jdbc.useSSL", "true");
        props.put("jdbc.verifyServerCertificate", "true");

        IcebergJdbcMetaStoreProperties jdbcProps = new IcebergJdbcMetaStoreProperties(props);
        jdbcProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = jdbcProps.getIcebergJdbcCatalogProperties();
        Assertions.assertEquals("true", catalogProps.get("jdbc.useSSL"));
        Assertions.assertEquals("true", catalogProps.get("jdbc.verifyServerCertificate"));
    }

    @Test
    public void testMissingWarehouse() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jdbc:mysql://localhost:3306/iceberg");

        IcebergJdbcMetaStoreProperties jdbcProps = new IcebergJdbcMetaStoreProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, jdbcProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testMissingUri() {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", "s3://warehouse/path");

        IcebergJdbcMetaStoreProperties jdbcProps = new IcebergJdbcMetaStoreProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, jdbcProps::initNormalizeAndCheckProps);
    }
}
