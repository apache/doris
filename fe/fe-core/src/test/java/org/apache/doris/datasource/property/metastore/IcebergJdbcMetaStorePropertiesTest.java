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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IcebergJdbcMetaStorePropertiesTest {

    private static class CapturingIcebergJdbcMetaStoreProperties extends IcebergJdbcMetaStoreProperties {
        private String capturedCatalogName;
        private Map<String, String> capturedOptions;

        CapturingIcebergJdbcMetaStoreProperties(Map<String, String> props) {
            super(props);
        }

        @Override
        protected Catalog buildIcebergCatalog(String catalogName, Map<String, String> options, Configuration conf) {
            capturedCatalogName = catalogName;
            capturedOptions = new HashMap<>(options);
            return Mockito.mock(Catalog.class);
        }

        String getCapturedCatalogName() {
            return capturedCatalogName;
        }

        Map<String, String> getCapturedOptions() {
            return capturedOptions;
        }
    }

    @Test
    public void testBasicJdbcProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jdbc:mysql://localhost:3306/iceberg");
        props.put("warehouse", "s3://warehouse/path");
        props.put("jdbc.user", "iceberg");
        props.put("jdbc.password", "secret");
        props.put("iceberg.jdbc.catalog_name", "iceberg_catalog");

        IcebergJdbcMetaStoreProperties jdbcProps = new IcebergJdbcMetaStoreProperties(props);
        jdbcProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = jdbcProps.getIcebergJdbcCatalogProperties();
        Assertions.assertEquals(CatalogUtil.ICEBERG_CATALOG_JDBC,
                catalogProps.get(CatalogProperties.CATALOG_IMPL));
        Assertions.assertEquals("jdbc:mysql://localhost:3306/iceberg", catalogProps.get(CatalogProperties.URI));
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
        props.put("iceberg.jdbc.catalog_name", "iceberg_catalog");

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

    @Test
    public void testJdbcCatalogNameOverridesSdkCatalogName() {
        Map<String, String> props = createBaseProps();
        props.put("iceberg.jdbc.catalog_name", "spark_catalog");

        CapturingIcebergJdbcMetaStoreProperties jdbcProps = new CapturingIcebergJdbcMetaStoreProperties(props);
        jdbcProps.initNormalizeAndCheckProps();
        jdbcProps.initializeCatalog("doris_catalog", Collections.emptyList());

        Assertions.assertEquals("spark_catalog", jdbcProps.getCapturedCatalogName());
        Assertions.assertFalse(jdbcProps.getCapturedOptions().containsKey("iceberg.jdbc.catalog_name"));
    }

    @Test
    public void testMissingJdbcCatalogNameThrowsException() {
        CapturingIcebergJdbcMetaStoreProperties jdbcProps =
                new CapturingIcebergJdbcMetaStoreProperties(createBaseProps());

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, jdbcProps::initNormalizeAndCheckProps);
        Assertions.assertEquals("Property iceberg.jdbc.catalog_name is required.", exception.getMessage());
    }

    @Test
    public void testBlankJdbcCatalogNameThrowsException() {
        Map<String, String> props = createBaseProps();
        props.put("iceberg.jdbc.catalog_name", " ");

        CapturingIcebergJdbcMetaStoreProperties jdbcProps = new CapturingIcebergJdbcMetaStoreProperties(props);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, jdbcProps::initNormalizeAndCheckProps);
        Assertions.assertEquals("Property iceberg.jdbc.catalog_name is required.", exception.getMessage());
    }

    private static Map<String, String> createBaseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jdbc:mysql://localhost:3306/iceberg");
        props.put("warehouse", "s3://warehouse/path");
        return props;
    }
}
