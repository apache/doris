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

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;

import org.apache.paimon.options.CatalogOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PaimonJdbcMetaStorePropertiesTest {

    @Test
    public void testBasicJdbcProperties() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "s3://warehouse/path");
        props.put("paimon.jdbc.user", "paimon");
        props.put("paimon.jdbc.password", "secret");

        PaimonJdbcMetaStoreProperties jdbcProps = (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        jdbcProps.initNormalizeAndCheckProps();
        jdbcProps.buildCatalogOptions();

        Assertions.assertEquals(PaimonExternalCatalog.PAIMON_JDBC, jdbcProps.getPaimonCatalogType());
        Assertions.assertEquals("jdbc", jdbcProps.getCatalogOptions().get(CatalogOptions.METASTORE.key()));
        Assertions.assertEquals("jdbc:mysql://localhost:3306/paimon",
                jdbcProps.getCatalogOptions().get(CatalogOptions.URI.key()));
        Assertions.assertEquals("paimon", jdbcProps.getCatalogOptions().get("jdbc.user"));
        Assertions.assertEquals("secret", jdbcProps.getCatalogOptions().get("jdbc.password"));
    }

    @Test
    public void testJdbcPrefixPassthrough() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "s3://warehouse/path");
        props.put("paimon.jdbc.useSSL", "true");
        props.put("paimon.jdbc.verifyServerCertificate", "true");

        PaimonJdbcMetaStoreProperties jdbcProps = (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        jdbcProps.initNormalizeAndCheckProps();
        jdbcProps.buildCatalogOptions();

        Assertions.assertEquals("true", jdbcProps.getCatalogOptions().get("jdbc.useSSL"));
        Assertions.assertEquals("true", jdbcProps.getCatalogOptions().get("jdbc.verifyServerCertificate"));
    }

    @Test
    public void testRawJdbcPrefixPassthrough() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "s3://warehouse/path");
        props.put("jdbc.user", "raw_user");
        props.put("jdbc.password", "raw_password");
        props.put("jdbc.useSSL", "true");
        props.put("jdbc.verifyServerCertificate", "true");

        PaimonJdbcMetaStoreProperties jdbcProps = (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        jdbcProps.initNormalizeAndCheckProps();
        jdbcProps.buildCatalogOptions();

        Assertions.assertEquals("raw_user", jdbcProps.getCatalogOptions().get("jdbc.user"));
        Assertions.assertEquals("raw_password", jdbcProps.getCatalogOptions().get("jdbc.password"));
        Assertions.assertEquals("true", jdbcProps.getCatalogOptions().get("jdbc.useSSL"));
        Assertions.assertEquals("true", jdbcProps.getCatalogOptions().get("jdbc.verifyServerCertificate"));
    }

    @Test
    public void testFactoryCreateJdbcType() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "s3://warehouse/path");

        MetastoreProperties properties = MetastoreProperties.create(props);
        Assertions.assertEquals(PaimonJdbcMetaStoreProperties.class, properties.getClass());
    }

    @Test
    public void testMissingWarehouse() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");

        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(props));
    }

    @Test
    public void testMissingUri() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("warehouse", "s3://warehouse/path");

        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(props));
    }

    @Test
    public void testDriverClassRequiredWhenDriverUrlIsSet() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "s3://warehouse/path");
        props.put("paimon.jdbc.driver_url", "https://example.com/mysql-connector-java.jar");

        PaimonJdbcMetaStoreProperties jdbcProps = (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        jdbcProps.initNormalizeAndCheckProps();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> jdbcProps.initializeCatalog("paimon_catalog", Collections.emptyList()));
    }

    @Test
    public void testRawDriverClassRequiredWhenDriverUrlIsSet() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:mysql://localhost:3306/paimon");
        props.put("warehouse", "s3://warehouse/path");
        props.put("jdbc.driver_url", "https://example.com/mysql-connector-java.jar");

        PaimonJdbcMetaStoreProperties jdbcProps = (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        jdbcProps.initNormalizeAndCheckProps();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> jdbcProps.initializeCatalog("paimon_catalog", Collections.emptyList()));
    }

    @Test
    public void testGetBackendPaimonOptions() throws Exception {
        String driverUrl = "file:///tmp/postgresql-42.5.0.jar";
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:postgresql://127.0.0.1:5442/postgres");
        props.put("warehouse", "s3://warehouse/path");
        props.put("paimon.jdbc.driver_url", driverUrl);
        props.put("paimon.jdbc.driver_class", "org.postgresql.Driver");

        PaimonJdbcMetaStoreProperties jdbcProps = (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        Map<String, String> backendOptions = jdbcProps.getBackendPaimonOptions();

        Assertions.assertEquals(
                JdbcResource.getFullDriverUrl(driverUrl),
                backendOptions.get("jdbc.driver_url"));
        Assertions.assertEquals("org.postgresql.Driver", backendOptions.get("jdbc.driver_class"));
        Assertions.assertEquals(2, backendOptions.size());
    }

    @Test
    public void testGetBackendPaimonOptionsRequiresDriverClass() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:postgresql://127.0.0.1:5442/postgres");
        props.put("warehouse", "s3://warehouse/path");
        props.put("paimon.jdbc.driver_url", "file:///tmp/postgresql-42.5.0.jar");

        PaimonJdbcMetaStoreProperties jdbcProps = (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                jdbcProps::getBackendPaimonOptions);
        Assertions.assertTrue(exception.getMessage().contains("driver_class"));
    }
}
