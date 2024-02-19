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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogFactory;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JdbcExternalCatalogTest {
    private JdbcExternalCatalog jdbcExternalCatalog;

    @Before
    public void setUp() throws DdlException {
        FeConstants.runningUnitTest = true;
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "jdbc");
        properties.put(JdbcResource.DRIVER_URL, "ojdbc8.jar");
        properties.put(JdbcResource.JDBC_URL, "jdbc:oracle:thin:@127.0.0.1:1521:XE");
        properties.put(JdbcResource.DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver");
        jdbcExternalCatalog = new JdbcExternalCatalog(1L, "testCatalog", null, properties, "testComment", false);
    }

    @Test
    public void testProcessCompatibleProperties() throws DdlException {
        // Create a properties map with lower_case_table_names
        Map<String, String> inputProps = new HashMap<>();
        inputProps.put("lower_case_table_names", "true");

        // Call processCompatibleProperties
        Map<String, String> resultProps = jdbcExternalCatalog.processCompatibleProperties(inputProps, true);

        // Assert that lower_case_meta_names is present and has the correct value
        Assert.assertTrue(resultProps.containsKey("lower_case_meta_names"));
        Assert.assertEquals("true", resultProps.get("lower_case_meta_names"));

        // Assert that lower_case_table_names is not present
        Assert.assertFalse(resultProps.containsKey("lower_case_table_names"));
    }

    @Test
    public void replayJdbcCatalogTest() throws DdlException {
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.CONNECTION_POOL_MIN_SIZE, "1");
        JdbcExternalCatalog replayJdbcCatalog = (JdbcExternalCatalog) CatalogFactory.createFromLog(
                jdbcExternalCatalog.constructEditLog());
        Map<String, String> properties = replayJdbcCatalog.getProperties();
        Assert.assertEquals("1", properties.get("connection_pool_min_size"));
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(JdbcResource.CONNECTION_POOL_MIN_SIZE, "2");
        jdbcExternalCatalog.getCatalogProperty().modifyCatalogProps(newProperties);
        jdbcExternalCatalog.notifyPropertiesUpdated(newProperties);
        JdbcExternalCatalog replayJdbcCatalog2 = (JdbcExternalCatalog) CatalogFactory.createFromLog(
                jdbcExternalCatalog.constructEditLog());
        Map<String, String> properties2 = replayJdbcCatalog2.getProperties();
        Assert.assertEquals("2", properties2.get("connection_pool_min_size"));
    }

    @Test
    public void checkPropertiesTest() {
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, "1");
        Exception exception1 = Assert.assertThrows(DdlException.class, () -> jdbcExternalCatalog.checkProperties());
        Assert.assertEquals("errCode = 2, detailMessage = only_specified_database must be true or false",
                exception1.getMessage());

        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, "true");
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.LOWER_CASE_META_NAMES, "1");
        Exception exception2 = Assert.assertThrows(DdlException.class, () -> jdbcExternalCatalog.checkProperties());
        Assert.assertEquals("errCode = 2, detailMessage = lower_case_meta_names must be true or false",
                exception2.getMessage());

        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, "false");
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.LOWER_CASE_META_NAMES, "false");
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.INCLUDE_DATABASE_LIST, "db1,db2");
        DdlException exceptione3 = Assert.assertThrows(DdlException.class, () -> jdbcExternalCatalog.checkProperties());
        Assert.assertEquals(
                "errCode = 2, detailMessage = include_database_list and exclude_database_list cannot be set when only_specified_database is false",
                exceptione3.getMessage());

    }
}
