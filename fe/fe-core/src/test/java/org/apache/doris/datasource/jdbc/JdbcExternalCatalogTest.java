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

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class JdbcExternalCatalogTest {
    private JdbcExternalCatalog jdbcExternalCatalog;

    @BeforeEach
    public void setUp() throws DdlException {
        FeConstants.runningUnitTest = true;
        Map<String, String> properties = new HashMap<>();
        properties.put(JdbcResource.DRIVER_URL, "ojdbc8.jar");
        properties.put(JdbcResource.JDBC_URL, "jdbc:oracle:thin:@127.0.0.1:1521:XE");
        properties.put(JdbcResource.DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver");
        jdbcExternalCatalog = new JdbcExternalCatalog(1L, "testCatalog", "testResource", properties, "testComment");
    }

    @Test
    public void setDefaultPropsWhenCreatingTest() {
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, "1");
        Exception exception1 = Assert.assertThrows(DdlException.class, () -> jdbcExternalCatalog.setDefaultPropsWhenCreating(false));
        Assert.assertEquals("errCode = 2, detailMessage = only_specified_database must be true or false", exception1.getMessage());

        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, "true");
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.LOWER_CASE_TABLE_NAMES, "1");
        Exception exception2 = Assert.assertThrows(DdlException.class, () -> jdbcExternalCatalog.setDefaultPropsWhenCreating(false));
        Assert.assertEquals("errCode = 2, detailMessage = lower_case_table_names must be true or false", exception2.getMessage());

        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, "false");
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.LOWER_CASE_TABLE_NAMES, "false");
        jdbcExternalCatalog.getCatalogProperty().addProperty(JdbcResource.INCLUDE_DATABASE_LIST, "db1,db2");
        DdlException exceptione3 = Assert.assertThrows(DdlException.class, () -> jdbcExternalCatalog.setDefaultPropsWhenCreating(false));
        Assert.assertEquals("errCode = 2, detailMessage = include_database_list and exclude_database_list can not be set when only_specified_database is false", exceptione3.getMessage());

    }
}

