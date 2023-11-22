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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;

import org.junit.Assert;
import org.junit.Test;

public class JdbcResourceTest {

    @Test
    public void testHandleJdbcUrlForMySql() throws DdlException {
        String inputUrl = "jdbc:mysql://127.0.0.1:3306/test";
        String resultUrl = JdbcResource.handleJdbcUrl(inputUrl);

        // Check if the result URL contains the necessary delimiters for MySQL
        Assert.assertTrue(resultUrl.contains("?"));
        Assert.assertTrue(resultUrl.contains("&"));
    }

    @Test
    public void testHandleJdbcUrlForSqlServerWithoutParams() throws DdlException {
        String inputUrl = "jdbc:sqlserver://43.129.237.12:1433;databaseName=doris_test";
        String resultUrl = JdbcResource.handleJdbcUrl(inputUrl);

        // Ensure that the result URL for SQL Server doesn't have '?' or '&'
        Assert.assertFalse(resultUrl.contains("?"));
        Assert.assertFalse(resultUrl.contains("&"));

        // Ensure the result URL still contains ';'
        Assert.assertTrue(resultUrl.contains(";"));
    }

    @Test
    public void testHandleJdbcUrlForSqlServerWithParams() throws DdlException {
        String inputUrl = "jdbc:sqlserver://43.129.237.12:1433;encrypt=false;databaseName=doris_test;trustServerCertificate=false";
        String resultUrl = JdbcResource.handleJdbcUrl(inputUrl);

        // Ensure that the result URL for SQL Server doesn't have '?' or '&'
        Assert.assertFalse(resultUrl.contains("?"));
        Assert.assertFalse(resultUrl.contains("&"));

        // Ensure the result URL still contains ';'
        Assert.assertTrue(resultUrl.contains(";"));
    }
}

