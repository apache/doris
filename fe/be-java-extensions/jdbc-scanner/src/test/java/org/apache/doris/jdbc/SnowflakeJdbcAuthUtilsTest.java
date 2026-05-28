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

package org.apache.doris.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;

public class SnowflakeJdbcAuthUtilsTest {

    @Test
    public void testConfigureAddsTokenForSnowflakeOauthUrl() {
        HikariDataSource dataSource = new HikariDataSource();

        SnowflakeJdbcAuthUtils.configure(dataSource,
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=OAUTH",
                "snowflake-oauth-token");

        Assert.assertEquals("snowflake-oauth-token",
                dataSource.getDataSourceProperties().getProperty("token"));
    }

    @Test
    public void testConfigurePrefersExplicitOauthAccessToken() {
        HikariDataSource dataSource = new HikariDataSource();

        SnowflakeJdbcAuthUtils.configure(dataSource,
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=OAUTH",
                "snowflake-password",
                "snowflake-oauth-token");

        Assert.assertEquals("snowflake-oauth-token",
                dataSource.getDataSourceProperties().getProperty("token"));
    }

    @Test
    public void testResolveCredentialDoesNotOverridePasswordAuth() {
        String credential = SnowflakeJdbcAuthUtils.resolveCredential(
                "jdbc:snowflake://example.snowflakecomputing.com/?warehouse=COMPUTE_WH",
                "snowflake-password",
                "snowflake-oauth-token");

        Assert.assertEquals("snowflake-password", credential);
    }

    @Test
    public void testResolveCredentialDoesNotOverrideProgrammaticAccessTokenPassword() {
        String credential = SnowflakeJdbcAuthUtils.resolveCredential(
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=programmatic_access_token",
                "snowflake-pat",
                "snowflake-oauth-token");

        Assert.assertEquals("snowflake-pat", credential);
    }

    @Test
    public void testConfigureSkipsPasswordUrl() {
        HikariDataSource dataSource = new HikariDataSource();

        SnowflakeJdbcAuthUtils.configure(dataSource,
                "jdbc:snowflake://example.snowflakecomputing.com/?warehouse=COMPUTE_WH",
                "password");

        Assert.assertNull(dataSource.getDataSourceProperties().getProperty("token"));
    }

    @Test
    public void testConfigureSkipsOauthPrefixValue() {
        HikariDataSource dataSource = new HikariDataSource();

        SnowflakeJdbcAuthUtils.configure(dataSource,
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=oauth2",
                "password");

        Assert.assertNull(dataSource.getDataSourceProperties().getProperty("token"));
    }

    @Test
    public void testConfigureSkipsOtherJdbcUrl() {
        HikariDataSource dataSource = new HikariDataSource();

        SnowflakeJdbcAuthUtils.configure(dataSource,
                "jdbc:mysql://127.0.0.1:3306/test?authenticator=oauth",
                "password");

        Assert.assertNull(dataSource.getDataSourceProperties().getProperty("token"));
    }
}
