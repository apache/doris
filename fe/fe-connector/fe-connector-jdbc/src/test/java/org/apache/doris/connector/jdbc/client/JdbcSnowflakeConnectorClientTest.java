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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.jdbc.JdbcDbType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class JdbcSnowflakeConnectorClientTest {

    private JdbcSnowflakeConnectorClient createClient(boolean enableVarbinary, boolean enableTimestampTz) {
        return new JdbcSnowflakeConnectorClient(
                "test_catalog",
                JdbcDbType.SNOWFLAKE,
                "jdbc:snowflake://example.snowflakecomputing.com/?db=PLAUD_TEST&schema=ADS",
                false,
                Collections.emptyMap(),
                Collections.emptyMap(),
                enableVarbinary,
                enableTimestampTz);
    }

    @Test
    void testNumberMapsToDecimalV3() {
        ConnectorType type = createClient(false, false).jdbcTypeToConnectorType(
                field(Types.NUMERIC, "NUMBER", 38, 0));
        Assertions.assertEquals("DECIMALV3", type.getTypeName());
        Assertions.assertEquals(38, type.getPrecision());
        Assertions.assertEquals(0, type.getScale());
    }

    @Test
    void testTimestampNtzScaleIsCappedAtMicrosecond() {
        ConnectorType type = createClient(false, false).jdbcTypeToConnectorType(
                field(Types.TIMESTAMP, "TIMESTAMP_NTZ", 0, 9));
        Assertions.assertEquals("DATETIMEV2", type.getTypeName());
        Assertions.assertEquals(6, type.getPrecision());
    }

    @Test
    void testTimestampTzMappingIsOptIn() {
        ConnectorType defaultType = createClient(false, false).jdbcTypeToConnectorType(
                field(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP_TZ", 0, 6));
        Assertions.assertEquals("DATETIMEV2", defaultType.getTypeName());

        ConnectorType timestampTz = createClient(false, true).jdbcTypeToConnectorType(
                field(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP_TZ", 0, 6));
        Assertions.assertEquals("TIMESTAMPTZ", timestampTz.getTypeName());
    }

    @Test
    void testComplexTypesMapToString() {
        JdbcSnowflakeConnectorClient client = createClient(false, false);
        Assertions.assertEquals("STRING", client.jdbcTypeToConnectorType(
                field(Types.OTHER, "VARIANT", 0, 0)).getTypeName());
        Assertions.assertEquals("STRING", client.jdbcTypeToConnectorType(
                field(Types.OTHER, "ARRAY", 0, 0)).getTypeName());
        Assertions.assertEquals("STRING", client.jdbcTypeToConnectorType(
                field(Types.OTHER, "OBJECT", 0, 0)).getTypeName());
    }

    @Test
    void testBinaryMappingIsOptIn() {
        ConnectorType defaultType = createClient(false, false).jdbcTypeToConnectorType(
                field(Types.BINARY, "BINARY", 16, 0));
        Assertions.assertEquals("STRING", defaultType.getTypeName());

        ConnectorType varbinary = createClient(true, false).jdbcTypeToConnectorType(
                field(Types.BINARY, "BINARY", 16, 0));
        Assertions.assertEquals("VARBINARY", varbinary.getTypeName());
        Assertions.assertEquals(16, varbinary.getPrecision());
    }

    @Test
    void testFallbackFloatTypesMapToDouble() {
        JdbcSnowflakeConnectorClient client = createClient(false, false);
        Assertions.assertEquals("DOUBLE", client.jdbcTypeToConnectorType(
                field(Types.FLOAT, "", 0, 0)).getTypeName());
        Assertions.assertEquals("DOUBLE", client.jdbcTypeToConnectorType(
                field(Types.REAL, "", 0, 0)).getTypeName());
    }

    @Test
    void testOauthAuthenticatorRequiresExactValue() {
        Assertions.assertTrue(JdbcConnectorClient.isSnowflakeOauthUrl(
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=OAUTH"));
        Assertions.assertFalse(JdbcConnectorClient.isSnowflakeOauthUrl(
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=oauth2"));
        Assertions.assertFalse(JdbcConnectorClient.isSnowflakeOauthUrl(
                "jdbc:mysql://127.0.0.1:3306/test?authenticator=oauth"));
    }

    @Test
    void testExplicitOauthAccessTokenTakesPrecedenceOverPassword() {
        Map<String, String> properties = new HashMap<>();
        properties.put("password", "snowflake-password");
        properties.put("snowflake.oauth.access_token", "snowflake-oauth-token");

        String token = JdbcConnectorClient.resolveSnowflakeOauthToken(
                properties,
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=oauth",
                properties.get("password"));

        Assertions.assertEquals("snowflake-oauth-token", token);
    }

    @Test
    void testPasswordRemainsBackwardCompatibleForOauthToken() {
        Map<String, String> properties = new HashMap<>();
        properties.put("password", "legacy-oauth-token");

        String token = JdbcConnectorClient.resolveSnowflakeOauthToken(
                properties,
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=oauth",
                properties.get("password"));

        Assertions.assertEquals("legacy-oauth-token", token);
    }

    @Test
    void testExplicitOauthAccessTokenDoesNotOverridePasswordAuth() {
        Map<String, String> properties = new HashMap<>();
        properties.put("password", "snowflake-password");
        properties.put("snowflake.oauth.access_token", "snowflake-oauth-token");

        String credential = JdbcConnectorClient.resolveSnowflakeOauthToken(
                properties,
                "jdbc:snowflake://example.snowflakecomputing.com/?warehouse=COMPUTE_WH",
                properties.get("password"));

        Assertions.assertEquals("snowflake-password", credential);
    }

    @Test
    void testExplicitOauthAccessTokenDoesNotOverrideProgrammaticAccessTokenPassword() {
        Map<String, String> properties = new HashMap<>();
        properties.put("password", "snowflake-pat");
        properties.put("snowflake.oauth.access_token", "snowflake-oauth-token");

        String credential = JdbcConnectorClient.resolveSnowflakeOauthToken(
                properties,
                "jdbc:snowflake://example.snowflakecomputing.com/?authenticator=programmatic_access_token",
                properties.get("password"));

        Assertions.assertEquals("snowflake-pat", credential);
    }

    private JdbcFieldInfo field(int jdbcType, String typeName, int precision, int scale) {
        return new JdbcFieldInfo(
                "C1",
                Optional.of(typeName),
                jdbcType,
                Optional.of(precision),
                Optional.of(scale),
                Optional.empty());
    }
}
