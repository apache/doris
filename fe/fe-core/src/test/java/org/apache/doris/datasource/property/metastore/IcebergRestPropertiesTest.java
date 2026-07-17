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

import org.apache.doris.common.Config;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.qe.ConnectContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergRestPropertiesTest {

    private String originalS3ClientHttpScheme;

    @BeforeEach
    public void setUpS3ClientHttpScheme() {
        originalS3ClientHttpScheme = Config.s3_client_http_scheme;
        Config.s3_client_http_scheme = "https";
    }

    @AfterEach
    public void restoreS3ClientHttpScheme() {
        Config.s3_client_http_scheme = originalS3ClientHttpScheme;
    }

    @Test
    public void testBasicRestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.prefix", "prefix");
        props.put("warehouse", "s3://warehouse/path");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals(CatalogUtil.ICEBERG_CATALOG_REST,
                catalogProps.get(CatalogProperties.CATALOG_IMPL));
        Assertions.assertEquals("http://localhost:8080", catalogProps.get(CatalogProperties.URI));
        Assertions.assertEquals("s3://warehouse/path", catalogProps.get(CatalogProperties.WAREHOUSE_LOCATION));
        Assertions.assertEquals("prefix", catalogProps.get("prefix"));
    }

    @Test
    public void testVendedCredentialsEnabled() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.vended-credentials-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Assertions.assertTrue(restProps.isIcebergRestVendedCredentialsEnabled());
        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("vended-credentials", catalogProps.get("header.X-Iceberg-Access-Delegation"));
    }

    @Test
    public void testVendedCredentialsDisabled() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.vended-credentials-enabled", "false");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Assertions.assertFalse(restProps.isIcebergRestVendedCredentialsEnabled());
        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertFalse(catalogProps.containsKey("header.X-Iceberg-Access-Delegation"));
    }

    @Test
    public void testRestViewEnabled() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");

        IcebergRestProperties defaultProps = new IcebergRestProperties(props);
        defaultProps.initNormalizeAndCheckProps();
        Assertions.assertTrue(defaultProps.isIcebergRestViewEnabled());

        props.put("iceberg.rest.view-enabled", "false");
        IcebergRestProperties disabledProps = new IcebergRestProperties(props);
        disabledProps.initNormalizeAndCheckProps();
        Assertions.assertFalse(disabledProps.isIcebergRestViewEnabled());
        Assertions.assertFalse(disabledProps.getIcebergRestCatalogProperties()
                .containsKey("iceberg.rest.view-enabled"));
    }

    @Test
    public void testOAuth2CredentialFlow() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.oauth2.credential", "client_credentials");
        props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");
        props.put("iceberg.rest.oauth2.scope", "read write");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("client_credentials", catalogProps.get(OAuth2Properties.CREDENTIAL));
        Assertions.assertEquals("http://auth.example.com/token", catalogProps.get(OAuth2Properties.OAUTH2_SERVER_URI));
        Assertions.assertEquals("read write", catalogProps.get(OAuth2Properties.SCOPE));
        Assertions.assertEquals(String.valueOf(OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT),
                catalogProps.get(OAuth2Properties.TOKEN_REFRESH_ENABLED));
    }

    @Test
    public void testOAuth2TokenFlow() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.oauth2.token", "my-access-token");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("my-access-token", catalogProps.get(OAuth2Properties.TOKEN));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.CREDENTIAL));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.OAUTH2_SERVER_URI));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.SCOPE));
    }

    @Test
    public void testOAuth2UserSessionFlow() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.session", "user");
        props.put("iceberg.rest.session-timeout", "60000");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertTrue(restProps.isIcebergRestUserSessionEnabled());
        Assertions.assertEquals(AuthProperties.AUTH_TYPE_OAUTH2, catalogProps.get(AuthProperties.AUTH_TYPE));
        Assertions.assertEquals("60000", catalogProps.get(CatalogProperties.AUTH_SESSION_TIMEOUT_MS));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.TOKEN));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.CREDENTIAL));
    }

    @Test
    public void testOAuth2UserSessionCatalogInitKeepsBootstrapCredential() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.session", "user");
        props.put("iceberg.rest.oauth2.credential", "client_credentials");
        props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");
        props.put("iceberg.rest.oauth2.scope", "read write");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();

        Assertions.assertEquals(AuthProperties.AUTH_TYPE_OAUTH2, catalogProps.get(AuthProperties.AUTH_TYPE));
        Assertions.assertEquals("client_credentials", catalogProps.get(OAuth2Properties.CREDENTIAL));
        Assertions.assertEquals("http://auth.example.com/token", catalogProps.get(OAuth2Properties.OAUTH2_SERVER_URI));
        Assertions.assertEquals("read write", catalogProps.get(OAuth2Properties.SCOPE));

        Map<String, String> tokenProps = new HashMap<>();
        tokenProps.put("iceberg.rest.uri", "http://localhost:8080");
        tokenProps.put("iceberg.rest.security.type", "oauth2");
        tokenProps.put("iceberg.rest.session", "user");
        tokenProps.put("iceberg.rest.oauth2.token", "static-access-token");

        IcebergRestProperties tokenRestProps = new IcebergRestProperties(tokenProps);
        tokenRestProps.initNormalizeAndCheckProps();

        Map<String, String> tokenCatalogProps = tokenRestProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("static-access-token", tokenCatalogProps.get(OAuth2Properties.TOKEN));
    }

    @Test
    public void testOAuth2UserSessionCatalogInitUsesDelegatedAccessToken() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.session", "user");
        props.put("iceberg.rest.oauth2.credential", "client_credentials");
        props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");
        props.put("iceberg.rest.oauth2.scope", "read write");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogPropertiesForCatalogInit(
                SessionContext.of(new DelegatedCredential(
                        DelegatedCredential.Type.ACCESS_TOKEN, "delegated-access-token")));

        Assertions.assertEquals("delegated-access-token", catalogProps.get(OAuth2Properties.TOKEN));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.CREDENTIAL));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.OAUTH2_SERVER_URI));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.SCOPE));
    }

    @Test
    public void testOAuth2UserSessionCatalogInitUsesDelegatedTokenExchangeCredential() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.session", "user");
        props.put("iceberg.rest.oauth2.delegated-token-mode", "token_exchange");
        props.put("iceberg.rest.oauth2.credential", "client_credentials");
        props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");
        props.put("iceberg.rest.oauth2.scope", "read write");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogPropertiesForCatalogInit(
                SessionContext.of(new DelegatedCredential(
                        DelegatedCredential.Type.ID_TOKEN, "delegated-id-token")));

        Assertions.assertEquals("delegated-id-token", catalogProps.get(OAuth2Properties.ID_TOKEN_TYPE));
        Assertions.assertEquals("client_credentials", catalogProps.get(OAuth2Properties.CREDENTIAL));
        Assertions.assertEquals("http://auth.example.com/token", catalogProps.get(OAuth2Properties.OAUTH2_SERVER_URI));
        Assertions.assertEquals("read write", catalogProps.get(OAuth2Properties.SCOPE));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.TOKEN));
    }

    @Test
    public void testInitCatalogDoesNotCaptureCurrentDelegatedCredential() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.session", "user");
        props.put("iceberg.rest.oauth2.credential", "client_credentials");
        props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");
        props.put("iceberg.rest.oauth2.scope", "read write");

        CapturingIcebergRestProperties restProps = new CapturingIcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        ConnectContext context = new ConnectContext();
        context.setSessionContext(SessionContext.of(new DelegatedCredential(
                DelegatedCredential.Type.ACCESS_TOKEN, "delegated-access-token")));
        context.setThreadLocalInfo();
        try {
            restProps.initCatalog("test_catalog", new HashMap<>(), new ArrayList<>());

            Assertions.assertFalse(restProps.capturedCatalogProps.containsKey(OAuth2Properties.TOKEN));
            Assertions.assertEquals("client_credentials",
                    restProps.capturedCatalogProps.get(OAuth2Properties.CREDENTIAL));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testOAuth2DelegatedTokenMode() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.session", "user");

        IcebergRestProperties defaultProps = new IcebergRestProperties(props);
        defaultProps.initNormalizeAndCheckProps();
        Assertions.assertEquals(IcebergRestProperties.DelegatedTokenMode.ACCESS_TOKEN,
                defaultProps.getDelegatedTokenMode());

        props.put("iceberg.rest.oauth2.delegated-token-mode", "token_exchange");
        IcebergRestProperties tokenExchangeProps = new IcebergRestProperties(props);
        tokenExchangeProps.initNormalizeAndCheckProps();
        Assertions.assertEquals(IcebergRestProperties.DelegatedTokenMode.TOKEN_EXCHANGE,
                tokenExchangeProps.getDelegatedTokenMode());

        props.put("iceberg.rest.oauth2.delegated-token-mode", "invalid");
        IcebergRestProperties invalidProps = new IcebergRestProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, invalidProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testUserSessionRequiresOAuth2SecurityType() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.session", "user");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
        Assertions.assertTrue(exception.getMessage().contains("iceberg.rest.session=user requires oauth2"));
    }

    @Test
    public void testOAuth2ValidationErrors() {
        // Test: both credential and token provided
        Map<String, String> props1 = new HashMap<>();
        props1.put("iceberg.rest.uri", "http://localhost:8080");
        props1.put("iceberg.rest.security.type", "oauth2");
        props1.put("iceberg.rest.oauth2.credential", "client_credentials");
        props1.put("iceberg.rest.oauth2.token", "my-token");

        IcebergRestProperties restProps1 = new IcebergRestProperties(props1);
        Assertions.assertThrows(IllegalArgumentException.class, restProps1::initNormalizeAndCheckProps);

        // Test: OAuth2 enabled but no credential or token
        Map<String, String> props2 = new HashMap<>();
        props2.put("iceberg.rest.uri", "http://localhost:8080");
        props2.put("iceberg.rest.security.type", "oauth2");

        IcebergRestProperties restProps2 = new IcebergRestProperties(props2);
        Assertions.assertThrows(IllegalArgumentException.class, restProps2::initNormalizeAndCheckProps);

        // Test: credential flow without server URI is ok
        Map<String, String> props3 = new HashMap<>();
        props3.put("iceberg.rest.uri", "http://localhost:8080");
        props3.put("iceberg.rest.security.type", "oauth2");
        props3.put("iceberg.rest.oauth2.credential", "client_credentials");

        IcebergRestProperties restProps3 = new IcebergRestProperties(props3);
        Assertions.assertDoesNotThrow(restProps3::initNormalizeAndCheckProps);

        // Test: scope with token (should fail)
        Map<String, String> props4 = new HashMap<>();
        props4.put("iceberg.rest.uri", "http://localhost:8080");
        props4.put("iceberg.rest.security.type", "oauth2");
        props4.put("iceberg.rest.oauth2.token", "my-token");
        props4.put("iceberg.rest.oauth2.scope", "read");

        IcebergRestProperties restProps4 = new IcebergRestProperties(props4);
        Assertions.assertThrows(IllegalArgumentException.class, restProps4::initNormalizeAndCheckProps);
    }

    @Test
    public void testInvalidSecurityType() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "invalid");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testSecurityTypeNone() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "none");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        // Should only have basic properties, no OAuth2 properties
        Assertions.assertEquals(CatalogUtil.ICEBERG_CATALOG_REST,
                catalogProps.get(CatalogProperties.CATALOG_IMPL));
        Assertions.assertEquals("http://localhost:8080", catalogProps.get(CatalogProperties.URI));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.CREDENTIAL));
        Assertions.assertFalse(catalogProps.containsKey(OAuth2Properties.TOKEN));
    }

    @Test
    public void testUriAliases() {
        // Test different URI property names
        Map<String, String> props1 = new HashMap<>();
        props1.put("uri", "http://localhost:8080");
        IcebergRestProperties restProps1 = new IcebergRestProperties(props1);
        restProps1.initNormalizeAndCheckProps();
        Assertions.assertEquals("http://localhost:8080",
                restProps1.getIcebergRestCatalogProperties().get(CatalogProperties.URI));

        Map<String, String> props2 = new HashMap<>();
        props2.put("iceberg.rest.uri", "http://localhost:8080");
        IcebergRestProperties restProps2 = new IcebergRestProperties(props2);
        restProps2.initNormalizeAndCheckProps();
        Assertions.assertEquals("http://localhost:8080",
                restProps2.getIcebergRestCatalogProperties().get(CatalogProperties.URI));
    }

    @Test
    public void testWarehouseAliases() {
        // Test different warehouse property names
        Map<String, String> props1 = new HashMap<>();
        props1.put("iceberg.rest.uri", "http://localhost:8080");
        props1.put("warehouse", "s3://warehouse/path");
        IcebergRestProperties restProps1 = new IcebergRestProperties(props1);
        restProps1.initNormalizeAndCheckProps();
        Assertions.assertEquals("s3://warehouse/path",
                restProps1.getIcebergRestCatalogProperties().get(CatalogProperties.WAREHOUSE_LOCATION));

        Map<String, String> props2 = new HashMap<>();
        props2.put("iceberg.rest.uri", "http://localhost:8080");
        props2.put("warehouse", "s3://warehouse/path");
        IcebergRestProperties restProps2 = new IcebergRestProperties(props2);
        restProps2.initNormalizeAndCheckProps();
        Assertions.assertEquals("s3://warehouse/path",
                restProps2.getIcebergRestCatalogProperties().get(CatalogProperties.WAREHOUSE_LOCATION));
    }

    @Test
    public void testImmutablePropertiesMap() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();

        // Should throw UnsupportedOperationException when trying to modify
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            catalogProps.put("test", "value");
        });
    }

    @Test
    public void testGlueRestCatalogValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("glue", catalogProps.get("rest.signing-name"));
        Assertions.assertEquals("us-east-1", catalogProps.get("rest.signing-region"));
        Assertions.assertEquals("AKIAIOSFODNN7EXAMPLE", catalogProps.get("rest.access-key-id"));
        Assertions.assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                catalogProps.get("rest.secret-access-key"));
        Assertions.assertEquals("true", catalogProps.get("rest.sigv4-enabled"));
    }

    @Test
    public void testGlueRestCatalogCaseInsensitive() {
        // Test that "GLUE" is also recognized (case insensitive)
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "GLUE");
        props.put("iceberg.rest.signing-region", "us-west-2");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("GLUE", catalogProps.get("rest.signing-name"));
        Assertions.assertEquals("us-west-2", catalogProps.get("rest.signing-region"));
    }

    @Test
    public void testGlueRestCatalogMissingSigningRegion() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");
        // Missing signing-region

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testGlueRestCatalogMissingAccessKeyId() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");
        // Missing access-key-id

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testGlueRestCatalogMissingSecretAccessKey() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.sigv4-enabled", "true");
        // Missing secret-access-key

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testGlueRestCatalogMissingSigV4Enabled() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        // Missing sigv4-enabled

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertThrows(IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);
    }

    @Test
    public void testNonGlueSigningNameDoesNotRequireAdditionalProperties() {
        // Test that non-glue signing names don't require additional properties
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing", "custom-service");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps(); // Should not throw

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        // Should not contain glue-specific properties
        Assertions.assertFalse(catalogProps.containsKey("rest.signing-name"));
        Assertions.assertFalse(catalogProps.containsKey("rest.signing-region"));
        Assertions.assertFalse(catalogProps.containsKey("rest.access-key-id"));
        Assertions.assertFalse(catalogProps.containsKey("rest.secret-access-key"));
        Assertions.assertFalse(catalogProps.containsKey("rest.sigv4-enabled"));
        props.put("iceberg.rest.signing-name", "custom-service");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps(); // Should not throw
        catalogProps = restProps.getIcebergRestCatalogProperties();
        // Should not contain glue-specific properties
        Assertions.assertTrue(catalogProps.containsKey("rest.signing-name"));
        Assertions.assertTrue(catalogProps.containsKey("rest.signing-region"));
        Assertions.assertTrue(catalogProps.containsKey("rest.access-key-id"));
        Assertions.assertTrue(catalogProps.containsKey("rest.secret-access-key"));
        Assertions.assertTrue(catalogProps.containsKey("rest.sigv4-enabled"));
    }

    @Test
    public void testEmptySigningNameDoesNotAddGlueProperties() {
        // Test that empty signing name doesn't add glue properties
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps(); // Should not throw

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        // Should not contain glue-specific properties since signing-name is not "glue"
        Assertions.assertFalse(catalogProps.containsKey("rest.signing-name"));
        Assertions.assertFalse(catalogProps.containsKey("rest.signing-region"));
        Assertions.assertFalse(catalogProps.containsKey("rest.access-key-id"));
        Assertions.assertFalse(catalogProps.containsKey("rest.secret-access-key"));
        Assertions.assertFalse(catalogProps.containsKey("rest.sigv4-enabled"));
    }

    @Test
    public void testGlueRestCatalogWithOAuth2() {
        // Test that Glue properties can be combined with OAuth2
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.oauth2.token", "my-access-token");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        // Should have both OAuth2 and Glue properties
        Assertions.assertEquals("my-access-token", catalogProps.get(OAuth2Properties.TOKEN));
        Assertions.assertEquals("glue", catalogProps.get("rest.signing-name"));
        Assertions.assertEquals("us-east-1", catalogProps.get("rest.signing-region"));
        Assertions.assertEquals("AKIAIOSFODNN7EXAMPLE", catalogProps.get("rest.access-key-id"));
        Assertions.assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                catalogProps.get("rest.secret-access-key"));
        Assertions.assertEquals("true", catalogProps.get("rest.sigv4-enabled"));
    }

    @Test
    public void testGlueRestCatalogMissingMultipleProperties() {
        // Test error message when multiple required properties are missing
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        // Missing all required properties

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, restProps::initNormalizeAndCheckProps);

        // The error message should mention the required properties
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("signing-region")
                || errorMessage.contains("access-key-id")
                || errorMessage.contains("secret-access-key")
                || errorMessage.contains("sigv4-enabled"));
    }

    @Test
    public void testS3TablesSigningNameValidWithAccessKeyAndSecretKey() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "s3tables");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("s3tables", catalogProps.get("rest.signing-name"));
        Assertions.assertEquals("us-east-1", catalogProps.get("rest.signing-region"));
        Assertions.assertEquals("AKIAIOSFODNN7EXAMPLE", catalogProps.get("rest.access-key-id"));
        Assertions.assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                catalogProps.get("rest.secret-access-key"));
    }

    @Test
    public void testS3TablesSigningNameCaseInsensitive() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "S3TABLES");
        props.put("iceberg.rest.signing-region", "us-west-2");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);
        Assertions.assertEquals("S3TABLES", restProps.getIcebergRestCatalogProperties().get("rest.signing-name"));
    }

    @Test
    public void testGlueSigningNameWithIamRoleFails() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.role_arn", "arn:aws:iam::123456789012:role/MyGlueRole");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                restProps::initNormalizeAndCheckProps);
        Assertions.assertTrue(e.getMessage().contains("iceberg.rest.role_arn"));
    }

    @Test
    public void testS3TablesSigningNameWithIamRoleFails() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "s3tables");
        props.put("iceberg.rest.signing-region", "us-west-2");
        props.put("iceberg.rest.role_arn", "arn:aws:iam::999999999999:role/S3TablesRole");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                restProps::initNormalizeAndCheckProps);
        Assertions.assertTrue(e.getMessage().contains("iceberg.rest.role_arn"));
    }

    @Test
    public void testGlueSigningNameWithDefaultCredentialsProvider() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertFalse(catalogProps.containsKey("client.credentials-provider"));
    }

    @Test
    public void testS3TablesSigningNameWithDefaultCredentialsProvider() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "s3tables");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.sigv4-enabled", "true");
        // No credentials, should use DEFAULT provider

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertFalse(catalogProps.containsKey("client.credentials-provider"));
    }

    @Test
    public void testS3TablesSigningNameMissingSigningRegionFails() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "s3tables");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                restProps::initNormalizeAndCheckProps);
        Assertions.assertTrue(e.getMessage().contains("signing-region") && e.getMessage().contains("s3tables"));
    }

    @Test
    public void testAccessKeyAndSecretKeyMustBeSetTogether() {
        Map<String, String> props1 = new HashMap<>();
        props1.put("iceberg.rest.uri", "http://localhost:8080");
        props1.put("iceberg.rest.signing-name", "glue");
        props1.put("iceberg.rest.signing-region", "us-east-1");
        props1.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props1.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps1 = new IcebergRestProperties(props1);
        IllegalArgumentException e1 = Assertions.assertThrows(IllegalArgumentException.class,
                restProps1::initNormalizeAndCheckProps);
        Assertions.assertTrue(e1.getMessage().contains("access-key-id")
                && e1.getMessage().contains("secret-access-key"));

        Map<String, String> props2 = new HashMap<>();
        props2.put("iceberg.rest.uri", "http://localhost:8080");
        props2.put("iceberg.rest.signing-name", "glue");
        props2.put("iceberg.rest.signing-region", "us-east-1");
        props2.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props2.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps2 = new IcebergRestProperties(props2);
        Assertions.assertThrows(IllegalArgumentException.class, restProps2::initNormalizeAndCheckProps);
    }

    @Test
    public void testGlueWithIamRoleAndExternalIdFails() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.role_arn", "arn:aws:iam::123456789012:role/MyGlueRole");
        props.put("iceberg.rest.external-id", "external-123");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                restProps::initNormalizeAndCheckProps);
        Assertions.assertTrue(e.getMessage().contains("iceberg.rest.role_arn"));
    }

    @Test
    public void testGlueWithExternalIdFails() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.external-id", "external-123");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                restProps::initNormalizeAndCheckProps);
        Assertions.assertTrue(e.getMessage().contains("iceberg.rest.external-id"));
    }

    @Test
    public void testGlueWithCredentialsProviderTypeDefault() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.sigv4-enabled", "true");
        props.put("iceberg.rest.credentials_provider_type", "DEFAULT");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("glue", catalogProps.get("rest.signing-name"));
        Assertions.assertEquals("us-east-1", catalogProps.get("rest.signing-region"));
        Assertions.assertFalse(catalogProps.containsKey("client.credentials-provider"));
    }

    @Test
    public void testS3TablesWithCredentialsProviderTypeInstanceProfile() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "s3tables");
        props.put("iceberg.rest.signing-region", "us-west-2");
        props.put("iceberg.rest.sigv4-enabled", "true");
        props.put("iceberg.rest.credentials_provider_type", "INSTANCE_PROFILE");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("s3tables", catalogProps.get("rest.signing-name"));
        Assertions.assertEquals(
                "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider",
                catalogProps.get("client.credentials-provider"));
    }

    @Test
    public void testS3TablesWithCredentialsProviderTypeEnvWithoutAccessKey() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "s3tables");
        props.put("iceberg.rest.signing-region", "us-west-2");
        props.put("iceberg.rest.sigv4-enabled", "true");
        props.put("iceberg.rest.credentials_provider_type", "ENV");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("s3tables", catalogProps.get("rest.signing-name"));
        Assertions.assertEquals("us-west-2", catalogProps.get("rest.signing-region"));
        Assertions.assertEquals(
                "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
                catalogProps.get("client.credentials-provider"));
        Assertions.assertFalse(catalogProps.containsKey("rest.access-key-id"));
        Assertions.assertFalse(catalogProps.containsKey("rest.secret-access-key"));
    }

    @Test
    public void testGlueWithCredentialsProviderTypeEnv() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "ap-east-1");
        props.put("iceberg.rest.sigv4-enabled", "true");
        props.put("iceberg.rest.credentials_provider_type", "ENV");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals(
                "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
                catalogProps.get("client.credentials-provider"));
    }

    @Test
    public void testAccessKeyPriorityOverCredentialsProviderType() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "glue");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.sigv4-enabled", "true");
        props.put("iceberg.rest.access-key-id", "AKIAIOSFODNN7EXAMPLE");
        props.put("iceberg.rest.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        props.put("iceberg.rest.credentials_provider_type", "DEFAULT");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals("AKIAIOSFODNN7EXAMPLE",
                catalogProps.get("rest.access-key-id"));
        Assertions.assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                catalogProps.get("rest.secret-access-key"));
        Assertions.assertFalse(catalogProps.containsKey("client.credentials-provider"));
        Assertions.assertFalse(catalogProps.containsKey("client.credentials-provider.s3.access-key-id"));
        Assertions.assertFalse(catalogProps.containsKey("client.credentials-provider.s3.secret-access-key"));
    }

    @Test
    public void testIamRoleWithCredentialsProviderTypeFails() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "s3tables");
        props.put("iceberg.rest.signing-region", "us-west-2");
        props.put("iceberg.rest.sigv4-enabled", "true");
        props.put("iceberg.rest.role_arn", "arn:aws:iam::123456789012:role/MyRole");
        props.put("iceberg.rest.credentials_provider_type", "INSTANCE_PROFILE");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                restProps::initNormalizeAndCheckProps);
        Assertions.assertTrue(e.getMessage().contains("iceberg.rest.role_arn"));
    }

    @Test
    public void testNonGlueSigningNameWithoutCredentialsAllowed() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.signing-name", "custom-service");
        props.put("iceberg.rest.signing-region", "us-east-1");
        props.put("iceberg.rest.sigv4-enabled", "true");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        Assertions.assertDoesNotThrow(restProps::initNormalizeAndCheckProps);

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertFalse(catalogProps.containsKey("client.credentials-provider"));
    }

    @Test
    public void testToFileIOPropertiesPrefersNonS3Properties() {
        // When both S3Properties and OSSProperties exist, OSSProperties should be chosen
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        s3Props.put("s3.access_key", "s3AccessKey");
        s3Props.put("s3.secret_key", "s3SecretKey");
        s3Props.put("s3.region", "us-east-1");
        s3Props.put(StorageProperties.FS_S3_SUPPORT, "true");
        S3Properties s3 = (S3Properties) StorageProperties.createPrimary(s3Props);

        Map<String, String> ossProps = new HashMap<>();
        ossProps.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        ossProps.put("oss.access_key", "ossAccessKey");
        ossProps.put("oss.secret_key", "ossSecretKey");
        ossProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        OSSProperties oss = (OSSProperties) StorageProperties.createPrimary(ossProps);

        Map<String, String> restPropsMap = new HashMap<>();
        restPropsMap.put("iceberg.rest.uri", "http://localhost:8080");
        IcebergRestProperties restProps = new IcebergRestProperties(restPropsMap);
        restProps.initNormalizeAndCheckProps();

        List<StorageProperties> storageList = new ArrayList<>();
        storageList.add(s3);
        storageList.add(oss);

        Map<String, String> fileIOProperties = new HashMap<>();
        Configuration conf = new Configuration();
        restProps.toFileIOProperties(storageList, fileIOProperties, conf);

        // OSSProperties should be used, not S3Properties
        Assertions.assertEquals("https://oss-cn-beijing.aliyuncs.com",
                fileIOProperties.get(S3FileIOProperties.ENDPOINT));
        Assertions.assertEquals("ossAccessKey", fileIOProperties.get(S3FileIOProperties.ACCESS_KEY_ID));
        Assertions.assertEquals("ossSecretKey", fileIOProperties.get(S3FileIOProperties.SECRET_ACCESS_KEY));
    }

    @Test
    public void testBareS3CompatibleEndpointBuildsRealS3FileIOClient() {
        Map<String, String> ossProps = new HashMap<>();
        ossProps.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        ossProps.put("oss.region", "cn-beijing");
        ossProps.put("oss.access_key", "ossAccessKey");
        ossProps.put("oss.secret_key", "ossSecretKey");
        ossProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        OSSProperties oss = (OSSProperties) StorageProperties.createPrimary(ossProps);

        Map<String, String> restPropsMap = new HashMap<>();
        restPropsMap.put("iceberg.rest.uri", "http://localhost:8080");
        IcebergRestProperties restProps = new IcebergRestProperties(restPropsMap);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> fileIOProperties = new HashMap<>();
        restProps.toFileIOProperties(List.of(oss), fileIOProperties, new Configuration());

        try (S3FileIO fileIO = new S3FileIO()) {
            fileIO.initialize(fileIOProperties);
            Assertions.assertEquals(URI.create("https://oss-cn-beijing.aliyuncs.com"),
                    fileIO.client().serviceClientConfiguration().endpointOverride().orElseThrow());
        }
    }

    @Test
    public void testToFileIOPropertiesFallsBackToS3Properties() {
        // When only S3Properties exists, it should be used
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        s3Props.put("s3.access_key", "s3AccessKey");
        s3Props.put("s3.secret_key", "s3SecretKey");
        s3Props.put("s3.region", "us-east-1");
        s3Props.put(StorageProperties.FS_S3_SUPPORT, "true");
        S3Properties s3 = (S3Properties) StorageProperties.createPrimary(s3Props);

        Map<String, String> restPropsMap = new HashMap<>();
        restPropsMap.put("iceberg.rest.uri", "http://localhost:8080");
        IcebergRestProperties restProps = new IcebergRestProperties(restPropsMap);
        restProps.initNormalizeAndCheckProps();

        List<StorageProperties> storageList = new ArrayList<>();
        storageList.add(s3);

        Map<String, String> fileIOProperties = new HashMap<>();
        Configuration conf = new Configuration();
        restProps.toFileIOProperties(storageList, fileIOProperties, conf);

        Assertions.assertEquals("https://s3.us-east-1.amazonaws.com", fileIOProperties.get(S3FileIOProperties.ENDPOINT));
        Assertions.assertEquals("s3AccessKey", fileIOProperties.get(S3FileIOProperties.ACCESS_KEY_ID));
        Assertions.assertEquals("us-east-1", fileIOProperties.get(AwsClientProperties.CLIENT_REGION));
    }

    @Test
    public void testToFileIOPropertiesOnlyFirstNonS3Used() {
        // When S3Properties comes first, then two non-S3 types, only the first non-S3 is used
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("s3.endpoint", "https://s3.amazonaws.com");
        s3Props.put("s3.access_key", "s3AK");
        s3Props.put("s3.secret_key", "s3SK");
        s3Props.put("s3.region", "us-east-1");
        s3Props.put(StorageProperties.FS_S3_SUPPORT, "true");
        S3Properties s3 = (S3Properties) StorageProperties.createPrimary(s3Props);

        Map<String, String> ossProps1 = new HashMap<>();
        ossProps1.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        ossProps1.put("oss.access_key", "ossAK1");
        ossProps1.put("oss.secret_key", "ossSK1");
        ossProps1.put(StorageProperties.FS_OSS_SUPPORT, "true");
        OSSProperties oss1 = (OSSProperties) StorageProperties.createPrimary(ossProps1);

        Map<String, String> ossProps2 = new HashMap<>();
        ossProps2.put("oss.endpoint", "oss-cn-shanghai.aliyuncs.com");
        ossProps2.put("oss.access_key", "ossAK2");
        ossProps2.put("oss.secret_key", "ossSK2");
        ossProps2.put(StorageProperties.FS_OSS_SUPPORT, "true");
        OSSProperties oss2 = (OSSProperties) StorageProperties.createPrimary(ossProps2);

        Map<String, String> restPropsMap = new HashMap<>();
        restPropsMap.put("iceberg.rest.uri", "http://localhost:8080");
        IcebergRestProperties restProps = new IcebergRestProperties(restPropsMap);
        restProps.initNormalizeAndCheckProps();

        List<StorageProperties> storageList = new ArrayList<>();
        storageList.add(s3);
        storageList.add(oss1);
        storageList.add(oss2);

        Map<String, String> fileIOProperties = new HashMap<>();
        Configuration conf = new Configuration();
        restProps.toFileIOProperties(storageList, fileIOProperties, conf);

        // First non-S3Properties (oss1) should be used
        Assertions.assertEquals("https://oss-cn-beijing.aliyuncs.com",
                fileIOProperties.get(S3FileIOProperties.ENDPOINT));
        Assertions.assertEquals("ossAK1", fileIOProperties.get(S3FileIOProperties.ACCESS_KEY_ID));
    }

    private static class CapturingIcebergRestProperties extends IcebergRestProperties {
        private Map<String, String> capturedCatalogProps;

        private CapturingIcebergRestProperties(Map<String, String> props) {
            super(props);
        }

        @Override
        protected RESTSessionCatalog buildRestSessionCatalog(String catalogName, Map<String, String> options,
                Configuration conf) {
            capturedCatalogProps = new HashMap<>(options);
            // Return an uninitialized RESTSessionCatalog: asCatalog(empty) on it is a cheap, lazy wrapper
            // (no REST/OAuth network call), which is all initCatalog does with the result here.
            return new RESTSessionCatalog();
        }
    }
}
