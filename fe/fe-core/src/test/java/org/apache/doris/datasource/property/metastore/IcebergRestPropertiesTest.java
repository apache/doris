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
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergRestPropertiesTest {

    @Test
    public void testBasicRestProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8080");
        props.put("iceberg.rest.warehouse", "s3://warehouse/path");
        props.put("iceberg.rest.prefix", "prefix");

        IcebergRestProperties restProps = new IcebergRestProperties(props);
        restProps.initNormalizeAndCheckProps();

        Map<String, String> catalogProps = restProps.getIcebergRestCatalogProperties();
        Assertions.assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
                catalogProps.get(CatalogUtil.ICEBERG_CATALOG_TYPE));
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
        Assertions.assertEquals("false", catalogProps.get(OAuth2Properties.TOKEN_REFRESH_ENABLED));
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

        // Test: credential flow without server URI
        Map<String, String> props3 = new HashMap<>();
        props3.put("iceberg.rest.uri", "http://localhost:8080");
        props3.put("iceberg.rest.security.type", "oauth2");
        props3.put("iceberg.rest.oauth2.credential", "client_credentials");

        IcebergRestProperties restProps3 = new IcebergRestProperties(props3);
        Assertions.assertThrows(IllegalArgumentException.class, restProps3::initNormalizeAndCheckProps);

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
        Assertions.assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
                catalogProps.get(CatalogUtil.ICEBERG_CATALOG_TYPE));
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
        props2.put("iceberg.rest.warehouse", "s3://warehouse/path");
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
}
