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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DatasourcePropertyMaskingTest {
    private static final String MASK = DatasourcePrintableMap.PASSWORD_MASK;

    private void assertRendered(String key, String value, String expected) {
        Map<String, String> properties = Map.of(key, value);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getProperties()).thenReturn(properties);
        Assertions.assertEquals(expected, CatalogMgr.getCatalogPropertiesWithPrintable(catalog).get(key), key);
        Assertions.assertEquals("\"" + key + "\" = \"" + expected + "\"",
                new DatasourcePrintableMap<>(properties, "=", true, false, true).toString(), key);
        Assertions.assertEquals(value, properties.get(key));
    }

    @Test
    void jdbcConnectionPropertiesAreEntirelyMasked() {
        for (String key : List.of("jdbc_url", "jdbc.jdbc_url", "iceberg.jdbc.uri", "paimon.jdbc.uri")) {
            assertRendered(key, "jdbc:fixture://db.example.com/database", MASK);
        }
        assertRendered("uri", "jdbc:oracle:thin:fixture/fixture@db.example.com:1521/service", MASK);
        assertRendered("custom.connection", "JDBC:sqlserver://db.example.com;password=fixture", MASK);
        assertRendered("connection_string", "Server=db.example.com;Pwd=fixture", MASK);
        assertRendered("azure.connection-string", "AccountName=fixture;AccountKey=fixture", MASK);
    }

    @Test
    void nativeAndCustomCredentialNamesAreMasked() {
        for (String key : List.of("token", "credential", "custom.password", "custom.PASSWORD",
                "rest.clientSecret", "custom.accessKey", "custom.api-key", "custom.session_token")) {
            assertRendered(key, "fixture-credential", MASK);
        }
    }

    @Test
    void hadoopCredentialNamesIncludeDynamicAccountsAndBuckets() {
        for (String key : List.of("fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token",
                "fs.s3a.bucket.fixture.secret.key", "fs.s3a.bucket.fixture.encryption.key",
                "fs.s3a.proxy.password", "fs.azure.account.key.fixture.blob.core.windows.net",
                "fs.azure.sas.fixture.fixture.blob.core.windows.net",
                "fs.azure.encryption.encoded.client-provided.key", "hadoop.fs.adl.oauth2.credential")) {
            assertRendered(key, "fixture-credential", MASK);
        }
    }

    @Test
    void urlsRemoveUserInfoAndMaskSensitiveQueryValues() {
        assertRendered("uri", "https://fixture:fixture-password@api.example.com/v1?region=east&token=fixture-token",
                "https://api.example.com/v1?region=east&token=" + MASK);
        assertRendered("iceberg.rest.uri", "https://api.example.com/v1?Password=one;password=two&scope=read",
                "https://api.example.com/v1?Password=" + MASK + "&password=" + MASK + "&scope=read");
        assertRendered("hosts", "https://fixture:fixture-password@api.example.com:8443",
                "https://api.example.com:8443");
        assertRendered("warehouse", "s3://fixture:fixture-password@bucket.example.com/path",
                "s3://bucket.example.com/path");
        assertRendered("endpoint", "https://api.example.com/?authorization=fixture&api_key=fixture",
                "https://api.example.com/?authorization=" + MASK + "&api_key=" + MASK);
        assertRendered("endpoint", "https://api.example.com/?key=fixture&account_key=fixture",
                "https://api.example.com/?key=" + MASK + "&account_key=" + MASK);
    }

    @Test
    void encodedNamesAndFragmentsDoNotExposeCredentials() {
        assertRendered("uri", "https://api.example.com/?%74oken=fixture&safe=yes",
                "https://api.example.com/?%74oken=" + MASK + "&safe=yes");
        assertRendered("uri", "https://api.example.com/#access_token=fixture", "https://api.example.com/");
        assertRendered("uri", "https://api.example.com/?%2574oken=fixture", MASK);
        assertRendered("uri", "https://api.example.com/path;password=fixture", MASK);
    }

    @Test
    void signedUrlsAreEntirelyMasked() {
        for (String query : List.of("X-Amz-Signature=fixture", "X-Goog-Signature=fixture", "sig=fixture",
                "Signature=fixture", "q-signature=fixture", "%73ig=fixture")) {
            assertRendered("endpoint", "https://api.example.com/path?" + query, MASK);
        }
    }

    @Test
    void malformedAndUnknownAddressFormatsFailClosed() {
        for (String value : List.of("https://fixture:fixture@api.example.com:bad/path", "https://[invalid/",
                "https://api.example.com/?token=%GG", "fixture:password@host", "unknown:fixture-secret",
                "unknown://fixture-secret@host/path", "host1:9000;password=fixture", "https://api.example.com/%GG",
                "host.example.com:65536", "host.example.com:999999999999999999999999")) {
            assertRendered("hosts", value, MASK);
        }
    }

    @Test
    void urlListsCannotHideLaterCredentialsInTheFirstUrlsPath() {
        for (String value : List.of(
                "https://host1.example:9200/,https://fixture:fixture-list-password@host2.example:9200/",
                "https://host1.example:9200/path,https://host2.example:9200/?token=fixture-list-token",
                "https://host1.example:9200/,//fixture:fixture-list-password@host2.example:9200/",
                "https://host1.example:9200/,https://host2.example:9200/",
                "https://host1.example:9200/%2Chttps://fixture:fixture-list-password@host2.example:9200/",
                "https://api.example.com/?region=east,west")) {
            assertRendered("hosts", value, MASK);
        }
    }

    @Test
    void nestedConnectionStringsAreNotPassedThroughQueryValues() {
        assertRendered("uri", "https://api.example.com/?redirect=https%3A%2F%2Fu%3Ap%40host.example.com", MASK);
        assertRendered("uri", "https://api.example.com/?options=jdbc%3Afixture%3Apassword", MASK);
        assertRendered("uri", "https://api.example.com/?redirect=https%253A%252F%252Fhost.example.com", MASK);
    }

    @Test
    void configurablePatternIsDynamicAndCannotDisableKnownCredentials() throws Exception {
        String original = Config.datasource_sensitive_property_pattern;
        Map<String, java.lang.reflect.Field> originalFields = ConfigBase.confFields;
        try {
            ConfigBase.confFields = new HashMap<>();
            ConfigBase.confFields.put("datasource_sensitive_property_pattern",
                    Config.class.getField("datasource_sensitive_property_pattern"));
            DatasourcePrintableMap.registerSensitiveKeys(List.of("fixture.private_material"));
            ConfigBase.setMutableConfig("datasource_sensitive_property_pattern", "custom[.]value");
            assertRendered("CUSTOM.VALUE", "fixture", MASK);
            assertRendered("uri", "https://api.example.com/?CUSTOM.VALUE=fixture",
                    "https://api.example.com/?CUSTOM.VALUE=" + MASK);
            ConfigBase.setMutableConfig("datasource_sensitive_property_pattern", "(?!)");
            assertRendered("custom.value", "fixture", "fixture");
            for (String key : List.of("password", "token", "credential", "mc.session_token", "s3.access_key",
                    "fixture.private_material",
                    "fs.s3a.bucket.fixture.secret.key", "fs.s3a.bucket.fixture.bucket.secret.key",
                    "fs.s3a.server-side-encryption.key", "fs.azure.account.key.fixture", "fs.azure.sas.fixture",
                    "fs.azure.sas.fixed.token.fixture", "fs.azure.account.oauth2.client.secret.fixture",
                    "fs.azure.account.oauth2.refresh.token.fixture", "fs.adl.oauth2.credential",
                    "fs.oss.accessKeyId", "fs.oss.accessKeySecret", "fs.oss.securityToken",
                    "fs.cosn.userinfo.secretId", "fs.cosn.userinfo.secretKey", "fs.obs.access.key",
                    "fs.obs.secret.key", "fs.obs.security.token")) {
                assertRendered(key, "fixture", MASK);
            }
            assertRendered("uri", "https://api.example.com/?token=fixture",
                    "https://api.example.com/?token=" + MASK);
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("datasource_sensitive_property_pattern", "["));
            Assertions.assertEquals("(?!)", Config.datasource_sensitive_property_pattern);
        } finally {
            Config.datasource_sensitive_property_pattern = original;
            ConfigBase.confFields = originalFields;
        }
    }

    @Test
    void ordinaryEndpointsAndOperationalPropertiesRemainVisible() {
        for (String value : List.of("host.example.com:9000", "host1:9000,host2:9000", "[::1]:9000",
                "https://api.example.com/v1?region=east", "thrift://host.example.com:9083")) {
            assertRendered("endpoint", value, value);
        }
        assertRendered("uri", "https://api.example.com/v1", "https://api.example.com/v1");
        assertRendered("fs.s3a.endpoint", "host.example.com:9000", "host.example.com:9000");
        assertRendered("region", "east", "east");
        assertRendered("timeout", "3000", "3000");
        assertRendered("endpoint", "", "");
    }

    @Test
    void longHostListsDoNotRequireRecursiveRegexMatching() {
        String hosts = "host.example.com:9000,".repeat(5000) + "host.example.com:9000";
        Assertions.assertEquals(hosts, DatasourcePrintableMap.redactProperty("hosts", hosts));
    }

    @Test
    void auditCreateAndAlterUseTheSameValueAwarePolicy() {
        NeedAuditEncryption audit = () -> true;
        String properties = "('jdbc_url'='jdbc:fixture://db.example.com;password=fixture-jdbc',"
                + "'uri'='https://fixture:fixture-userinfo@api.example.com/?token=fixture-token&region=east',"
                + "'fs.s3a.secret.key'='fixture-hadoop','custom.password'='fixture-custom',"
                + "'hosts'='https://host1.example:9200/,https://fixture:fixture-list-password@host2.example:9200/',"
                + "'credential'='fixture-sdk')";
        for (String prefix : List.of("CREATE CATALOG masking_catalog PROPERTIES ",
                "ALTER CATALOG masking_catalog SET PROPERTIES ")) {
            String masked = audit.geneEncryptionSQL(prefix + properties);
            for (String secret : List.of("fixture-jdbc", "fixture-userinfo", "fixture-token", "fixture-hadoop",
                    "fixture-custom", "fixture-sdk", "fixture-list-password")) {
                Assertions.assertFalse(masked.contains(secret), masked);
            }
            Assertions.assertTrue(masked.contains("https://api.example.com/?token=" + MASK + "&region=east"), masked);
        }
    }
}
