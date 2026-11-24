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
        // Only selected native keys treat hashes literally; ordinary URL fragments remain omitted.
        assertRendered("uri", "s3://bucket/logs/file#1.csv", "s3://bucket/logs/file");
        assertRendered("uri", "s3://bucket/logs/100%.csv", MASK);
        assertRendered("uri", "s3://bucket/logs/literal%252F.csv", MASK);
        assertRendered("endpoint", "https://api.example.com/data?x=1;;y=2&&",
                "https://api.example.com/data?x=1&y=2");
        assertRendered("endpoint", "https://api.example.com/data?target=%20jdbc:oracle:thin:"
                + "fixture/fixture-query-password@db.example.com:1521/service", MASK);
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
    void azureStorageUrisPreserveTheirContainer() {
        for (String scheme : List.of("abfs", "abfss", "wasb", "wasbs", "ABFSS")) {
            for (String container : List.of("warehouse", "data-archive", "c", "a".repeat(63))) {
                String uri = scheme + "://" + container + "@account.dfs.core.windows.net/tables";
                assertRendered("warehouse", uri, uri);
            }
        }
    }

    @Test
    void azureStorageUrisStillMaskQueryCredentialsAndSignatures() {
        for (String scheme : List.of("abfs", "abfss", "wasb", "wasbs")) {
            String uri = scheme + "://warehouse@account.dfs.core.windows.net/tables";
            assertRendered("warehouse", uri + "?token=fixture-token&region=east",
                    uri + "?token=" + MASK + "&region=east");
            for (String query : List.of("sig=fixture&se=2030", "%73ig=fixture", "sas=fixture")) {
                assertRendered("warehouse", uri + "?" + query, MASK);
            }
        }
    }

    @Test
    void malformedAzureAuthoritiesFailClosed() {
        for (String scheme : List.of("abfs", "abfss", "wasb", "wasbs")) {
            for (String authority : List.of("account.dfs.core.windows.net", "@account.dfs.core.windows.net",
                    "warehouse:fixture-password@account.dfs.core.windows.net",
                    "BadName@account.dfs.core.windows.net", "bad-@account.dfs.core.windows.net",
                    "ab@account.dfs.core.windows.net", "bad_name@account.dfs.core.windows.net",
                    "a".repeat(64) + "@account.dfs.core.windows.net",
                    "%77arehouse@account.dfs.core.windows.net", "warehouse%3Afixture@account.dfs.core.windows.net",
                    "warehouse@fixture@account.dfs.core.windows.net")) {
                assertRendered("warehouse", scheme + "://" + authority + "/tables", MASK);
            }
        }
    }

    @Test
    void azureAuditPreservesTheContainerAndMasksCredentials() {
        NeedAuditEncryption audit = () -> true;
        for (String scheme : List.of("abfs", "abfss", "wasb", "wasbs")) {
            String uri = scheme + "://warehouse@account.dfs.core.windows.net/tables";
            for (String prefix : List.of("CREATE CATALOG masking_catalog PROPERTIES ",
                    "ALTER CATALOG masking_catalog SET PROPERTIES ")) {
                String masked = audit.geneEncryptionSQL(prefix
                        + "('warehouse'='" + uri + "?token=fixture-azure-token&region=east')");
                Assertions.assertTrue(masked.contains(uri + "?token=" + MASK + "&region=east"), masked);
                Assertions.assertFalse(masked.contains("fixture-azure-token"), masked);
            }
        }
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
                "s3://invalid_bucket/object",
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
    void localDriverPathsAndFileUrisRemainVisible() {
        for (String path : List.of("/opt/example/libdriver.so", "/opt/example/native drivers/libdriver.so",
                "/opt/example/本地/libdriver.so", "file:/opt/example/libdriver.so",
                "file:///opt/example/libdriver.so")) {
            assertRendered("driver_url", path, path);
        }
        for (String uri : List.of("file:/data/example/catalog.db", "file:///data/example/catalog.db",
                "file:/data/example/catalog.db?mode=ro&cache=private")) {
            assertRendered("uri", uri, uri);
        }
        assertRendered("uri", "file:/data/example/catalog.db?token=fixture-token&mode=ro",
                "file:/data/example/catalog.db?token=" + MASK + "&mode=ro");
        assertRendered("uri", "file:/data/example/catalog.db#token=fixture-token",
                "file:/data/example/catalog.db");
        assertRendered("warehouse", "file:/data/example/catalog.db?token=fixture-token",
                "file:/data/example/catalog.db?token=" + MASK);
    }

    @Test
    void localAddressExceptionsCannotExposeCredentials() {
        for (String value : List.of("file://fixture:fixture-password@host/catalog.db",
                "file://host/catalog.db", "file:relative.db", "file:/data/example/%GG",
                "file:/data/example/catalog.db?sig=fixture-signature", "/opt/example/libdriver.so?token=fixture",
                "/opt/example/fixture:password@host/libdriver.so", "/opt/example/https://fixture:password@host")) {
            assertRendered("driver_url", value, MASK);
        }
        assertRendered("custom.password", "file:/data/example/catalog.db", MASK);
        assertRendered("jdbc_url", "file:/data/example/catalog.db", MASK);
        assertRendered("uri", "postgresql://fixture:fixture-password@host/database", MASK);
    }

    @Test
    void viewfsLocationsUseTheHdfsAddressPolicy() {
        assertRendered("fs.defaultFS", "viewfs://my-cluster", "viewfs://my-cluster");
        assertRendered("warehouse", "viewfs://my-cluster/ns1/table", "viewfs://my-cluster/ns1/table");
        assertRendered("fs.defaultFS", "viewfs://fixture:fixture-password@my-cluster/ns1?token=fixture-token",
                "viewfs://my-cluster/ns1?token=" + MASK);
        assertRendered("fs.defaultFS", "viewfs://my-cluster/ns1?sig=fixture", MASK);
        assertRendered("fs.defaultFS", "viewfs://fixture:fixture-password@mount_table/ns1?token=fixture-token",
                "viewfs://mount_table/ns1?token=" + MASK);
    }

    @Test
    void nativeFilesystemLocationsRemainVisible() {
        NeedAuditEncryption audit = () -> true;
        for (String uri : List.of("gs://example_bucket/warehouse", "gs://example_bucket.example.com/warehouse",
                "hdfs:///data.csv", "hdfs:/data.csv", "viewfs:///warehouse", "viewfs://mount_table/warehouse")) {
            assertRendered("uri", uri, uri);
            for (String prefix : List.of("create catalog example properties (",
                    "alter catalog example set properties (")) {
                Assertions.assertTrue(audit.geneEncryptionSQL(prefix + "'uri' = '" + uri + "')").contains(uri));
            }
        }
        assertRendered("uri", "gs://fixture:fixture-password@example_bucket/warehouse?token=fixture-token&mode=ro",
                "gs://example_bucket/warehouse?token=" + MASK + "&mode=ro");
        assertRendered("fs.defaultFS", "viewfs:///warehouse?token=fixture-token&mode=ro#fixture-fragment",
                "viewfs:///warehouse?token=" + MASK + "&mode=ro");
        assertRendered("warehouse", "hdfs:/data.csv?token=fixture-token", "hdfs:/data.csv?token=" + MASK);
    }

    @Test
    void nativeFilesystemExceptionsRetainCredentialBoundaries() {
        for (String uri : List.of("gs://example_bucket:9000/warehouse", "gs://example%5Fbucket/warehouse",
                "gs://example_bucket/warehouse?X-Goog-Signature=fixture-signature", "gs:relative",
                "hdfs:relative", "viewfs:relative", "s3://example_bucket/warehouse", "s3:///warehouse",
                "https:///warehouse", "hdfs:///data.csv?sig=fixture-signature", "viewfs:///path;password=fixture",
                "viewfs://mount%5Ftable/warehouse", "viewfs://mount_table:password/warehouse",
                "viewfs://mount_table:70000/warehouse", "viewfs://mount_table/warehouse?sig=fixture")) {
            assertRendered("uri", uri, MASK);
        }
        assertRendered("custom.password", "gs://example_bucket/warehouse", MASK);
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
