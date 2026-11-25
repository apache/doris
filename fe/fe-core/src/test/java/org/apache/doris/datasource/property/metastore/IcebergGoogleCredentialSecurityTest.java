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

import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;

import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class IcebergGoogleCredentialSecurityTest {
    @ParameterizedTest
    @ValueSource(strings = {"external_account", "external_account_authorized_user", "authorized_user",
            "impersonated_service_account", "gdch_service_account"})
    void rejectsUntrustedCredentialTypes(String type) {
        Map<String, String> raw = restProperties();
        raw.put("gcp.auth.credentials-json", "{\"type\":\"" + type + "\","
                + "\"credential_source\":{\"file\":\"credential-source.txt\"},"
                + "\"token_url\":\"https://tokens.example.invalid/exchange\"}");
        Assertions.assertThrows(IllegalArgumentException.class, () -> normalize(raw));
    }

    @Test
    void rejectsCatalogSuppliedCredentialPaths() {
        Map<String, String> raw = restProperties();
        raw.put("gcp.auth.credentials-path", "credentials.json");
        Assertions.assertThrows(IllegalArgumentException.class, () -> normalize(raw));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-json-secret-marker", "[]", "null", "{}", "{\"type\":7}",
            "{\"type\":\"service_account\",\"token_uri\":null}"})
    void rejectsMalformedCredentialsWithoutEchoingSecrets(String json) {
        Map<String, String> raw = restProperties();
        raw.put("gcp.auth.credentials-json", json);
        IllegalArgumentException failure = Assertions.assertThrows(
                IllegalArgumentException.class, () -> normalize(raw));
        Assertions.assertFalse(failure.toString().contains(json));
        Assertions.assertNull(failure.getCause());
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://tokens.example.invalid/token", "http://oauth2.googleapis.com/token",
            "https://oauth2.googleapis.com/token?redirect=other", "https://oauth2.googleapis.com:444/token",
            "https://oauth2.googleapis.com/token#other", "https://user@oauth2.googleapis.com/token"})
    void rejectsCustomServiceAccountTokenEndpoints(String endpoint) {
        Map<String, String> raw = restProperties();
        raw.put("gcp.auth.credentials-json", "{\"type\":\"service_account\",\"token_uri\":\"" + endpoint + "\"}");
        Assertions.assertThrows(IllegalArgumentException.class, () -> normalize(raw));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"type\":\"service_account\"}",
            "{\"type\":\"service_account\",\"token_uri\":\"https://oauth2.googleapis.com/token\"}",
            "{\"type\":\"service_account\",\"token_uri\":\"https://accounts.google.com/o/oauth2/token\"}"})
    void acceptsInlineServiceAccountsWithGoogleTokenEndpoints(String json) {
        Map<String, String> raw = restProperties();
        raw.put("gcp.auth.credentials-json", json);
        Assertions.assertDoesNotThrow(() -> normalize(raw));
    }

    @Test
    void validatesAgainBeforeInitializingAnyCatalog() {
        Map<String, String> raw = restProperties();
        raw.put("gcp.auth.credentials-json", "{\"type\":\"external_account\"}");
        AtomicBoolean initialized = new AtomicBoolean();
        AbstractIcebergProperties properties = new AbstractIcebergProperties(raw) {
            @Override
            public String getIcebergCatalogType() {
                return "test";
            }

            @Override
            protected Catalog initCatalog(String name, Map<String, String> options, List<StorageProperties> storage) {
                initialized.set(true);
                return Mockito.mock(Catalog.class);
            }
        };
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> properties.initializeCatalog("test", Collections.emptyList()));
        Assertions.assertFalse(initialized.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"gcp.auth.credentials-json", "GCP.AUTH.CREDENTIALS-JSON", "gcs.oauth2.token",
            "gcs.encryption-key", "gcs.decryption-key"})
    void masksCreateAndAlterAuditSql(String key) {
        for (String template : new String[] {"CREATE CATALOG test PROPERTIES(\"type\"=\"iceberg\",%s)",
                "ALTER CATALOG test SET PROPERTIES(%s)"}) {
            String sql = String.format(template, "\"" + key + "\"=\"synthetic-secret-material\"");
            Object command = new NereidsParser().parseSingle(sql);
            Assertions.assertTrue(command instanceof NeedAuditEncryption);
            String masked = ((NeedAuditEncryption) command).geneEncryptionSQL(sql);
            Assertions.assertFalse(masked.contains("synthetic-secret-material"));
            Assertions.assertTrue(masked.contains(PrintableMap.PASSWORD_MASK));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"gcp.auth.credentials-json", "GCP.AUTH.CREDENTIALS-JSON", "gcs.oauth2.token",
            "gcs.encryption-key", "gcs.decryption-key"})
    @SuppressWarnings("unchecked")
    void masksShowCreateAndCatalogProperties(String key) throws Exception {
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Map<String, String> raw = new HashMap<>();
        raw.put(key, "synthetic-secret-material");
        Mockito.when(catalog.getProperties()).thenReturn(raw);
        CatalogMgr manager = new CatalogMgr();
        Field field = CatalogMgr.class.getDeclaredField("nameToCatalog");
        field.setAccessible(true);
        ((Map<String, CatalogIf<?>>) field.get(manager)).put("test", catalog);
        Assertions.assertEquals(PrintableMap.PASSWORD_MASK,
                CatalogMgr.getCatalogPropertiesWithPrintable(catalog).get(key));
        String statement = manager.showCreateCatalog("test").get(0).get(1);
        Assertions.assertFalse(statement.contains("synthetic-secret-material"));
        Assertions.assertTrue(statement.contains(PrintableMap.PASSWORD_MASK));
    }

    private static IcebergRestProperties normalize(Map<String, String> raw) {
        IcebergRestProperties properties = new IcebergRestProperties(raw);
        properties.initNormalizeAndCheckProps();
        return properties;
    }

    private static Map<String, String> restProperties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("iceberg.rest.uri", "https://catalog.example.com");
        raw.put("iceberg.rest.security.type", "google");
        return raw;
    }
}
