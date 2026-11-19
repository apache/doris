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

package org.apache.doris.connector.lance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class LanceCatalogPropertiesTest {

    private static final List<String> HTTP_CONTROL_CHARACTERS = List.of(
            "\0", "\001", "\b", "\n", "\013", "\f", "\r", "\032", "\037", "\177");

    private static Map<String, String> filesystem(String warehouse) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", warehouse);
        return properties;
    }

    private static Map<String, String> rest() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("lance.catalog.type", "rest");
        properties.put("lance.rest.uri", "http://127.0.0.1:19102");
        return properties;
    }

    private static Map<String, String> rest(String key, String value) {
        Map<String, String> properties = rest();
        properties.put(key, value);
        return properties;
    }

    private static String rejection(Map<String, String> properties) {
        return Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceCatalogProperties.of(properties)).getMessage();
    }

    @Test
    void defaultsDescribeAFilesystemCatalogRootedAtTheWarehouse() {
        LanceCatalogProperties properties = LanceCatalogProperties.of(filesystem("/tmp/lance"));
        Assertions.assertEquals("filesystem", properties.getCatalogType());
        Assertions.assertEquals("/tmp/lance", properties.getWarehouse());
        Assertions.assertEquals("$", properties.getNamespaceDelimiter());
        Assertions.assertEquals("default", properties.getRootDatabase());
        Assertions.assertEquals(Collections.emptyList(), properties.getParentNamespace());
        Assertions.assertFalse(properties.isRest());
        Assertions.assertEquals(Collections.emptyList(), properties.secretValues());
    }

    /** The maps 4.1.x persisted for its Lance catalogs (fe-core's upgrade/415 fixture) still bind unchanged. */
    @Test
    void catalogsWrittenByEarlierVersionsStillBind() {
        Map<String, String> s3 = filesystem("s3://warehouse/lance");
        s3.put("lance.catalog.type", "filesystem");
        s3.put("s3.endpoint", "http://127.0.0.1:19001");
        s3.put("s3.access_key", "admin");
        s3.put("s3.secret_key", "password");
        s3.put("s3.region", "us-east-1");
        s3.put("use_path_style", "true");
        s3.put("use_meta_cache", "true");
        Assertions.assertEquals("s3://warehouse/lance", LanceCatalogProperties.of(s3).getWarehouse());

        Map<String, String> local = filesystem("file:///tmp/lance_wh");
        local.put("use_meta_cache", "true");
        Assertions.assertEquals("file:///tmp/lance_wh", LanceCatalogProperties.of(local).getWarehouse());

        Map<String, String> rest = new HashMap<>();
        rest.put("type", "lance");
        rest.put("lance.catalog.type", "rest");
        rest.put("lance.rest.uri", "http://127.0.0.1:19102");
        rest.put("lance.rest.security.type", "bearer");
        rest.put("lance.rest.bearer-token", "doris-lance-rest-test-token");
        rest.put("lance.namespace.delimiter", ".");
        rest.put("lance.namespace.root_database", "default");
        rest.put("use_meta_cache", "true");
        LanceCatalogProperties restProperties = LanceCatalogProperties.of(rest);
        Assertions.assertEquals("rest", restProperties.getCatalogType());
        Assertions.assertTrue(restProperties.isRest());
        Map<String, String> expected = new HashMap<>();
        expected.put("uri", "http://127.0.0.1:19102");
        expected.put("delimiter", ".");
        expected.put("header.Authorization", "Bearer doris-lance-rest-test-token");
        Assertions.assertEquals(expected, restProperties.restNamespaceProperties());
    }

    @Test
    void catalogTypeIsMatchedTrimmedAndCaseInsensitively() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.catalog.type", " FileSystem ");
        Assertions.assertEquals("filesystem", LanceCatalogProperties.of(properties).getCatalogType());
        properties.put("lance.catalog.type", "hive");
        Assertions.assertEquals("Property 'lance.catalog.type' must be 'filesystem' or 'rest', but was 'hive'",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
    }

    @Test
    void filesystemCatalogNeedsAnAbsoluteS3OrOssWarehouse() {
        Map<String, String> missing = new HashMap<>();
        missing.put("type", "lance");
        Assertions.assertEquals("Missing required property 'warehouse' for Lance filesystem catalog",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(missing)).getMessage());
        Assertions.assertEquals("Local Lance warehouse must be an absolute path: relative/dir",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(filesystem("relative/dir"))).getMessage());
        Assertions.assertEquals("Unsupported Lance filesystem warehouse scheme 'hdfs'; supported schemes are"
                + " local/file, s3, and oss", rejection(filesystem("hdfs://nn/lance")));
        Assertions.assertEquals("Lance s3 warehouse must name a bucket, as in s3://bucket/path, but was: s3:/lance",
                rejection(filesystem("s3:/lance")));
        Assertions.assertEquals("oss://bucket/lance",
                LanceCatalogProperties.of(filesystem("oss://bucket/lance")).getWarehouse());
        Assertions.assertEquals("Lance oss warehouse must name a bucket, as in oss://bucket/path, but was:"
                + " oss:/lance", rejection(filesystem("oss:/lance")));
    }

    /** The rules 4.1 and 4.2 apply to a REST catalog, which this class must accept and refuse alike. */
    @Test
    void restCatalogNeedsAnHttpServerAndOnlyItsOwnKeys() {
        Assertions.assertTrue(LanceCatalogProperties.of(rest()).isRest());
        Map<String, String> missing = rest();
        missing.remove("lance.rest.uri");
        Assertions.assertEquals("Missing required property 'lance.rest.uri' for Lance REST catalog",
                rejection(missing));
        Assertions.assertEquals("Property 'warehouse' is not valid for Lance REST catalog",
                rejection(rest("warehouse", "/unused/lance-warehouse")));
        Assertions.assertEquals("Property 'lance.rest.uri' must use http or https",
                rejection(rest("lance.rest.uri", "ftp://127.0.0.1:19102")));
        String noExtras = "Property 'lance.rest.uri' must contain an authority and cannot contain user-info,"
                + " query, or fragment";
        Assertions.assertEquals(noExtras, rejection(rest("lance.rest.uri", "http://user@127.0.0.1:19102")));
        Assertions.assertEquals(noExtras, rejection(rest("lance.rest.uri", "http://127.0.0.1:19102/?a=b")));
        Assertions.assertEquals(noExtras, rejection(rest("lance.rest.uri", "http://127.0.0.1:19102/#f")));
        Assertions.assertEquals(noExtras, rejection(rest("lance.rest.uri", "http:///ns")));
        Assertions.assertEquals("Unsupported Lance REST property 'lance.rest.unknown'",
                rejection(rest("lance.rest.unknown", "value")));
    }

    @Test
    void restSecurityTypeTakesExactlyItsOwnCredential() {
        Assertions.assertEquals("Lance REST security type 'none' cannot configure 'lance.rest.bearer-token' or"
                + " 'lance.rest.api-key'", rejection(rest("lance.rest.api-key", "unexpected")));
        Map<String, String> bearer = rest("lance.rest.security.type", " Bearer ");
        Assertions.assertEquals("Lance REST security type 'bearer' requires only 'lance.rest.bearer-token'",
                rejection(bearer));
        bearer.put("lance.rest.bearer-token", "token");
        bearer.put("lance.rest.api-key", "key");
        Assertions.assertEquals("Lance REST security type 'bearer' requires only 'lance.rest.bearer-token'",
                rejection(bearer));
        Map<String, String> apiKey = rest("lance.rest.security.type", "api_key");
        apiKey.put("lance.rest.api-key", " ");
        Assertions.assertEquals("Lance REST security type 'api_key' requires only 'lance.rest.api-key'",
                rejection(apiKey));
        Assertions.assertEquals("Property 'lance.rest.security.type' must be 'none', 'bearer', or 'api_key'",
                rejection(rest("lance.rest.security.type", "oauth2")));
        Map<String, String> injected = rest("lance.rest.security.type", "bearer");
        injected.put("lance.rest.bearer-token", "secret\nX-Injected: true");
        Assertions.assertEquals("Invalid HTTP credential value in property 'lance.rest.bearer-token'",
                rejection(injected));
    }

    @Test
    void alterBearerToApiKeyClearsThePreviousCredential() {
        Map<String, String> properties = rest("lance.rest.security.type", "bearer");
        properties.put("lance.rest.bearer-token", "old-token");
        Map<String, String> updates = Map.of(
                "lance.rest.security.type", "api_key",
                "lance.rest.bearer-token", "",
                "lance.rest.api-key", "new-key");
        new LanceConnectorProvider().validatePropertiesForUpdate(properties, updates);
        Assertions.assertEquals("old-token", properties.get("lance.rest.bearer-token"));
        properties.putAll(updates);
        Assertions.assertEquals(Map.of("uri", "http://127.0.0.1:19102", "delimiter", "$",
                "header.x-api-key", "new-key"), LanceCatalogProperties.of(properties).restNamespaceProperties());
    }

    @Test
    void alterApiKeyToBearerClearsThePreviousCredential() {
        Map<String, String> properties = rest("lance.rest.security.type", "api_key");
        properties.put("lance.rest.api-key", "old-key");
        Map<String, String> updates = Map.of(
                "lance.rest.security.type", "bearer",
                "lance.rest.api-key", "",
                "lance.rest.bearer-token", "new-token");
        new LanceConnectorProvider().validatePropertiesForUpdate(properties, updates);
        Assertions.assertEquals("old-key", properties.get("lance.rest.api-key"));
        properties.putAll(updates);
        Assertions.assertEquals(Map.of("uri", "http://127.0.0.1:19102", "delimiter", "$",
                "header.Authorization", "Bearer new-token"),
                LanceCatalogProperties.of(properties).restNamespaceProperties());
    }

    @Test
    void alterCanDisableAndReenableEitherAuthenticationMode() {
        for (String credential : List.of("lance.rest.bearer-token", "lance.rest.api-key")) {
            String mode = credential.equals("lance.rest.bearer-token") ? "bearer" : "api_key";
            Map<String, String> properties = rest("lance.rest.security.type", mode);
            properties.put(credential, "original-credential");
            Map<String, String> updates = Map.of("lance.rest.security.type", "none", credential, "");
            new LanceConnectorProvider().validatePropertiesForUpdate(properties, updates);
            properties.putAll(updates);
            Assertions.assertEquals(Map.of("uri", "http://127.0.0.1:19102", "delimiter", "$"),
                    LanceCatalogProperties.of(properties).restNamespaceProperties());

            updates = Map.of("lance.rest.security.type", mode, credential, "replacement-credential");
            new LanceConnectorProvider().validatePropertiesForUpdate(properties, updates);
            properties.putAll(updates);
            String header = mode.equals("bearer") ? "header.Authorization" : "header.x-api-key";
            String value = mode.equals("bearer") ? "Bearer replacement-credential" : "replacement-credential";
            Assertions.assertEquals(Map.of("uri", "http://127.0.0.1:19102", "delimiter", "$", header, value),
                    LanceCatalogProperties.of(properties).restNamespaceProperties());
        }
    }

    @Test
    void alterStillRejectsUnclearedAndBlankActiveCredentials() {
        for (String credential : List.of("lance.rest.bearer-token", "lance.rest.api-key")) {
            String mode = credential.equals("lance.rest.bearer-token") ? "bearer" : "api_key";
            String otherMode = mode.equals("bearer") ? "api_key" : "bearer";
            String otherCredential = mode.equals("bearer") ? "lance.rest.api-key" : "lance.rest.bearer-token";
            Map<String, String> properties = rest("lance.rest.security.type", mode);
            properties.put(credential, "old-credential");
            for (String inactive : List.of("old-credential", " ", "\r\n")) {
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> new LanceConnectorProvider().validatePropertiesForUpdate(properties,
                                Map.of("lance.rest.security.type", otherMode, credential, inactive,
                                        otherCredential, "new-credential")));
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> new LanceConnectorProvider().validatePropertiesForUpdate(properties,
                                Map.of("lance.rest.security.type", "none", credential, inactive)));
            }
            for (String active : List.of("", " ", "\r\n")) {
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> new LanceConnectorProvider().validatePropertiesForUpdate(properties,
                                Map.of("lance.rest.security.type", otherMode, credential, "",
                                        otherCredential, active)));
            }
        }
    }

    /** The credentials go in their own header: one sent as an extra header would dodge the security type. */
    @Test
    void restHeaderValuesAreSentAsWrittenButNeverCarryTheCredentials() {
        Map<String, String> properties = rest("lance.rest.security.type", "api_key");
        properties.put("lance.rest.api-key", "test-api-key");
        properties.put("lance.rest.header.x-tenant", " doris-test ");
        properties.put("lance.rest.uri", " http://127.0.0.1:19102/ns// ");
        Map<String, String> expected = new HashMap<>();
        expected.put("uri", "http://127.0.0.1:19102/ns");
        expected.put("delimiter", "$");
        expected.put("header.x-tenant", " doris-test ");
        expected.put("header.x-api-key", "test-api-key");
        Assertions.assertEquals(expected, LanceCatalogProperties.of(properties).restNamespaceProperties());

        Assertions.assertEquals("Authentication header 'Authorization' must be configured through"
                + " 'lance.rest.security.type'", rejection(rest("lance.rest.header.Authorization", "secret")));
        Assertions.assertEquals("Authentication header 'X-API-KEY' must be configured through"
                + " 'lance.rest.security.type'", rejection(rest("lance.rest.header.X-API-KEY", "secret")));
        Assertions.assertEquals("Invalid HTTP header name in property 'lance.rest.header.bad header'",
                rejection(rest("lance.rest.header.bad header", "v")));
        Assertions.assertEquals("Invalid HTTP header name in property 'lance.rest.header.'",
                rejection(rest("lance.rest.header.", "v")));
        Assertions.assertEquals("Invalid HTTP header value in property 'lance.rest.header.x-tenant'",
                rejection(rest("lance.rest.header.x-tenant", "a\r\nX-Injected: true")));
    }

    @Test
    void restHeadersRejectHttpControlCharactersOnCreateAndAlter() {
        LanceConnectorProvider provider = new LanceConnectorProvider();
        String property = "lance.rest.header.x-tenant";
        String expected = "Invalid HTTP header value in property '" + property + "'";
        for (String control : HTTP_CONTROL_CHARACTERS) {
            for (String value : List.of(control + "tenant", "ten" + control + "ant", "tenant" + control)) {
                Assertions.assertEquals(expected, Assertions.assertThrows(IllegalArgumentException.class,
                        () -> provider.validateProperties(rest(property, value))).getMessage());
                Assertions.assertEquals(expected, Assertions.assertThrows(IllegalArgumentException.class,
                        () -> provider.validatePropertiesForUpdate(rest(property, "old"),
                                Map.of(property, value))).getMessage());
            }
        }
    }

    @Test
    void restCredentialsRejectHttpControlCharactersOnCreateAndAlter() {
        LanceConnectorProvider provider = new LanceConnectorProvider();
        for (String mode : List.of("bearer", "api_key")) {
            String property = mode.equals("bearer") ? "lance.rest.bearer-token" : "lance.rest.api-key";
            String expected = "Invalid HTTP credential value in property '" + property + "'";
            Map<String, String> properties = rest("lance.rest.security.type", mode);
            properties.put(property, "old-credential");
            for (String control : HTTP_CONTROL_CHARACTERS) {
                for (String value : List.of(control + "token", "to" + control + "ken", "token" + control)) {
                    Map<String, String> candidate = new HashMap<>(properties);
                    candidate.put(property, value);
                    Assertions.assertEquals(expected, Assertions.assertThrows(IllegalArgumentException.class,
                            () -> provider.validateProperties(candidate)).getMessage());
                    Assertions.assertEquals(expected, Assertions.assertThrows(IllegalArgumentException.class,
                            () -> provider.validatePropertiesForUpdate(properties,
                                    Map.of(property, value))).getMessage());
                }
            }
        }
    }

    @Test
    void restHeaderValuesKeepLegalTabsSpacesAndNonAsciiText() {
        for (String value : List.of("", "\t", " \t ", "tenant\tname", "~", "\u0080", "租户")) {
            Assertions.assertEquals(value, LanceCatalogProperties.of(rest("lance.rest.header.x-tenant", value))
                    .restNamespaceProperties().get("header.x-tenant"));
        }
        LanceConnectorProvider provider = new LanceConnectorProvider();
        for (String mode : List.of("bearer", "api_key")) {
            String property = mode.equals("bearer") ? "lance.rest.bearer-token" : "lance.rest.api-key";
            String header = mode.equals("bearer") ? "header.Authorization" : "header.x-api-key";
            Map<String, String> properties = rest("lance.rest.security.type", mode);
            properties.put(property, "old-credential");
            for (String value : List.of("token\tvalue", " token ", "~", "\u0080", "令牌")) {
                provider.validatePropertiesForUpdate(properties, Map.of(property, value));
                properties.put(property, value);
                String expected = mode.equals("bearer") ? "Bearer " + value : value;
                Assertions.assertEquals(expected,
                        LanceCatalogProperties.of(properties).restNamespaceProperties().get(header));
            }
        }
    }

    @Test
    void restHeadersRejectConflictingCaseVariants() {
        Map<String, String> properties = rest("lance.rest.header.X-Tenant", "old");
        properties.put("lance.rest.header.x-tenant", "new");
        Assertions.assertEquals("Conflicting values for HTTP header 'x-tenant' in Lance REST properties",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> new LanceConnectorProvider().validateProperties(properties)).getMessage());

        properties.put("lance.rest.header.x-tenant", "OLD");
        Assertions.assertEquals("Conflicting values for HTTP header 'x-tenant' in Lance REST properties",
                rejection(properties));
    }

    @Test
    void restHeadersCoalesceCaseVariantsAndPreserveRawProperties() {
        Map<String, String> properties = rest("lance.rest.header.X-Tenant", " doris-test ");
        properties.put("lance.rest.header.x-tenant", " doris-test ");
        properties.put("lance.rest.header.X-Empty", "");
        properties.put("lance.rest.header.x-empty", "");
        LanceCatalogProperties parsed = LanceCatalogProperties.of(properties);
        Assertions.assertEquals(Map.of("uri", "http://127.0.0.1:19102", "delimiter", "$",
                "header.x-tenant", " doris-test ", "header.x-empty", ""), parsed.restNamespaceProperties());
        Assertions.assertEquals(properties, parsed.getRaw());
    }

    @Test
    void alterRejectsConflictingHeaderCaseVariantsButAllowsAnUnambiguousUpdate() {
        Map<String, String> properties = rest("lance.rest.header.X-Tenant", "old");
        LanceConnectorProvider provider = new LanceConnectorProvider();
        Assertions.assertEquals("Conflicting values for HTTP header 'x-tenant' in Lance REST properties",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> provider.validatePropertiesForUpdate(properties,
                                Map.of("lance.rest.header.x-tenant", "new"))).getMessage());
        Assertions.assertEquals("old", properties.get("lance.rest.header.X-Tenant"));
        Assertions.assertFalse(properties.containsKey("lance.rest.header.x-tenant"));

        Map<String, String> updates = Map.of("lance.rest.header.X-Tenant", "new",
                "lance.rest.header.x-tenant", "new");
        provider.validatePropertiesForUpdate(properties, updates);
        properties.putAll(updates);
        Assertions.assertEquals(Map.of("uri", "http://127.0.0.1:19102", "delimiter", "$",
                "header.x-tenant", "new"), LanceCatalogProperties.of(properties).restNamespaceProperties());
    }

    @Test
    void headerConflictsAreRejectedDuringAuthenticationTransitions() {
        for (String from : List.of("none", "bearer", "api_key")) {
            for (String to : List.of("none", "bearer", "api_key")) {
                if (from.equals(to)) {
                    continue;
                }
                Map<String, String> properties = rest("lance.rest.security.type", from);
                properties.put("lance.rest.bearer-token", from.equals("bearer") ? "old-token" : "");
                properties.put("lance.rest.api-key", from.equals("api_key") ? "old-key" : "");
                properties.put("lance.rest.header.X-Tenant", "old");
                Map<String, String> updates = new HashMap<>();
                updates.put("lance.rest.security.type", to);
                updates.put("lance.rest.bearer-token", to.equals("bearer") ? "new-token" : "");
                updates.put("lance.rest.api-key", to.equals("api_key") ? "new-key" : "");
                updates.put("lance.rest.header.x-tenant", "new");
                LanceConnectorProvider provider = new LanceConnectorProvider();
                Assertions.assertEquals("Conflicting values for HTTP header 'x-tenant' in Lance REST properties",
                        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> provider.validatePropertiesForUpdate(properties, updates)).getMessage());

                updates.put("lance.rest.header.X-Tenant", "new");
                provider.validatePropertiesForUpdate(properties, updates);
                properties.putAll(updates);
                Map<String, String> expected = new HashMap<>(Map.of("uri", "http://127.0.0.1:19102",
                        "delimiter", "$", "header.x-tenant", "new"));
                if (to.equals("bearer")) {
                    expected.put("header.Authorization", "Bearer new-token");
                } else if (to.equals("api_key")) {
                    expected.put("header.x-api-key", "new-key");
                }
                Assertions.assertEquals(expected, LanceCatalogProperties.of(properties).restNamespaceProperties());
            }
        }
    }

    /** What SHOW CREATE CATALOG masks is what provider errors must never echo back. */
    @Test
    void secretValuesAreTheValuesTheSurfaceMasks() {
        Map<String, String> properties = rest("lance.rest.security.type", "bearer");
        properties.put("lance.rest.bearer-token", "test-bearer-token");
        properties.put("lance.rest.header.X-Session-Token", "header-token");
        properties.put("lance.rest.header.Cookie", "session=abc");
        properties.put("lance.rest.header.x-tenant", "doris-test");
        List<String> secrets = new ArrayList<>(LanceCatalogProperties.of(properties).secretValues());
        Collections.sort(secrets);
        Assertions.assertEquals(Arrays.asList("header-token", "session=abc", "test-bearer-token"), secrets);
    }

    @Test
    void restKeysAreRejectedOnAFilesystemCatalog() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.rest.uri", "http://127.0.0.1:19102");
        Assertions.assertEquals("Property 'lance.rest.uri' is not valid for Lance filesystem catalog",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
    }

    /** Namespace values keep their whitespace and explicit emptiness; both matter to the escaping rules. */
    @Test
    void namespaceKeysAreValidatedAsWritten() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.namespace.parent", "company.analytics");
        properties.put("lance.namespace.delimiter", ".");
        properties.put("lance.namespace.root_database", "root");
        LanceCatalogProperties parsed = LanceCatalogProperties.of(properties);
        Assertions.assertEquals(Arrays.asList("company", "analytics"), parsed.getParentNamespace());
        Assertions.assertEquals("root", parsed.getRootDatabase());

        properties.put("lance.namespace.delimiter", "");
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceCatalogProperties.of(properties)).getMessage().contains("cannot be empty"));
        properties.put("lance.namespace.delimiter", ".");
        properties.put("lance.namespace.root_database", " ");
        Assertions.assertEquals("Property 'lance.namespace.root_database' must be non-empty",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
        properties.put("lance.namespace.root_database", "default");
        properties.put("lance.namespace.parent", "a..b");
        Assertions.assertEquals("Empty Lance namespace component in 'a..b'",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> LanceCatalogProperties.of(properties)).getMessage());
    }

    /** ALTER CATALOG merges and cannot remove a key, so a key nobody here knows must not be refused. */
    @Test
    void unknownKeysAreAccepted() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("lance.table_access_cache_ttl_seconds", "0");
        properties.put("meta.cache.lance.anything", "1");
        properties.put("some.future.key", "x");
        LanceCatalogProperties.of(properties);
    }

    @Test
    void toStringNeverPrintsCredentials() {
        Map<String, String> properties = filesystem("/tmp/lance");
        properties.put("s3.secret_key", "sentinel-secret");
        Assertions.assertFalse(LanceCatalogProperties.of(properties).toString().contains("sentinel-secret"));
        Map<String, String> bearer = rest("lance.rest.security.type", "bearer");
        bearer.put("lance.rest.bearer-token", "sentinel-token");
        Assertions.assertFalse(LanceCatalogProperties.of(bearer).toString().contains("sentinel-token"));
    }
}
