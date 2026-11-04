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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LanceFilesystemCatalogTest {

    @Test
    public void testCatalogTypeDoesNotInitializeNativeResources() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE, LanceExternalCatalog.LANCE_REST);
        properties.put(LanceExternalCatalog.REST_URI, "http://127.0.0.1:1/");
        LanceExternalCatalog catalog = new LanceExternalCatalog(7, "lance_type_only", null, properties, "");

        Assert.assertEquals(LanceExternalCatalog.LANCE_REST, catalog.getLanceCatalogType());
        Assert.assertFalse(catalog.isInitialized());
    }

    @Test
    public void testLoadTableIndexEntriesRejectsRestCatalogBeforeInit() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE, LanceExternalCatalog.LANCE_REST);
        properties.put(LanceExternalCatalog.REST_URI, "http://127.0.0.1:1/");
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                5, "lance_rest_entries", null, properties, "");

        Assert.assertFalse(catalog.isInitialized());
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> catalog.loadTableIndexEntries("db", "table"));
        Assert.assertEquals("Lance index inspection is not supported for Lance REST catalogs",
                exception.getDetailMessage());
        Assert.assertFalse(catalog.isInitialized());
    }

    @Test
    public void testNamespaceNameRoundTrip() throws Exception {
        Assert.assertEquals(Collections.emptyList(), LanceNamespaceName.dorisDatabaseNameToNamespace(
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        Collections.emptyList(), ".", "default"),
                ".", "default"));
        Assert.assertEquals("doris",
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        Collections.singletonList("doris"), ".", "default"));
        Assert.assertEquals("company.analytics",
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        java.util.Arrays.asList("company", "analytics"), ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("company", "analytics"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("company", "analytics"), ".", "default"),
                        ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("a.b", "c"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("a.b", "c"), ".", "default"),
                        ".", "default"));

        java.util.List<String> delimiterAtEnd = java.util.Arrays.asList("a.", "b");
        java.util.List<String> delimiterAtStart = java.util.Arrays.asList("a", ".b");
        String encodedAtEnd =
                LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtEnd, ".", "default");
        String encodedAtStart =
                LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtStart, ".", "default");
        Assert.assertNotEquals(encodedAtEnd, encodedAtStart);
        Assert.assertEquals(delimiterAtEnd,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtEnd, ".", "default"));
        Assert.assertEquals(delimiterAtStart,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtStart, ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("a\\b", "c"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("a\\b", "c"), ".", "default"),
                        ".", "default"));

        String rootCollision = LanceNamespaceName.namespaceToDorisDatabaseName(
                Collections.singletonList("default"), ".", "default");
        Assert.assertEquals("\\default", rootCollision);
        Assert.assertEquals(Collections.singletonList("default"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(rootCollision, ".", "default"));
    }

    @Test
    public void testLoadTableIndexEntriesWrapsFailureWithSanitizedMessage() {
        String accessKey = "sentinel-access-key";
        String secretKey = "sentinel-secret-key";
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE,
                LanceExternalCatalog.LANCE_FILESYSTEM);
        properties.put(LanceExternalCatalog.WAREHOUSE, "/nonexistent-lance-warehouse-dir");
        properties.put("AWS_ACCESS_KEY", accessKey);
        properties.put("AWS_SECRET_KEY", secretKey);
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                6, "lance_filesystem_entries", null, properties, "");

        RuntimeException exception = Assert.assertThrows(RuntimeException.class,
                () -> catalog.loadTableIndexEntries("db", "table"));

        Assert.assertTrue(exception.getMessage(), exception.getMessage().startsWith(
                "Failed to init catalog: lance_filesystem_entries, error: "));
        Assert.assertNotNull(exception.getCause());
        StringWriter stackTrace = new StringWriter();
        exception.printStackTrace(new PrintWriter(stackTrace));
        for (String sentinel : Arrays.asList(accessKey, secretKey)) {
            Assert.assertFalse(exception.getMessage().contains(sentinel));
            Assert.assertFalse(exception.getCause().getMessage().contains(sentinel));
            Assert.assertFalse(stackTrace.toString().contains(sentinel));
        }
    }

    @Test
    public void testIndexMetadataErrorSanitization() {
        String bearerToken = "sentinel-bearer-token";
        String apiKey = "sentinel-api-key";
        String accessKey = "sentinel-access-key";
        String secretKey = "sentinel-secret-key";
        String sessionToken = "sentinel-session-token";
        String ossAccessKey = "sentinel-oss-access-key";
        String ossSecretKey = "sentinel-oss-secret-key";
        String ossSecurityToken = "sentinel-oss-security-token";
        String datasetUri = "s3://sentinel-user:sentinel-password@bucket/private/table.lance";

        Map<String, String> runtimeStorageOptions = new HashMap<>();
        runtimeStorageOptions.put("aws_access_key_id", accessKey);
        runtimeStorageOptions.put("aws_secret_access_key", secretKey);
        runtimeStorageOptions.put("aws_session_token", sessionToken);
        runtimeStorageOptions.put("oss_access_key_id", ossAccessKey);
        runtimeStorageOptions.put("oss_secret_access_key", ossSecretKey);
        runtimeStorageOptions.put("oss_security_token", ossSecurityToken);
        String providerMessage = "provider failure\nuri=" + datasetUri
                + " bearer=" + bearerToken + " api-key=" + apiKey
                + " access=" + accessKey + " secret=" + secretKey + " session=" + sessionToken
                + " oss-access=" + ossAccessKey + " oss-secret=" + ossSecretKey
                + " oss-token=" + ossSecurityToken;

        RuntimeException providerFailure = new RuntimeException(providerMessage);
        RuntimeException exposed = LanceErrorMessages.failure(
                "Failed to load Lance index metadata for db.table", providerFailure, datasetUri,
                runtimeStorageOptions, Arrays.asList(bearerToken, apiKey));
        StringWriter stackTrace = new StringWriter();
        exposed.printStackTrace(new PrintWriter(stackTrace));

        for (String sentinel : Arrays.asList(bearerToken, apiKey, accessKey, secretKey,
                sessionToken, ossAccessKey, ossSecretKey, ossSecurityToken, datasetUri)) {
            Assert.assertFalse(exposed.getMessage().contains(sentinel));
            Assert.assertFalse(exposed.getCause().getMessage().contains(sentinel));
            Assert.assertFalse(stackTrace.toString().contains(sentinel));
        }
        Assert.assertNotSame(providerFailure, exposed.getCause());
        Assert.assertTrue(exposed.getCause().getMessage().contains("***"));
        Assert.assertFalse(exposed.getCause().getMessage().contains("\n"));
        Assert.assertTrue(exposed.getCause().getMessage().getBytes(StandardCharsets.UTF_8).length <= 1024);
    }

    @Test
    public void testIndexMetadataErrorSanitizationUsesUtf8ByteLimit() {
        char[] multibyteCharacters = new char[1024];
        Arrays.fill(multibyteCharacters, '界');

        String sanitized = LanceErrorMessages.sanitize(
                new RuntimeException(new String(multibyteCharacters)), null, Collections.emptyMap(),
                Collections.emptyList());

        Assert.assertTrue(sanitized.getBytes(StandardCharsets.UTF_8).length <= 1024);
    }

    @Test
    public void testIndexMetadataErrorSanitizationReplacesOverlappingSecrets() {
        Map<String, String> runtimeStorageOptions = Collections.singletonMap(
                "aws_secret_access_key", "overlapping-secret-with-suffix");

        String sanitized = LanceErrorMessages.sanitize(
                new RuntimeException("overlapping-secret-with-suffix"), null, runtimeStorageOptions,
                Collections.singletonList("overlapping-secret"));

        Assert.assertEquals("RuntimeException: ***", sanitized);
    }

    @Test
    public void testIndexMetadataFailurePreservesSanitizedMetadataErrorType() {
        IllegalArgumentException metadataFailure = new IllegalArgumentException("invalid metadata");

        RuntimeException exposed = LanceErrorMessages.failure(
                "Failed to load Lance index metadata for db.table", metadataFailure, null, null, Collections.emptyList());

        Assert.assertTrue(exposed.getCause() instanceof IllegalArgumentException);
        Assert.assertNotSame(metadataFailure, exposed.getCause());
        Assert.assertEquals("IllegalArgumentException: invalid metadata",
                exposed.getCause().getMessage());
    }

}
