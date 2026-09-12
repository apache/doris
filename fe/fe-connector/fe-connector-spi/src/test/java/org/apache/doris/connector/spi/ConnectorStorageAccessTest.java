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

package org.apache.doris.connector.spi;

import org.apache.doris.filesystem.properties.BackendStorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConnectorStorageAccessTest {

    @Test
    public void resolvedIdentityAndCredentialsBelongToOneSnapshot() {
        String uri = "abfss://container@account.dfs.core.windows.net/data/file.parquet";
        Map<String, String> original = new LinkedHashMap<>(Map.of(
                "provider", "azure", "AZURE_AUTH_TYPE", "SAS", "AZURE_SAS_TOKEN", "test-sas-token"));
        ConnectorStorageAccess access = new ConnectorStorageAccess(
                "azure", uri, BackendStorageKind.NATIVE, "FILE_S3", original);

        Assertions.assertEquals("azure", access.getProviderName());
        Assertions.assertEquals(uri, access.getNormalizedUri());
        Assertions.assertEquals(BackendStorageKind.NATIVE, access.getBackendKind());
        Assertions.assertEquals("FILE_S3", access.getBackendFileType());
        Assertions.assertNotSame(original, access.getBackendProperties());

        original.put("AZURE_SAS_TOKEN", "next-generation-token");
        original.clear();
        Assertions.assertEquals(Map.of("provider", "azure", "AZURE_AUTH_TYPE", "SAS",
                "AZURE_SAS_TOKEN", "test-sas-token"), access.getBackendProperties());
    }

    @Test
    public void backendPropertiesCannotBeChangedThroughTheReturnedMapOrEntries() {
        ConnectorStorageAccess access = new ConnectorStorageAccess("azure",
                "abfss://container@account.dfs.core.windows.net/data/file.parquet",
                BackendStorageKind.NATIVE, "FILE_S3", Map.of("AZURE_SAS_TOKEN", "test-sas-token"));
        Map<String, String> properties = access.getBackendProperties();

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> properties.put("AZURE_SAS_TOKEN", "replacement-token"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> properties.remove("AZURE_SAS_TOKEN"));
        Assertions.assertThrows(UnsupportedOperationException.class, properties::clear);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> properties.entrySet().iterator().next().setValue("replacement-token"));
        Assertions.assertEquals(Map.of("AZURE_SAS_TOKEN", "test-sas-token"), access.getBackendProperties());
    }

    @Test
    public void diagnosticStringOmitsTheEntireLocationAndPropertyMap() {
        ConnectorStorageAccess access = new ConnectorStorageAccess("azure",
                "https://account.blob.core.windows.net/private-location"
                        + "?sig=uri-secret-sentinel#fragment-secret-sentinel",
                BackendStorageKind.NATIVE, "FILE_S3", Map.of(
                        "AZURE_SAS_TOKEN", "test-sas-secret",
                        "AZURE_CLIENT_SECRET", "test-client-secret",
                        "unrecognized.secret.key", "test-unrecognized-secret"));

        String diagnostic = access.toString();
        Assertions.assertTrue(diagnostic.contains("azure"));
        Assertions.assertTrue(diagnostic.contains("NATIVE"));
        Assertions.assertTrue(diagnostic.contains("FILE_S3"));
        Assertions.assertTrue(diagnostic.contains("<redacted>"));
        for (String privateValue : List.of("account.blob.core.windows.net", "private-location",
                "uri-secret-sentinel", "fragment-secret-sentinel", "AZURE_SAS_TOKEN",
                "AZURE_CLIENT_SECRET", "unrecognized.secret.key", "test-sas-secret",
                "test-client-secret", "test-unrecognized-secret")) {
            Assertions.assertFalse(diagnostic.contains(privateValue));
        }
    }

    @Test
    public void noopAndDefaultContextsRejectAccessResolutionBeforeReturningAResolver() {
        ConnectorStorageContext defaultContext = new ConnectorStorageContext() {
        };
        List<Map<String, String>> credentialSets = Arrays.asList(null, Map.of(),
                Map.of("adls.sas-token.account", "test-vended-token"));
        for (ConnectorStorageContext context : List.of(ConnectorStorageContext.NOOP, defaultContext)) {
            for (Map<String, String> credentials : credentialSets) {
                UnsupportedOperationException error = Assertions.assertThrows(UnsupportedOperationException.class,
                        () -> context.newStorageAccessResolver(credentials));
                Assertions.assertEquals("Storage access resolution is unavailable for this catalog",
                        error.getMessage());
            }
        }
    }
}
