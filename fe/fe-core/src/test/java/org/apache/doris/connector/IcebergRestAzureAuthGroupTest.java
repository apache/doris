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

package org.apache.doris.connector;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Locale;
import java.util.Map;

class IcebergRestAzureAuthGroupTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void serverOverridesReplaceTheNativeSharedKeyAuthenticationGroup(boolean uppercase) throws Exception {
        String clientKey = "a2V5LWE=";
        String serverKey = "a2V5LWI=";
        String prefix = uppercase ? "ADLS.AUTH.SHARED-KEY.ACCOUNT." : "adls.auth.shared-key.account.";
        ConfigResponse serverConfig = ConfigResponse.builder()
                .withOverride(prefix + (uppercase ? "NAME" : "name"), "account")
                .withOverride(prefix + (uppercase ? "KEY" : "key"), serverKey)
                .build();
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.account_key", clientKey), serverConfig)) {
            Table table = fixture.load();
            Map<String, String> fileIOProperties = table.io().properties();

            Assertions.assertAll(
                    () -> Assertions.assertEquals("account", fileIOProperties.get("adls.auth.shared-key.account.name")),
                    () -> Assertions.assertEquals(serverKey, fileIOProperties.get("adls.auth.shared-key.account.key"),
                            "REST server overrides must replace the native client's SharedKey credential"));

            Map<String, String> nativeProperties = fixture.storageContext()
                    .resolveStorageProperties(fileIOProperties).stream()
                    .filter(binding -> binding.providerName().equalsIgnoreCase("AZURE"))
                    .findFirst().orElseThrow().toBackendProperties().orElseThrow().toMap();

            Assertions.assertEquals("SHARED_KEY", nativeProperties.get("AZURE_AUTH_TYPE"));
            Assertions.assertEquals(serverKey, nativeProperties.get("AZURE_ACCOUNT_KEY"),
                    "native data access must use the same SharedKey as the actual table FileIO");
            Assertions.assertTrue(fileIOProperties.keySet().stream()
                    .filter(key -> key.regionMatches(true, 0, "adls.auth.shared-key.", 0,
                            "adls.auth.shared-key.".length()))
                    .allMatch(key -> key.equals(key.toLowerCase(Locale.ROOT))));
            Assertions.assertEquals(table.schema().asStruct(), TableMetadataParser.read(table.io(),
                    "abfss://container@account.dfs.core.windows.net/table/v1.metadata.json").schema().asStruct());
        }
    }
}
