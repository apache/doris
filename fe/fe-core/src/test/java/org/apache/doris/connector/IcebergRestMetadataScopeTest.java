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
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

class IcebergRestMetadataScopeTest {

    @ParameterizedTest
    @ValueSource(strings = {"SAS", "SHARED_KEY"})
    void selectedAzureIdentityMustMatchTheMetadataAccountBeforeFileIoCreation(String authType) throws Exception {
        Map<String, String> credentials = authType.equals("SAS")
                ? Map.of("adls.sas-token.other.dfs.core.windows.net", "sig=wrong-account-test-signature")
                : Map.of("adls.auth.shared-key.account.name", "other",
                        "adls.auth.shared-key.account.key", "a2V5LWI=");
        Map<String, String> catalog = authType.equals("SAS") ? Map.of()
                : Map.of("azure.account_name", "account", "azure.account_key", "a2V5LWE=");
        ConfigResponse serverConfig = ConfigResponse.builder().withOverrides(new HashMap<>(credentials)).build();
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(catalog, serverConfig)) {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class, fixture::load);

            Assertions.assertTrue(failure.getMessage().contains("account"));
            Assertions.assertFalse(failure.getMessage().contains("wrong-account-test-signature"));
            Assertions.assertFalse(failure.getMessage().contains("a2V5LWI="));
        }
    }

    @Test
    void unusedExpiredAzureCredentialDoesNotPreventAnOfficialS3MetadataRead() throws Exception {
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.sas_token", "sig=expired-test-signature",
                        "azure.sas_expiry_ms", "1"), ConfigResponse.builder().build(), "s3://bucket/table",
                        "abfss://container@account.dfs.core.windows.net/data")) {
            fixture.tableConfig(Map.of("s3.endpoint", fixture.endpoint(), "s3.path-style-access", "true",
                    "s3.access-key-id", "test-access", "s3.secret-access-key", "test-secret",
                    "client.region", "us-east-1"));

            Table table = fixture.load();

            Assertions.assertInstanceOf(ResolvingFileIO.class, table.io());
            Assertions.assertEquals("abfss://container@account.dfs.core.windows.net/data", table.location());
            Assertions.assertFalse(table.io().properties().containsKey("adls.sas-token.account.dfs.core.windows.net"));
            TableMetadata actual = TableMetadataParser.read(table.io(), "s3://bucket/table/v1.metadata.json");
            Assertions.assertEquals(table.schema().asStruct(), actual.schema().asStruct());
        }
    }
}
