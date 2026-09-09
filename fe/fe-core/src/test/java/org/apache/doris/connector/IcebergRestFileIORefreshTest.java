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
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class IcebergRestFileIORefreshTest {

    @Test
    void metadataRefreshKeepsTheExistingVendedFileIOWhenResponseOmitsCredentials() throws Exception {
        String sasProperty = "adls.sas-token.account.dfs.core.windows.net";
        String freshSas = "sv=2024-11-04&sp=r&sig=refresh-test-signature&se=2100-01-01T00:00:00Z";
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.sas_token", "sig=expired-static-test-signature",
                        "azure.sas_expiry_ms", "1"))) {
            fixture.tableConfig(Map.of(sasProperty, freshSas));
            Table table = fixture.loadForRefresh();
            FileIO fileIO = table.io();
            Assertions.assertEquals(freshSas, fileIO.properties().get(sasProperty));
            String metadataLocation = table.location() + "/v1.metadata.json";
            TableMetadata beforeRefresh = TableMetadataParser.read(fileIO, metadataLocation);
            Assertions.assertEquals(table.schema().asStruct(), beforeRefresh.schema().asStruct());

            // The server may omit credentials on metadata refresh: the existing FileIO still owns the valid SAS.
            fixture.removeTableConfig(sasProperty);
            int requestsBeforeRefresh = fixture.tableLoadRequestCount();

            Assertions.assertDoesNotThrow(table::refresh);

            Assertions.assertEquals(requestsBeforeRefresh + 1, fixture.tableLoadRequestCount(),
                    "the SDK table refresh must issue one actual REST metadata GET");
            Assertions.assertSame(fileIO, table.io(), "metadata refresh must not replace the table's FileIO");
            Assertions.assertEquals(freshSas, table.io().properties().get(sasProperty),
                    "an omitted refresh credential must not restore the expired catalog SAS");
            TableMetadata afterRefresh = TableMetadataParser.read(table.io(), metadataLocation);
            Assertions.assertEquals(beforeRefresh.uuid(), afterRefresh.uuid());
            Assertions.assertEquals(beforeRefresh.schema().asStruct(), afterRefresh.schema().asStruct());
        }
    }
}
