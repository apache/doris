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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorStorageContext;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class IcebergRestFileIOPropertiesTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void responseAdaptationPreservesMetadataLocationAndEveryCredentialScope(boolean stagedCreate) {
        TableMetadata metadata = tableResponse().tableMetadata();
        if (!stagedCreate) {
            metadata = TableMetadataParser.fromJson(
                    "abfss://container@account.dfs.core.windows.net/metadata/v1.metadata.json",
                    TableMetadataParser.toJson(metadata));
        }
        List<Credential> credentials = List.of(
                ImmutableCredential.builder().prefix("abfss://container@account.dfs.core.windows.net/table/")
                        .putConfig("adls.token", "test-scoped-data-token").build(),
                ImmutableCredential.builder().prefix("s3://other-bucket/metadata/")
                        .putConfig("s3.session-token", "test-scoped-metadata-token").build());
        LoadTableResponse original = LoadTableResponse.builder().withTableMetadata(metadata)
                .addAllCredentials(credentials).build();

        LoadTableResponse adapted = (LoadTableResponse) policy().adapt(original);

        Assertions.assertEquals(original.metadataLocation(), adapted.metadataLocation());
        Assertions.assertEquals(original.tableMetadata().location(), adapted.tableMetadata().location());
        Assertions.assertEquals(credentials, adapted.credentials());
        Assertions.assertEquals("test-sdk-token", adapted.config().get("adls.token"),
                "scoped credentials remain for the SDK instead of being flattened into a table-wide identity");
    }

    @Test
    void onlyInitialTableGetAdaptsFileIoProperties() {
        IcebergRestFileIOProperties policy = policy();
        LoadTableResponse response = tableResponse();

        Assertions.assertSame(response, policy.adaptGet(response));
        LoadTableResponse loaded = (LoadTableResponse) policy.withTableLoad(() -> policy.adaptGet(response));

        Assertions.assertNotSame(response, loaded);
        Assertions.assertEquals("test-sdk-token", loaded.config().get("adls.token"));
        Assertions.assertSame(response.tableMetadata(), loaded.tableMetadata());
        Assertions.assertSame(response, policy.adaptGet(response));
    }

    @Test
    void creationResponseDoesNotRequireATableGetScope() {
        LoadTableResponse loaded = (LoadTableResponse) policy().adapt(tableResponse());

        Assertions.assertEquals("test-sdk-token", loaded.config().get("adls.token"));
    }

    @Test
    void nestedLoadRestoresTheOuterScopeAndRemovesItAfterFailure() {
        IcebergRestFileIOProperties policy = policy();
        LoadTableResponse response = tableResponse();
        IllegalArgumentException failure = new IllegalArgumentException("test load failure");

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                () -> policy.withTableLoad(() -> {
                    Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                            () -> policy.withTableLoad(() -> {
                                throw failure;
                            })));
                    Assertions.assertNotSame(response, policy.adaptGet(response));
                    throw failure;
                })));

        Assertions.assertSame(response, policy.adaptGet(response));
    }

    @Test
    void catalogScopesAreIndependentOnTheSameThread() {
        IcebergRestFileIOProperties first = policy();
        IcebergRestFileIOProperties second = policy();
        LoadTableResponse response = tableResponse();

        first.withTableLoad(() -> {
            Assertions.assertNotSame(response, first.adaptGet(response));
            Assertions.assertSame(response, second.adaptGet(response));
            return null;
        });

        Assertions.assertSame(response, first.adaptGet(response));
    }

    @Test
    void loadScopeDoesNotPropagateToAnotherThread() {
        IcebergRestFileIOProperties policy = policy();
        LoadTableResponse response = tableResponse();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            policy.withTableLoad(() -> {
                Assertions.assertSame(response, Assertions.assertDoesNotThrow(
                        () -> executor.submit(() -> policy.adaptGet(response)).get(5, TimeUnit.SECONDS)));
                return null;
            });
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void configurationIsAdaptedOutsideTableLoadWithoutRemovingRestAuthentication() {
        IcebergRestFileIOProperties policy = policy();
        ConfigResponse original = ConfigResponse.builder().withOverride("adls.token", "test-override-token")
                .withOverride("token", "test-rest-token").withDefault("fixture-option", "default").build();

        ConfigResponse adapted = (ConfigResponse) policy.adaptGet(original);

        Assertions.assertFalse(adapted.overrides().containsKey("adls.token"));
        Assertions.assertEquals("test-rest-token", adapted.overrides().get("token"));
        Assertions.assertEquals(original.defaults(), adapted.defaults());
        LoadTableResponse loaded = (LoadTableResponse) policy.withTableLoad(() -> policy.adaptGet(tableResponse()));
        Assertions.assertEquals("test-override-token", loaded.config().get("adls.token"));
        Assertions.assertEquals("test-rest-token", loaded.config().get("token"));
    }

    private static IcebergRestFileIOProperties policy() {
        // Raw SDK tokens belong only to Java FileIO; this test needs no native storage provider.
        return new IcebergRestFileIOProperties(ConnectorStorageContext.NOOP, Map.of("adls.token", "test-sdk-token"));
    }

    private static LoadTableResponse tableResponse() {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), "abfss://container@account.dfs.core.windows.net/table", Map.of());
        return LoadTableResponse.builder().withTableMetadata(metadata).build();
    }
}
