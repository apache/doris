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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergCreateFileIOFailureTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void createDiagnosticRetainsPostSuccessBoundaryInsteadOfCredentialBearingRootCause(boolean wrappedMarker) {
        IcebergPostSuccessFileIOInitializationException marker = new IcebergPostSuccessFileIOInitializationException(
                IcebergPostSuccessFileIOInitializationException.Operation.CREATE,
                new IllegalArgumentException("fixture-secret-sentinel"));
        RuntimeException failure = wrappedMarker ? new RuntimeException("outer catalog failure", marker) : marker;

        DorisConnectorException result = createFailure(failure);

        Assertions.assertEquals("Failed to create Iceberg table db1.t1: " + marker.getMessage(), result.getMessage());
        Assertions.assertFalse(result.getMessage().contains("fixture-secret-sentinel"));
        Assertions.assertSame(marker, IcebergPostSuccessFileIOInitializationException.find(result).orElseThrow());
    }

    @Test
    void ordinaryCreateFailureKeepsExistingRootCauseDiagnostic() {
        RuntimeException failure = new RuntimeException("outer catalog failure",
                new IllegalArgumentException("fixture remote schema failure"));

        DorisConnectorException result = createFailure(failure);

        Assertions.assertTrue(result.getMessage().contains("Failed to create Iceberg table db1.t1"));
        Assertions.assertTrue(result.getMessage().contains("fixture remote schema failure"));
        Assertions.assertFalse(result.getMessage().contains("request succeeded"));
        Assertions.assertTrue(IcebergPostSuccessFileIOInitializationException.find(result).isEmpty());
    }

    private static DorisConnectorException createFailure(RuntimeException failure) {
        AtomicInteger createCalls = new AtomicInteger();
        // Only the external Iceberg Catalog is substituted; Doris metadata, catalog ops and auth wrapping execute.
        Catalog catalog = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(),
                new Class<?>[] {Catalog.class},
                (proxy, method, args) -> {
                    if ("createTable".equals(method.getName())) {
                        Assertions.assertEquals(TableIdentifier.of("db1", "t1"), args[0]);
                        createCalls.incrementAndGet();
                        throw failure;
                    }
                    throw new AssertionError("Unexpected external catalog call: " + method.getName());
                });
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(catalog),
                IcebergCatalogProperties.of(Map.of(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE,
                        IcebergCatalogProperties.TYPE_REST)), context);
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db1").tableName("t1")
                .columns(List.of(new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", true, null, false)))
                .build();

        DorisConnectorException result = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.createTable(null, request));

        Assertions.assertEquals(1, createCalls.get());
        Assertions.assertEquals(1, context.authCount);
        Assertions.assertSame(failure, result.getCause());
        return result;
    }
}
