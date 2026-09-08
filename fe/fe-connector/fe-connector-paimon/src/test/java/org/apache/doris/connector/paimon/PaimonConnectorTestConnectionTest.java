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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorTestResult;

import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class PaimonConnectorTestConnectionTest {

    @Test
    void dlfProbeChecksMetadataAndBothStorageSides() throws Exception {
        RecordingConnectorContext context = new RecordingConnectorContext();
        context.backendStorageProperties = Map.of("AWS_ACCESS_KEY", "ak");
        PaimonConnector connector = connector(context, false);

        ConnectorTestResult result = connector.testConnection(null);

        Assertions.assertTrue(result.isSuccess(), result.getMessage());
        Assertions.assertEquals(2, context.authCount);
        Assertions.assertEquals(1, context.fileSystemExistsCount);
        Assertions.assertEquals("s3://bucket/warehouse", context.backendProbeProperties.get("test_location"));
    }

    @Test
    void dlfMetadataFailureIsReportedInsteadOfUnconditionalSuccess() throws Exception {
        RecordingConnectorContext context = new RecordingConnectorContext();
        PaimonConnector connector = connector(context, true);

        ConnectorTestResult result = connector.testConnection(null);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertTrue(result.getMessage().contains("Paimon DLF connectivity test failed"),
                result.getMessage());
        Assertions.assertTrue(result.getMessage().contains("connection refused"), result.getMessage());
        Assertions.assertEquals(0, context.fileSystemExistsCount);
    }

    private static PaimonConnector connector(RecordingConnectorContext context, boolean failMetadata)
            throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "dlf");
        properties.put("warehouse", "oss://bucket/warehouse");
        PaimonConnector connector = new PaimonConnector(properties, context);
        Catalog catalog = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(),
                new Class<?>[] {Catalog.class}, (proxy, method, args) -> {
                    if (method.getName().equals("listDatabases")) {
                        Assertions.assertSame(PaimonConnector.class.getClassLoader(),
                                Thread.currentThread().getContextClassLoader());
                        if (failMetadata) {
                            throw new RuntimeException("connection refused");
                        }
                        return Collections.emptyList();
                    }
                    return null;
                });
        Field field = PaimonConnector.class.getDeclaredField("catalog");
        field.setAccessible(true);
        field.set(connector, catalog);
        return connector;
    }
}
