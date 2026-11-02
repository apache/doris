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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.property.metastore.IcebergRestProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class IcebergRestConnectivityTesterTest {

    @Test
    public void testSessionCatalogWrapperUsesResolvedWarehouse() throws Exception {
        RESTSessionCatalog sessionCatalog = sessionCatalog(
                Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/path"));
        IcebergRestProperties properties = properties(sessionCatalog);
        IcebergRestConnectivityTester tester = new IcebergRestConnectivityTester(properties, Collections.emptyList());

        tester.testConnection();

        Mockito.verify(sessionCatalog).listNamespaces(Mockito.any(SessionContext.class), Mockito.eq(Namespace.empty()));
        Mockito.verify(sessionCatalog).close();
        Assertions.assertNull(properties.getRestSessionCatalog());
        Assertions.assertEquals("s3://warehouse/path", tester.getTestLocation());
    }

    @Test
    public void testSessionCatalogWrapperUsesPolarisBaseLocation() throws Exception {
        RESTSessionCatalog sessionCatalog = sessionCatalog(Collections.singletonMap(
                "default-base-location", "s3://warehouse/polaris"));
        IcebergRestProperties properties = properties(sessionCatalog);
        IcebergRestConnectivityTester tester = new IcebergRestConnectivityTester(properties, Collections.emptyList());

        tester.testConnection();

        Assertions.assertEquals("s3://warehouse/polaris", tester.getTestLocation());
        Mockito.verify(sessionCatalog).close();
    }

    @Test
    public void testCatalogNameIsNotTreatedAsStorageLocation() throws Exception {
        RESTSessionCatalog sessionCatalog = sessionCatalog(
                Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION, "doris_test"));
        IcebergRestConnectivityTester tester = new IcebergRestConnectivityTester(
                properties(sessionCatalog), Collections.emptyList());

        tester.testConnection();

        Assertions.assertNull(tester.getTestLocation());
        Mockito.verify(sessionCatalog).close();
    }

    @Test
    public void testNamespaceFailureClosesSessionCatalog() throws Exception {
        RESTSessionCatalog sessionCatalog = sessionCatalog(Collections.emptyMap());
        IllegalStateException failure = new IllegalStateException("REST authentication failed");
        Mockito.when(sessionCatalog.listNamespaces(Mockito.any(SessionContext.class), Mockito.eq(Namespace.empty())))
                .thenThrow(failure);
        IcebergRestProperties properties = properties(sessionCatalog);
        IcebergRestConnectivityTester tester = new IcebergRestConnectivityTester(properties, Collections.emptyList());

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class, tester::testConnection));

        Mockito.verify(sessionCatalog).close();
        Assertions.assertNull(properties.getRestSessionCatalog());
    }

    private static RESTSessionCatalog sessionCatalog(Map<String, String> resolvedProperties) {
        RESTSessionCatalog sessionCatalog = Mockito.mock(RESTSessionCatalog.class,
                Mockito.withSettings().useConstructor());
        // Exercise Iceberg's real BaseSessionCatalog.AsCatalog wrapper, which is not a RESTCatalog.
        Mockito.when(sessionCatalog.asCatalog(Mockito.any(SessionContext.class))).thenCallRealMethod();
        Mockito.when(sessionCatalog.properties()).thenReturn(resolvedProperties);
        return sessionCatalog;
    }

    private static IcebergRestProperties properties(RESTSessionCatalog sessionCatalog) {
        IcebergRestProperties properties = new IcebergRestProperties(Collections.singletonMap("warehouse", "doris_test")) {
            @Override
            protected RESTSessionCatalog buildRestSessionCatalog(String catalogName, Map<String, String> catalogProps,
                    Configuration configuration) {
                return sessionCatalog;
            }
        };
        properties.initNormalizeAndCheckProps();
        return properties;
    }
}
